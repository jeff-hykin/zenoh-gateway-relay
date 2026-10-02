//! The relay against an in-process backend, with Rust clients as viewers (`cargo test`): leases, queries and puts
//! forwarded, a data topic pulled once for two viewers, auth at the relay.

use std::time::Duration;
use tokio::time::timeout;
use zenoh_web::client::{Client, ClientOptions, Delivery, Message, PublisherOptions, SubscribeOptions};
use zenoh_web::{AudioPcm, Codec, CodecOutput, CodecSample, DecodedFrame, Grant, Server, zenoh};
use zenoh_web_relay::Relay;

fn isolated(extra: &[(&str, String)]) -> zenoh::Config {
    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").unwrap();
    config.insert_json5("listen/endpoints", "[]").unwrap();
    for (key, value) in extra {
        config.insert_json5(key, value).unwrap();
    }
    config
}

/// Any sample -> 20 ms of a 440 Hz tone, 48 kHz mono.
struct Tone;

impl Codec for Tone {
    fn name(&self) -> &str {
        "test-tone"
    }

    fn output(&self) -> CodecOutput {
        CodecOutput::Audio
    }

    fn decode(&self, _sample: &CodecSample<'_>) -> anyhow::Result<DecodedFrame> {
        let samples = (0..960).map(|index| ((index as f64 * 440.0 / 48_000.0 * std::f64::consts::TAU).sin() * 8000.0) as i16).collect();
        Ok(DecodedFrame::Audio(AudioPcm::new(48_000, 1, samples)?))
    }
}

/// A backend (lease group `drive` on `cmd/**`) whose zenoh dials out to `endpoint`.
async fn start_backend(endpoint: &str) -> (Server, zenoh::Session) {
    let session = zenoh::open(isolated(&[("connect/endpoints", format!("[\"{endpoint}\"]"))])).await.unwrap();
    let backend = Server::builder()
        .session(session.clone())
        .zenoh_signalling("robot")
        .codec(Tone)
        .lease_group("drive", ["cmd/**"])
        .authorize(|token, _| if token == Some("relay-secret") { Ok(Grant::all()) } else { Err("unknown token".into()) })
        .build()
        .await
        .unwrap();
    (backend, session)
}

/// Puts `<label> <n>` on robot/state every 50 ms.
fn keep_putting(session: &zenoh::Session, label: &'static str) -> tokio::task::JoinHandle<()> {
    let session = session.clone();
    tokio::spawn(async move {
        for count in 0..=u64::MAX {
            let _ = session.put("robot/state", format!("{label} {count}")).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
}

/// A relay on free ports and a backend (lease group `drive` on `cmd/**`) dialled out to it, and the relay's URL.
async fn start() -> (Relay, Server, zenoh::Session, String, String) {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let endpoint = format!("tcp/127.0.0.1:{port}");
    let relay = tokio::spawn(
        Relay::builder("robot")
            .zenoh_config(isolated(&[]))
            .listen(endpoint.clone())
            .backend_token("relay-secret")
            .viewers(|server| server.authorize(|token, _| if token == Some("viewer") { Ok(Grant::all()) } else { Err("unknown token".into()) }))
            .build(),
    );
    let (backend, session) = start_backend(&endpoint).await;
    let relay = timeout(Duration::from_secs(20), relay).await.unwrap().unwrap().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = relay.router();
    tokio::spawn(async move { axum::serve(listener, router).await });
    (relay, backend, session, url, endpoint)
}

fn viewer_options() -> ClientOptions {
    ClientOptions { token: Some("viewer".into()), heartbeat_hz: 5.0, ..Default::default() }
}

async fn eventually(mut condition: impl FnMut() -> bool, what: &str) {
    for _ in 0..300 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{what}");
}

#[tokio::test(flavor = "multi_thread")]
async fn relays_data_puts_queries_and_leases() {
    let (relay, backend, session, url, endpoint) = start().await;
    let refused = Client::connect(&url, ClientOptions { token: Some("nope".into()), ..Default::default() }).await;
    assert!(refused.err().unwrap().to_string().contains("unknown token"), "the relay authorizes viewers");

    // one data topic, two viewers: one upstream subscription
    let putter = keep_putting(&session, "state");
    let (first, second) = (Client::connect(&url, viewer_options()).await.unwrap(), Client::connect(&url, viewer_options()).await.unwrap());
    let mut subscriptions = vec![
        first.subscribe("robot/state", SubscribeOptions::default()).await.unwrap(),
        second.subscribe("robot/state", SubscribeOptions::default()).await.unwrap(),
    ];
    for subscription in &mut subscriptions {
        let Message::Data(message) = timeout(Duration::from_secs(10), subscription.recv()).await.unwrap().unwrap() else { panic!("not data") };
        assert!(String::from_utf8_lossy(&message.bytes).starts_with("state "));
    }
    assert_eq!(backend.subscriptions(), [("robot/state".to_owned(), None)], "pulled once for two viewers");

    // a viewer's put reaches the backend's zenoh
    let commands = session.declare_subscriber("cmd/vel").await.unwrap();
    let publisher = first.publish("cmd/vel", PublisherOptions { delivery: Some(Delivery::Reliable), ..Default::default() }).await.unwrap();
    publisher.put(b"forward").await.unwrap();
    let sample = timeout(Duration::from_secs(5), commands.recv_async()).await.unwrap().unwrap();
    assert_eq!(sample.payload().to_bytes().as_ref(), b"forward");

    // a viewer's query is answered by the backend's zenoh
    let _queryable = session.declare_queryable("robot/answer").callback(|query| {
        tokio::spawn(async move { query.reply("robot/answer", "42").await.unwrap() });
    }).await.unwrap();
    let replies = first.get("robot/answer", Duration::from_secs(3)).await.unwrap();
    assert_eq!(replies.iter().map(|reply| reply.bytes.clone()).collect::<Vec<_>>(), [b"42".to_vec()]);

    // a viewer's lease is held on the backend by the relay, and released with it
    let lease = first.lease("drive", Some(vec!["cmd/**".into()]), None).await.unwrap();
    eventually(|| backend.leases().iter().any(|(group, _)| group == "drive"), "the relay holds the viewer's lease on the backend").await;
    lease.release().await.unwrap();
    eventually(|| backend.leases().is_empty(), "the relay released it on the backend").await;

    // the backend ending the relay's lease ends the viewer's
    let lease = first.lease("drive", Some(vec!["cmd/**".into()]), None).await.unwrap();
    eventually(|| !backend.leases().is_empty(), "held again").await;
    assert!(backend.expire_lease("drive", "operator took over").await);
    let lost = timeout(Duration::from_secs(5), lease.wait_lost()).await.unwrap();
    assert!(lost.contains("operator took over"), "{lost}");

    // audio: the backend's Opus decoded once at the relay and encoded again for the viewer
    let tone_putter = {
        let session = session.clone();
        tokio::spawn(async move {
            loop {
                let _ = session.put("robot/mic", vec![0u8]).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };
    let mut mic = first.subscribe("robot/mic", SubscribeOptions { codec: Some("test-tone".into()), ..Default::default() }).await.unwrap();
    let mut packets = 0;
    while packets < 10 {
        if let Message::Audio(packet) = timeout(Duration::from_secs(10), mic.recv()).await.unwrap().unwrap() {
            assert!(!packet.data.is_empty());
            packets += 1;
        }
    }
    assert!(backend.subscriptions().contains(&("robot/mic".to_owned(), Some("test-tone".to_owned()))));
    drop(mic);
    tone_putter.abort();

    // the backend restarts: the relay reconnects and viewers' subscriptions resume
    putter.abort();
    backend.shutdown().await.unwrap();
    session.close().await.unwrap();
    let (backend, session) = start_backend(&endpoint).await;
    let putter = keep_putting(&session, "restarted");
    eventually(|| backend.subscriptions() == [("robot/state".to_owned(), None)], "the relay pulled the topic again after reconnecting").await;
    for subscription in &mut subscriptions {
        loop {
            let Message::Data(message) = timeout(Duration::from_secs(10), subscription.recv()).await.unwrap().unwrap() else { panic!("not data") };
            if message.bytes.starts_with(b"restarted ") {
                break;
            }
        }
    }

    // the last viewer leaving closes the upstream subscription
    drop(subscriptions);
    eventually(|| backend.subscriptions().is_empty(), "upstream closed after the last viewer").await;
    let stats = relay.stats().await;
    assert_eq!(stats["backend"]["connected"], true);
    assert!(stats["forwardedPuts"].as_u64().unwrap() >= 1);
    putter.abort();
    first.close().await;
    second.close().await;
    relay.shutdown().await.unwrap();
    backend.shutdown().await.unwrap();
}
