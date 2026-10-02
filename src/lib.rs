//! zenoh-web-relay: one zenoh-web backend (e.g. a robot) fanned out to many browsers, keeping the load off the backend.
//!
//! - The backend needs no inbound ports: its zenoh dials out to the relay's zenoh router, and the relay signals to the
//!   backend's zenoh-web over that link (`zenoh_signalling` / `Client::connect_zenoh`). WebRTC then flows from the
//!   backend to the relay's address.
//! - The relay pulls each camera once, at best quality, whatever the number of viewers, decodes it once, and serves
//!   it again through its own zenoh-web [`Server`], whose encoder sharing and allocator re-encode it per quality
//!   bucket for the viewers. Data topics are pulled once and passed through. Upstream subscriptions open with the
//!   first viewer and close after the last.
//! - Viewers' puts, queries and leases go to the backend through the relay's one connection; the relay authorizes
//!   viewers itself and authenticates to the backend with its own token.
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! let relay = zenoh_web_relay::Relay::builder("robot")
//!     .listen("tls/0.0.0.0:7447")
//!     .backend_token("relay-secret")
//!     .viewers(|server| server.serve_dir("web"))
//!     .build()
//!     .await?; // waits for the backend's first connection (its codecs)
//! relay.serve_with_shutdown(("0.0.0.0", 7448), async { let _ = tokio::signal::ctrl_c().await; }).await?;
//! # Ok(())
//! # }
//! ```

pub mod auth;
mod codecs;

use anyhow::{Context, Result, anyhow};
use axum::Json;
use axum::routing::get;
use log::{debug, info, warn};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::net::ToSocketAddrs;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use zenoh::key_expr::keyexpr;
use zenoh::qos::CongestionControl;
use zenoh_ext::{AdvancedPublisherBuilderExt, CacheConfig};
use zenoh_web::client::{Client, ClientOptions, ConnectionState, Delivery, Lease, Message, Publisher, PublisherOptions, SubscribeOptions};
use zenoh_web::{CodecOutput, IceServer, Server, ServerBuilder};

pub use zenoh_web;

/// The key chunk under which the relay puts what it pulled through a codec (`@relay/<codec>/<key>`); `**` never
/// matches it, so raw viewers don't see it.
pub const RELAY_PREFIX: &str = "@relay";
/// `GET` this path for the relay's stats (JSON).
pub const STATS_PATH: &str = "/zenoh-web-relay/stats";
/// Attachment of the relay's own raw puts on the viewers' session, so they aren't forwarded back.
const OWN_PUT: &[u8] = b"zenoh-web-relay";
/// An upstream stream nobody watches closes after this (a viewer reloading the page keeps it).
const LINGER: Duration = Duration::from_secs(1);
const RECONCILE_INTERVAL: Duration = Duration::from_millis(500);
const TOPICS_INTERVAL: Duration = Duration::from_secs(3);
const RETRY: Duration = Duration::from_secs(1);
/// A backend connection whose pings fail this long is dropped and remade.
const DEGRADED_LIMIT: Duration = Duration::from_secs(5);

/// Configures a [`Relay`]. Start with [`Relay::builder`].
#[must_use = "a builder does nothing until `build` is awaited"]
pub struct RelayBuilder {
    backend_name: String,
    backend_token: Option<String>,
    listen: Vec<String>,
    zenoh_config: Option<zenoh::Config>,
    upstream_ice_servers: Option<Vec<IceServer>>,
    upstream_max_bitrate: f64,
    topic_probe_ms: u64,
    viewers: Box<dyn FnOnce(ServerBuilder) -> ServerBuilder + Send>,
}

impl RelayBuilder {
    /// A zenoh endpoint the relay's router listens on for the backend, e.g. `tls/0.0.0.0:7447` (repeatable; replaces
    /// the config's `listen/endpoints`).
    pub fn listen(mut self, endpoint: impl Into<String>) -> Self {
        self.listen.push(endpoint.into());
        self
    }

    /// The router's zenoh config (TLS certificates, `transport/auth/usrpwd`, ...). Its mode is set to router. Default:
    /// no multicast scouting.
    pub fn zenoh_config(mut self, config: zenoh::Config) -> Self {
        self.zenoh_config = Some(config);
        self
    }

    /// The token the relay presents to the backend (its authorize hook decides what the relay, and so every
    /// viewer, may do there).
    pub fn backend_token(mut self, token: impl Into<String>) -> Self {
        self.backend_token = Some(token.into());
        self
    }

    /// STUN/TURN servers for the relay's connection to the backend. Default: the backend's (`zenoh-web/<name>/ice`).
    pub fn upstream_ice_servers(mut self, servers: impl IntoIterator<Item = IceServer>) -> Self {
        self.upstream_ice_servers = Some(servers.into_iter().collect());
        self
    }

    /// `maxBitrate` of each camera pulled from the backend, bits/s (default 8 Mbit/s).
    pub fn upstream_max_bitrate(mut self, bits_per_sec: f64) -> Self {
        self.upstream_max_bitrate = bits_per_sec;
        self
    }

    /// `probeMs` of the relay's topic listings of the backend (default 0: liveliness tokens only, so listing
    /// subscribes the backend to nothing).
    pub fn topic_probe_ms(mut self, ms: u64) -> Self {
        self.topic_probe_ms = ms;
        self
    }

    /// Configures the viewers' zenoh-web server (authorize hook, lease groups, ICE servers, UDP ports, static files,
    /// video policy, hardware encoder...). The relay sets its session and codecs.
    pub fn viewers(mut self, configure: impl FnOnce(ServerBuilder) -> ServerBuilder + Send + 'static) -> Self {
        self.viewers = Box::new(configure);
        self
    }

    /// Opens the router, waits for the backend's first connection (whose codecs the viewers' server mirrors), builds
    /// the viewers' server and starts relaying.
    pub async fn build(self) -> Result<Relay> {
        let mut config = match self.zenoh_config {
            Some(config) => config,
            None => {
                let mut config = zenoh::Config::default();
                config.insert_json5("scouting/multicast/enabled", "false").map_err(|error| anyhow!("{error}"))?;
                config
            }
        };
        config.insert_json5("mode", "\"router\"").map_err(|error| anyhow!("{error}"))?;
        if !self.listen.is_empty() {
            config.insert_json5("listen/endpoints", &serde_json::to_string(&self.listen)?).map_err(|error| anyhow!("{error}"))?;
        }
        let router = zenoh::open(config).await.map_err(|error| anyhow!("opening the relay's zenoh router: {error}"))?;
        let client_options = ClientOptions {
            token: self.backend_token,
            ice_servers: self.upstream_ice_servers,
            // leases need a heartbeat; 2 s of silence before the backend gives up on the relay
            heartbeat_hz: 5.0,
            heartbeat_misses: 10,
            ..Default::default()
        };
        info!("waiting for backend {:?} on zenoh-web/{}/offer", self.backend_name, self.backend_name);
        let client = connect(&router, &self.backend_name, &client_options).await?;
        let mut kinds = HashMap::new();
        let mut builder = (self.viewers)(Server::builder());
        for codec in client.codecs() {
            let output = match codec.output.as_str() {
                "video" => CodecOutput::Video,
                "fields" => CodecOutput::Fields,
                "data" => CodecOutput::Data,
                other => {
                    warn!("codec {} ({other}) is not relayed", codec.name);
                    continue;
                }
            };
            builder = match output {
                CodecOutput::Video => builder.codec(codecs::RelayVideo::new(&codec.name)),
                _ => builder.codec(codecs::RelayData::new(&codec.name, output)),
            };
            kinds.insert(codec.name.clone(), output);
        }
        let mut local_config = zenoh::Config::default();
        // timestamps: the cache that replays a data topic's latest message to a late viewer needs them
        let settings = [("scouting/multicast/enabled", "false"), ("listen/endpoints", "[]"), ("connect/endpoints", "[]"), ("timestamping/enabled", "{router: true, peer: true, client: true}")];
        for (key, value) in settings {
            local_config.insert_json5(key, value).map_err(|error| anyhow!("{error}"))?;
        }
        let local = zenoh::open(local_config).await.map_err(|error| anyhow!("opening the viewers' zenoh session: {error}"))?;
        let server = builder.session(local.clone()).build().await?;
        let (client_tx, _) = watch::channel(Some(client.clone()));
        let state = Arc::new(State {
            router,
            local,
            server,
            backend_name: self.backend_name,
            client_options,
            client: client_tx,
            generation: AtomicU64::new(0),
            kinds,
            upstream_max_bitrate: self.upstream_max_bitrate,
            topic_probe_ms: self.topic_probe_ms,
            streams: Mutex::default(),
            topics: Mutex::default(),
            tokens: Mutex::default(),
            publishers: tokio::sync::Mutex::default(),
            leases: tokio::sync::Mutex::default(),
            reconcile: Notify::new(),
            forwarded_puts: AtomicU64::new(0),
            forwarded_queries: AtomicU64::new(0),
            tasks: Mutex::default(),
        });
        let tasks = vec![
            tokio::spawn(supervise(Arc::downgrade(&state), client)),
            tokio::spawn(reconcile_loop(Arc::downgrade(&state))),
            tokio::spawn(topics_loop(Arc::downgrade(&state))),
            forward_puts(&state).await?,
            forward_queries(&state).await?,
        ];
        *state.tasks.lock().unwrap() = tasks;
        Ok(Relay { state })
    }
}

/// Connects to the backend over the router, retrying until it answers (a refused token is final).
async fn connect(router: &zenoh::Session, name: &str, options: &ClientOptions) -> Result<Client> {
    let mut attempts = 0u64;
    loop {
        match Client::connect_zenoh(router, name, options.clone()).await {
            Ok(client) => {
                info!("backend {name:?} connected");
                return Ok(client);
            }
            Err(error) if error.to_string().contains("refused the token") => return Err(error.context("the backend refused the relay")),
            Err(error) => {
                attempts += 1;
                if attempts % 10 == 1 {
                    info!("backend {name:?} not reachable yet: {error:#}");
                }
                tokio::time::sleep(RETRY).await;
            }
        }
    }
}

/// What the relay pulls for one viewer key: the concrete key and the codec (None: raw).
type StreamId = (String, Option<String>);

struct Stream {
    task: JoinHandle<()>,
    unwanted_since: Option<Instant>,
    stats: Arc<StreamStats>,
}

#[derive(Default)]
struct StreamStats {
    messages: AtomicU64,
    pictures: AtomicU64,
    dropped_access_units: AtomicU64,
}

struct State {
    router: zenoh::Session,
    local: zenoh::Session,
    server: Server,
    backend_name: String,
    client_options: ClientOptions,
    /// the backend connection, None while it is down
    client: watch::Sender<Option<Client>>,
    generation: AtomicU64,
    /// backend codec name -> its kind
    kinds: HashMap<String, CodecOutput>,
    upstream_max_bitrate: f64,
    topic_probe_ms: u64,
    streams: Mutex<HashMap<StreamId, Stream>>,
    /// the backend's topics
    topics: Mutex<BTreeSet<String>>,
    /// liveliness tokens mirroring them on the viewers' session, for listTopics
    tokens: Mutex<HashMap<String, zenoh::liveliness::LivelinessToken>>,
    /// viewers' puts: key -> the relay's publisher on the backend, with the connection generation it belongs to
    publishers: tokio::sync::Mutex<HashMap<String, (u64, Publisher)>>,
    /// viewers' leases held on the backend: group -> lease
    leases: tokio::sync::Mutex<HashMap<String, Arc<Lease>>>,
    reconcile: Notify,
    forwarded_puts: AtomicU64,
    forwarded_queries: AtomicU64,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl State {
    fn client(&self) -> Option<Client> {
        self.client.borrow().clone()
    }

    fn set_client(&self, client: Option<Client>) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        // streams belong to a connection: the reconcile loop reopens them on the next one
        for (_, stream) in self.streams.lock().unwrap().drain() {
            stream.task.abort();
        }
        self.client.send_replace(client);
        self.reconcile.notify_one();
    }

    /// Opens an upstream stream for every key some viewer watches, closes the ones nobody has watched for [`LINGER`].
    fn reconcile_streams(self: &Arc<Self>) {
        let Some(client) = self.client() else { return };
        let topics = self.topics.lock().unwrap().clone();
        let mut wanted = HashSet::new();
        for (expr, codec) in self.server.subscriptions() {
            let keys: Vec<String> = match keyexpr::new(&expr) {
                Ok(pattern) if pattern.is_wild() => topics.iter().filter(|topic| keyexpr::new(topic.as_str()).is_ok_and(|topic| pattern.intersects(topic))).cloned().collect(),
                _ => vec![expr],
            };
            wanted.extend(keys.into_iter().map(|key| (key, codec.clone())));
        }
        let now = Instant::now();
        let mut streams = self.streams.lock().unwrap();
        for id in &wanted {
            match streams.get_mut(id) {
                Some(stream) => stream.unwanted_since = None,
                None => {
                    let kind = id.1.as_ref().and_then(|codec| self.kinds.get(codec).copied());
                    let stats = Arc::new(StreamStats::default());
                    info!("pulling {} ({}) from the backend", id.0, id.1.as_deref().unwrap_or("raw"));
                    let task = tokio::spawn(run_stream(Arc::downgrade(self), client.clone(), id.clone(), kind, stats.clone()));
                    streams.insert(id.clone(), Stream { task, unwanted_since: None, stats });
                }
            }
        }
        streams.retain(|id, stream| {
            if wanted.contains(id) {
                return true;
            }
            let since = *stream.unwanted_since.get_or_insert(now);
            let keep = now.duration_since(since) < LINGER;
            if !keep {
                info!("no viewer watches {} ({}) any more: closed upstream", id.0, id.1.as_deref().unwrap_or("raw"));
                stream.task.abort();
            }
            keep
        });
    }

    /// Holds on the backend every lease a viewer holds here, and gives up the others; a lease the backend refuses
    /// or ends is ended for the viewer too.
    async fn reconcile_leases(self: &Arc<Self>) {
        let viewers: HashMap<String, Vec<String>> = self.server.leases().into_iter().collect();
        let mut held = self.leases.lock().await;
        let released: Vec<String> = held.keys().filter(|group| !viewers.contains_key(*group)).cloned().collect();
        for group in released {
            if let Some(lease) = held.remove(&group) {
                let _ = lease.release().await;
            }
        }
        let Some(client) = self.client() else { return };
        for (group, keys) in viewers {
            if held.contains_key(&group) {
                continue;
            }
            match client.lease(&group, Some(keys), None).await {
                Ok(lease) => {
                    let lease = Arc::new(lease);
                    held.insert(group.clone(), lease.clone());
                    tokio::spawn(watch_lease(Arc::downgrade(self), group, lease));
                }
                Err(error) => {
                    warn!("backend refused lease {group:?}: {error:#}");
                    self.server.expire_lease(&group, &format!("the backend refused it: {error:#}")).await;
                }
            }
        }
    }
}

/// When the backend ends a lease (not the relay releasing it), the viewer holding it loses it too.
async fn watch_lease(state: Weak<State>, group: String, lease: Arc<Lease>) {
    let reason = lease.wait_lost().await;
    let Some(state) = state.upgrade() else { return };
    if reason == "released" {
        return;
    }
    {
        let mut held = state.leases.lock().await;
        if held.get(&group).is_some_and(|current| Arc::ptr_eq(current, &lease)) {
            held.remove(&group);
        }
    }
    state.server.expire_lease(&group, &format!("backend: {reason}")).await;
}

/// Keeps a backend connection: on loss, every upstream stream closes and the relay reconnects.
async fn supervise(state: Weak<State>, mut client: Client) {
    let Some(strong) = state.upgrade() else { return };
    let querier = strong.router.declare_querier(format!("{}/{}/offer", zenoh_web::SIGNALLING_PREFIX, strong.backend_name)).await;
    drop(strong);
    let querier = match querier {
        Ok(querier) => querier,
        Err(error) => return warn!("watching the backend's signalling: {error}"),
    };
    loop {
        let why = backend_lost(&client, &querier).await;
        client.close().await;
        let Some(strong) = state.upgrade() else { return };
        warn!("backend {:?} lost ({why}); reconnecting", strong.backend_name);
        strong.set_client(None);
        let (router, name, options) = (strong.router.clone(), strong.backend_name.clone(), strong.client_options.clone());
        drop(strong);
        client = match connect(&router, &name, &options).await {
            Ok(client) => client,
            Err(error) => return warn!("{error:#}"),
        };
        let Some(strong) = state.upgrade() else { return };
        strong.set_client(Some(client.clone()));
    }
}

/// Resolves when the connection is gone: closed, the backend's signalling queryable left zenoh (it restarted or its
/// link dropped), or no ping answered for [`DEGRADED_LIMIT`].
async fn backend_lost(client: &Client, querier: &zenoh::query::Querier<'_>) -> &'static str {
    let mut ticker = tokio::time::interval(RECONCILE_INTERVAL);
    let mut degraded_since = None;
    loop {
        tokio::select! {
            _ = client.closed() => return "connection closed",
            _ = ticker.tick() => {}
        }
        if querier.matching_status().await.is_ok_and(|status| !status.matching()) {
            return "its zenoh-web left zenoh";
        }
        if client.state() == ConnectionState::Degraded {
            if degraded_since.get_or_insert_with(Instant::now).elapsed() > DEGRADED_LIMIT {
                return "no answer";
            }
        } else {
            degraded_since = None;
        }
    }
}

async fn reconcile_loop(state: Weak<State>) {
    let Some(strong) = state.upgrade() else { return };
    let (mut changes, mut client_changes) = (strong.server.changes(), strong.client.subscribe());
    drop(strong);
    let mut ticker = tokio::time::interval(RECONCILE_INTERVAL);
    loop {
        let Some(state) = state.upgrade() else { return };
        tokio::select! {
            _ = changes.changed() => {}
            _ = client_changes.changed() => {}
            _ = state.reconcile.notified() => {}
            _ = ticker.tick() => {}
        }
        state.reconcile_streams();
        state.reconcile_leases().await;
    }
}

/// Mirrors the backend's topics as liveliness tokens on the viewers' session (their listTopics) and expands viewers'
/// wildcard subscriptions over them.
async fn topics_loop(state: Weak<State>) {
    loop {
        let Some(strong) = state.upgrade() else { return };
        if let Some(client) = strong.client() {
            match client.list_topics("**", Some(strong.topic_probe_ms)).await {
                Ok(listed) => {
                    let listed: BTreeSet<String> = listed.into_iter().map(|topic| topic.key).collect();
                    let changed = *strong.topics.lock().unwrap() != listed;
                    if changed {
                        let mut tokens = strong.tokens.lock().unwrap();
                        tokens.retain(|key, _| listed.contains(key));
                        for key in &listed {
                            if !tokens.contains_key(key)
                                && let Ok(token) = zenoh::Wait::wait(strong.local.liveliness().declare_token(key.clone()))
                            {
                                tokens.insert(key.clone(), token);
                            }
                        }
                        debug!("backend topics: {listed:?}");
                        *strong.topics.lock().unwrap() = listed;
                        strong.reconcile.notify_one();
                    }
                }
                Err(error) => debug!("listing the backend's topics: {error:#}"),
            }
        }
        drop(strong);
        tokio::time::sleep(TOPICS_INTERVAL).await;
    }
}

/// Pulls one key from the backend until aborted, resubscribing after an error.
async fn run_stream(state: Weak<State>, client: Client, (key, codec): StreamId, kind: Option<CodecOutput>, stats: Arc<StreamStats>) {
    loop {
        let Some(strong) = state.upgrade() else { return };
        let (local, bitrate) = (strong.local.clone(), strong.upstream_max_bitrate);
        drop(strong);
        let result = match (kind, &codec) {
            (Some(CodecOutput::Video), Some(codec)) => pull_video(&client, &local, &key, codec, bitrate, &stats).await,
            (_, Some(codec)) => pull_data(&client, &local, &key, Some(codec), &stats).await,
            (_, None) => pull_data(&client, &local, &key, None, &stats).await,
        };
        if let Err(error) = result {
            warn!("upstream {key} ({}): {error:#}", codec.as_deref().unwrap_or("raw"));
        }
        tokio::time::sleep(RETRY).await;
    }
}

/// Raw topics and fields/data codecs: each message put as is on the viewers' session, the latest kept for late viewers.
async fn pull_data(client: &Client, local: &zenoh::Session, key: &str, codec: Option<&str>, stats: &StreamStats) -> Result<()> {
    let options = SubscribeOptions { codec: codec.map(str::to_owned), max_quality: codec.map(|_| 1.0), ..Default::default() };
    let mut subscription = client.subscribe(key, options).await?;
    let local_key = match codec {
        Some(codec) => format!("{}/{key}", codecs::prefix(codec)),
        None => key.to_owned(),
    };
    let publisher = local.declare_publisher(local_key).cache(CacheConfig::default().max_samples(1)).await.map_err(|error| anyhow!("{error}"))?;
    while let Some(message) = subscription.recv().await {
        if let Message::Data(message) = message {
            stats.messages.fetch_add(1, Ordering::Relaxed);
            publisher.put(message.bytes).attachment(OWN_PUT).await.map_err(|error| anyhow!("{error}"))?;
        }
    }
    Err(anyhow!("the subscription closed"))
}

/// A camera: the backend's access units decoded once (openh264, on a thread), the pictures put for the viewers'
/// encoders. A frame the decoder can't keep up with is dropped, and a keyframe asked for to resume.
async fn pull_video(client: &Client, local: &zenoh::Session, key: &str, codec: &str, bitrate: f64, stats: &Arc<StreamStats>) -> Result<()> {
    let options = SubscribeOptions { codec: Some(codec.to_owned()), max_quality: Some(1.0), max_bitrate: Some(bitrate), ..Default::default() };
    let mut subscription = client.subscribe(key, options).await?;
    let local_key = format!("{}/{key}", codecs::prefix(codec));
    let (units_tx, units_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(4);
    let (pictures_tx, mut pictures_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
    let decoder_stats = stats.clone();
    std::thread::Builder::new().name(format!("relay-decode {key}")).spawn(move || decode_thread(units_rx, pictures_tx, decoder_stats))?;
    let mut waiting_for_keyframe = false;
    loop {
        tokio::select! {
            message = subscription.recv() => match message {
                Some(Message::Video(frame)) => {
                    stats.messages.fetch_add(1, Ordering::Relaxed);
                    if waiting_for_keyframe && !frame.keyframe {
                        continue;
                    }
                    waiting_for_keyframe = false;
                    if units_tx.try_send(frame.data).is_err() {
                        stats.dropped_access_units.fetch_add(1, Ordering::Relaxed);
                        waiting_for_keyframe = true;
                        let _ = subscription.request_keyframe().await;
                    }
                }
                Some(_) => {}
                None => return Err(anyhow!("the subscription closed")),
            },
            picture = pictures_rx.recv() => {
                let picture = picture.context("the decoder stopped")?;
                local.put(&local_key, picture).await.map_err(|error| anyhow!("{error}"))?;
            }
        }
    }
}

fn decode_thread(units: std::sync::mpsc::Receiver<Vec<u8>>, pictures: tokio::sync::mpsc::Sender<Vec<u8>>, stats: Arc<StreamStats>) {
    use openh264::formats::YUVSource;
    let Ok(mut decoder) = openh264::decoder::Decoder::new() else { return warn!("openh264 decoder failed to start") };
    for unit in units {
        let picture = match decoder.decode(&unit) {
            Ok(Some(picture)) => picture,
            Ok(None) => continue,
            Err(error) => {
                debug!("decode: {error}");
                continue;
            }
        };
        // I420 wants even sizes; an odd edge row or column is dropped
        let (width, height) = picture.dimensions();
        let (width, height) = (width & !1, height & !1);
        let (y_stride, u_stride, v_stride) = picture.strides();
        let mut i420 = Vec::with_capacity(width * height * 3 / 2);
        for row in 0..height {
            i420.extend_from_slice(&picture.y()[row * y_stride..][..width]);
        }
        for (plane, stride) in [(picture.u(), u_stride), (picture.v(), v_stride)] {
            for row in 0..height / 2 {
                i420.extend_from_slice(&plane[row * stride..][..width / 2]);
            }
        }
        stats.pictures.fetch_add(1, Ordering::Relaxed);
        if pictures.blocking_send(codecs::picture_payload(width as u32, height as u32, &i420)).is_err() {
            return;
        }
    }
}

/// Viewers' puts (everything put on the viewers' session but the relay's own) go to the backend, one relay
/// publisher per key, in order.
async fn forward_puts(state: &Arc<State>) -> Result<JoinHandle<()>> {
    let (puts_tx, mut puts_rx) = tokio::sync::mpsc::channel::<zenoh::sample::Sample>(1024);
    let subscriber = state
        .local
        .declare_subscriber("**")
        .callback(move |sample| {
            if sample.attachment().is_none_or(|attachment| attachment.to_bytes() != OWN_PUT) && puts_tx.try_send(sample).is_err() {
                warn!("viewers' puts are backed up: one dropped");
            }
        })
        .await
        .map_err(|error| anyhow!("{error}"))?;
    let state = Arc::downgrade(state);
    Ok(tokio::spawn(async move {
        let _subscriber = subscriber;
        while let Some(sample) = puts_rx.recv().await {
            let Some(state) = state.upgrade() else { return };
            if let Err(error) = state.forward_put(&sample).await {
                warn!("forwarding a put on {}: {error:#}", sample.key_expr());
            }
        }
    }))
}

impl State {
    async fn forward_put(&self, sample: &zenoh::sample::Sample) -> Result<()> {
        let client = self.client().context("the backend is not connected")?;
        let generation = self.generation.load(Ordering::Acquire);
        let key = sample.key_expr().as_str();
        let mut publishers = self.publishers.lock().await;
        let stale = publishers.get(key).is_none_or(|(belongs_to, publisher)| *belongs_to != generation || publisher.tripped().is_some());
        if stale {
            let delivery = if sample.congestion_control() == CongestionControl::Block { Delivery::Reliable } else { Delivery::Latest };
            let options = PublisherOptions { delivery: Some(delivery), priority: Some(sample.priority() as u8), ..Default::default() };
            publishers.insert(key.to_owned(), (generation, client.publish(key, options).await?));
        }
        publishers[key].1.put(sample.payload().to_bytes()).await?;
        self.forwarded_puts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Viewers' queries (`get`) go to the backend.
async fn forward_queries(state: &Arc<State>) -> Result<JoinHandle<()>> {
    let queryable = state.local.declare_queryable("**").complete(false).await.map_err(|error| anyhow!("{error}"))?;
    let state = Arc::downgrade(state);
    Ok(tokio::spawn(async move {
        while let Ok(query) = queryable.recv_async().await {
            let Some(strong) = state.upgrade() else { return };
            let Some(client) = strong.client() else { continue };
            strong.forwarded_queries.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                match client.get(query.key_expr().as_str(), Duration::from_secs(2)).await {
                    Ok(replies) => {
                        for reply in replies {
                            let _ = match reply.key {
                                Some(key) if !reply.error => query.reply(key, reply.bytes).await,
                                _ => query.reply_err(reply.bytes).await,
                            };
                        }
                    }
                    Err(error) => {
                        let _ = query.reply_err(error.to_string()).await;
                    }
                }
            });
        }
    }))
}

/// A running relay. Clones share it.
#[derive(Clone)]
pub struct Relay {
    state: Arc<State>,
}

impl Relay {
    /// Starts configuring a relay for the backend whose zenoh-web server has `zenoh_signalling(backend_name)`.
    pub fn builder(backend_name: impl Into<String>) -> RelayBuilder {
        RelayBuilder {
            backend_name: backend_name.into(),
            backend_token: None,
            listen: Vec::new(),
            zenoh_config: None,
            upstream_ice_servers: None,
            upstream_max_bitrate: 8e6,
            topic_probe_ms: 0,
            viewers: Box::new(|builder| builder),
        }
    }

    /// The viewers' zenoh-web server.
    pub fn server(&self) -> &Server {
        &self.state.server
    }

    /// The router the backend connects to.
    pub fn router_session(&self) -> &zenoh::Session {
        &self.state.router
    }

    /// The viewers' routes ([`Server::router`]) plus `GET` [`STATS_PATH`].
    pub fn router(&self) -> axum::Router {
        let relay = self.clone();
        self.state.server.router().route(STATS_PATH, get(move || async move { Json(relay.stats().await) }))
    }

    /// The relay's state: the backend connection, its topics, what it pulls (and how much), viewers' subscriptions,
    /// forwarded puts and queries, and the backend's own stats of the relay's connection.
    pub async fn stats(&self) -> Value {
        let state = &self.state;
        let upstream: Vec<Value> = state
            .streams
            .lock()
            .unwrap()
            .iter()
            .map(|((key, codec), stream)| {
                json!({
                    "key": key,
                    "codec": codec,
                    "messages": stream.stats.messages.load(Ordering::Relaxed),
                    "pictures": stream.stats.pictures.load(Ordering::Relaxed),
                    "droppedAccessUnits": stream.stats.dropped_access_units.load(Ordering::Relaxed),
                    "closing": stream.unwanted_since.is_some(),
                })
            })
            .collect();
        let client = state.client();
        let backend_stats = match &client {
            Some(client) => client.stats().await.unwrap_or_else(|error| json!({"error": error.to_string()})),
            None => Value::Null,
        };
        let viewer_subscriptions: Vec<Value> = state.server.subscriptions().into_iter().map(|(key, codec)| json!({"key": key, "codec": codec})).collect();
        json!({
            "backend": {
                "name": state.backend_name,
                "connected": client.is_some(),
                "rttMs": client.as_ref().and_then(Client::rtt_ms),
                "topics": state.topics.lock().unwrap().iter().collect::<Vec<_>>(),
                "stats": backend_stats,
            },
            "upstream": upstream,
            "viewerSubscriptions": viewer_subscriptions,
            "leases": state.leases.lock().await.keys().collect::<Vec<_>>(),
            "forwardedPuts": state.forwarded_puts.load(Ordering::Relaxed),
            "forwardedQueries": state.forwarded_queries.load(Ordering::Relaxed),
        })
    }

    /// Serves [`router`](Self::router) on `addr` until `signal`, then [`shutdown`](Self::shutdown).
    pub async fn serve_with_shutdown(&self, addr: impl ToSocketAddrs, signal: impl Future<Output = ()> + Send + 'static) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await.context("binding the HTTP listener")?;
        info!("zenoh-web-relay listening on http://{}", listener.local_addr()?);
        let served = axum::serve(listener, self.router()).with_graceful_shutdown(signal).await;
        self.shutdown().await?;
        served.context("HTTP server")
    }

    /// Fires viewers' deadmen (forwarded to the backend), closes the viewers' server, the backend connection and the
    /// zenoh sessions.
    pub async fn shutdown(&self) -> Result<()> {
        let state = &self.state;
        state.server.shutdown().await?;
        // the deadmen just put are forwarded before the connection closes
        tokio::time::sleep(Duration::from_millis(200)).await;
        for task in state.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
        let client = state.client();
        state.set_client(None);
        if let Some(client) = client {
            client.close().await;
        }
        state.local.close().await.map_err(|error| anyhow!("{error}"))?;
        state.router.close().await.map_err(|error| anyhow!("{error}"))?;
        Ok(())
    }
}
