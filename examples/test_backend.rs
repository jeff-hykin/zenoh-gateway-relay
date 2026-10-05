//! The e2e test's backend: a zenoh-gateway server with no HTTP listener whose zenoh dials out to the relay, answering
//! signalling over zenoh (`zenoh_signalling`). It publishes test cameras and data topics, prints what viewers put,
//! and reports its own work once a second, for the test to check the relay keeps it flat:
//!
//! - `cam/<n>` (`--cameras`): a moving pattern of `--size` at `--fps`, through the video encoding `test-pattern`;
//! - `data/counter`: `count <n>` at 10 Hz (raw), and `data/depth`: 64 KiB at 10 Hz (raw, or the fields encoding `test-fields`);
//! - prints `RECV <key> <payload>` for puts on `cmd/**`;
//! - prints `STATS {"subscriptions": [[key, encoding]...], "encoders": live encode sessions, "encodedFrames": total}`.
//!
//! `cargo run --release --example test_backend -- --connect tcp/127.0.0.1:7447 --name robot --token relay-secret`

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;
use zenoh_gateway::{Channel, DecodedFrame, EncodeOptions, EncodingOutput, EncodingSample, MessageEncoding, EncodedVideo, Fields, Grant, H264Encoder, Server, VideoEncoder, VideoFormat, VideoImage, VideoTarget, zenoh};

#[derive(Parser)]
struct Cli {
    /// the relay's zenoh endpoint
    #[arg(long)]
    connect: String,
    /// zenoh-gateway name (signalling over zenoh)
    #[arg(long)]
    name: String,
    /// the only token accepted (the relay's)
    #[arg(long)]
    token: String,
    #[arg(long, default_value_t = 2)]
    cameras: usize,
    #[arg(long, default_value = "640x480")]
    size: String,
    #[arg(long, default_value_t = 30.0)]
    fps: f64,
}

/// `u32 frame | u16 width | u16 height` -> an I420 picture with a moving gradient (cheap, so encoding dominates).
struct TestPattern;

impl MessageEncoding for TestPattern {
    fn name(&self) -> &str {
        "test-pattern"
    }

    fn output(&self) -> EncodingOutput {
        EncodingOutput::Video
    }

    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        let p = sample.payload;
        let frame = u32::from_le_bytes(p[0..4].try_into()?) as usize;
        let (width, height) = (u16::from_le_bytes([p[4], p[5]]) as usize, u16::from_le_bytes([p[6], p[7]]) as usize);
        let mut data = Vec::with_capacity(width * height * 3 / 2);
        for y in 0..height {
            data.extend((0..width).map(|x| ((x + y + frame * 4) % 220 + 16) as u8));
        }
        data.extend(std::iter::repeat_n(90u8, width * height / 4));
        data.extend(std::iter::repeat_n(170u8, width * height / 4));
        Ok(DecodedFrame::Video(VideoImage::i420(width as u32, height as u32, data)?))
    }
}

/// bytes -> `{size, data}` fields
struct TestFields;

impl MessageEncoding for TestFields {
    fn name(&self) -> &str {
        "test-fields"
    }

    fn output(&self) -> EncodingOutput {
        EncodingOutput::Fields
    }

    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        Ok(DecodedFrame::data(sample.payload.to_vec()))
    }

    fn encode(&self, frame: &DecodedFrame, _options: &EncodeOptions) -> Result<Vec<u8>> {
        let bytes = frame.downcast::<Vec<u8>>()?;
        Ok(Fields::new().scalar("size", bytes.len() as u32).array("data", bytes).build())
    }
}

#[derive(Default)]
struct Counters {
    live: AtomicI64,
    frames: AtomicU64,
}

/// openh264, counting encode sessions and frames.
struct Counting {
    inner: H264Encoder,
    counters: Arc<Counters>,
}

impl VideoEncoder for Counting {
    fn format(&self) -> VideoFormat {
        self.inner.format()
    }

    fn encode(&mut self, frame: &DecodedFrame, target: &VideoTarget) -> Result<Option<EncodedVideo>> {
        self.counters.frames.fetch_add(1, Ordering::Relaxed);
        self.inner.encode(frame, target)
    }
}

impl Drop for Counting {
    fn drop(&mut self) {
        self.counters.live.fetch_sub(1, Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    let cli = Cli::parse();
    let (width, height) = cli.size.split_once('x').map(|(w, h)| (w.parse::<u16>(), h.parse::<u16>())).ok_or_else(|| anyhow::anyhow!("--size WxH"))?;
    let (width, height) = (width?, height?);
    let mut config = zenoh::Config::default();
    for (key, value) in [("scouting/multicast/enabled", "false".to_owned()), ("listen/endpoints", "[]".to_owned()), ("connect/endpoints", format!("[{:?}]", cli.connect))] {
        config.insert_json5(key, &value).map_err(|error| anyhow::anyhow!("{error}"))?;
    }
    let session = zenoh::open(config).await.map_err(|error| anyhow::anyhow!("{error}"))?;
    let counters = Arc::new(Counters::default());
    let factory_counters = counters.clone();
    let token = cli.token.clone();
    let server = Server::builder()
        .session(session.clone())
        .encoding(TestPattern)
        .encoding(TestFields)
        .video_encoder(move || {
            factory_counters.live.fetch_add(1, Ordering::Relaxed);
            Box::new(Counting { inner: H264Encoder::default(), counters: factory_counters.clone() })
        })
        .zenoh_signalling(&cli.name)
        .authorize(move |given, _headers| if given == Some(token.as_str()) { Ok(Grant::all()) } else { Err("unknown token".into()) })
        .build()
        .await?;
    let mut keep = Vec::new();
    for camera in 0..cli.cameras {
        let key = format!("cam/{camera}");
        keep.push(session.liveliness().declare_token(key.clone()).await.map_err(|error| anyhow::anyhow!("{error}"))?);
        let session = session.clone();
        let interval = Duration::from_secs_f64(1.0 / cli.fps);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            for frame in 0..=u32::MAX {
                ticker.tick().await;
                let payload = [&frame.to_le_bytes()[..], &width.to_le_bytes(), &height.to_le_bytes()].concat();
                let _ = session.put(&key, payload).await;
            }
        });
    }
    for key in ["data/counter", "data/depth"] {
        keep.push(session.liveliness().declare_token(key).await.map_err(|error| anyhow::anyhow!("{error}"))?);
    }
    let data_session = session.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        for count in 0..=u64::MAX {
            ticker.tick().await;
            let _ = data_session.put("data/counter", format!("count {count}")).await;
            let depth: Vec<u8> = (0..65536u32).map(|index| (index as u64 + count) as u8).collect();
            let _ = data_session.put("data/depth", depth).await;
        }
    });
    let _commands = session
        .declare_subscriber("cmd/**")
        .callback(|sample| println!("RECV {} {}", sample.key_expr(), String::from_utf8_lossy(&sample.payload().to_bytes())))
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    println!("READY");
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let stats = serde_json::json!({
                    "subscriptions": server.subscriptions(),
                    "encoders": counters.live.load(Ordering::Relaxed),
                    "encodedFrames": counters.frames.load(Ordering::Relaxed),
                });
                println!("STATS {stats}");
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    drop(keep);
    server.shutdown().await
}
