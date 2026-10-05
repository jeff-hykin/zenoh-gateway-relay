//! The `zenoh-gateway-relay` command: [zenoh_gateway_relay::Relay] with an auth file and ICE options.

use clap::Parser;
use log::info;
use std::path::PathBuf;
use zenoh_gateway::IceServer;
use zenoh_gateway_relay::{Relay, auth::AuthFileTokens, zenoh_gateway};

#[derive(Parser, Debug)]
#[command(name = "zenoh-gateway-relay", version, about = "Fan one zenoh-gateway backend out to many browsers, keeping the load off the backend")]
struct Cli {
    /// zenoh endpoint the relay's router listens on for the backend, e.g. tls/0.0.0.0:7447 (repeatable).
    #[arg(long, required = true)]
    listen: Vec<String>,
    /// HTTP address for viewers (signalling, static files, /zenoh-gateway-relay/stats).
    #[arg(long, default_value = "0.0.0.0:7448")]
    http: String,
    /// The backend's zenoh-gateway name (its ServerBuilder::zenoh_signalling(name); zenoh-gateway-cli --zenoh-signalling).
    #[arg(long)]
    backend_name: String,
    /// The relay's token for the backend.
    #[arg(long)]
    backend_token: Option<String>,
    /// Viewers' tokens: a json5 file mapping tokens to read / write / lease / a grant (zenoh-gateway-cli's format);
    /// re-read when it changes. Without it every viewer may do everything the relay may.
    #[arg(long)]
    auth_file: Option<PathBuf>,
    /// STUN/TURN server for the viewers' connections, e.g. turn:user:pass@turn.example.com:3478 (repeatable).
    #[arg(long)]
    ice_server: Vec<String>,
    /// STUN/TURN server for the relay's connection to the backend (default: the backend's).
    #[arg(long)]
    upstream_ice_server: Vec<String>,
    /// UDP port (50000) or range (50000-50100) for the viewers' WebRTC, one port per viewer.
    #[arg(long)]
    udp_ports: Option<String>,
    /// zenoh config file (json5) for the router: TLS certificates, transport/auth/usrpwd, ...
    #[arg(long)]
    zenoh_config: Option<PathBuf>,
    /// maxBitrate of each camera pulled from the backend, bits/s.
    #[arg(long, default_value_t = 8e6)]
    upstream_max_bitrate: f64,
    /// Serve this directory over HTTP (a viewer page).
    #[arg(long)]
    serve: Option<PathBuf>,
    /// Re-encoding for viewers: auto (hardware if one works: VideoToolbox, or GStreamer's nvv4l2h264enc / nvh264enc /
    /// VAAPI; else software), software (openh264), videotoolbox or gstreamer.
    #[arg(long, default_value = "auto")]
    video_encoder: zenoh_dimos_codecs::encoders::Backend,
}

/// `turn:user:pass@host:port` -> the URL without `user:pass@`, and the credentials.
fn ice_server(arg: &str) -> IceServer {
    let (scheme, rest) = arg.split_once(':').unwrap_or((arg, ""));
    match rest.rsplit_once('@') {
        Some((credentials, host)) => {
            let (username, credential) = credentials.split_once(':').unwrap_or((credentials, ""));
            IceServer { urls: vec![format!("{scheme}:{host}")], username: username.into(), credential: credential.into() }
        }
        None => IceServer { urls: vec![arg.to_owned()], ..Default::default() },
    }
}

/// Resolves on SIGINT or SIGTERM.
async fn terminated() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("a SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("SIGINT"),
        _ = terminate.recv() => info!("SIGTERM"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = std::env::var("RUST_LOG").ok().filter(|filter| !filter.is_empty()).unwrap_or_else(|| "info,zenoh=warn,zenoh_ext=warn,zenoh_gateway=info,zenoh_gateway_relay=info,rtc=warn,webrtc=warn".to_owned());
    env_logger::Builder::new().parse_filters(&filter).init();
    let cli = Cli::parse();
    let video = zenoh_dimos_codecs::encoders::select(cli.video_encoder)?;
    info!("video encoder: {}", video.name);
    let auth = cli.auth_file.as_ref().map(AuthFileTokens::load).transpose()?;
    let mut builder = Relay::builder(&cli.backend_name).upstream_max_bitrate(cli.upstream_max_bitrate);
    for endpoint in &cli.listen {
        builder = builder.listen(endpoint);
    }
    if let Some(path) = &cli.zenoh_config {
        builder = builder.zenoh_config(zenoh_gateway::zenoh::Config::from_file(path).map_err(|error| anyhow::anyhow!("{}: {error}", path.display()))?);
    }
    if let Some(token) = &cli.backend_token {
        builder = builder.backend_token(token);
    }
    if !cli.upstream_ice_server.is_empty() {
        builder = builder.upstream_ice_servers(cli.upstream_ice_server.iter().map(|arg| ice_server(arg)));
    }
    let udp_ports = match &cli.udp_ports {
        Some(ports) => {
            let (low, high) = ports.split_once('-').unwrap_or((ports, ports));
            Some(low.trim().parse::<u16>()?..=high.trim().parse::<u16>()?)
        }
        None => None,
    };
    let (viewer_ice, serve, viewer_auth) = (cli.ice_server.iter().map(|arg| ice_server(arg)).collect::<Vec<_>>(), cli.serve.clone(), auth.clone());
    builder = builder.viewers(move |mut server| {
        server = server.ice_servers(viewer_ice);
        if let Some(ports) = udp_ports {
            server = server.udp_ports(ports);
        }
        if let Some(dir) = serve {
            server = server.serve_dir(dir);
        }
        if let Some(factory) = video.factory {
            server = server.video_encoder(factory);
        }
        match viewer_auth {
            Some(auth) => auth.apply(server),
            None => server,
        }
    });
    let relay = builder.build().await?;
    if let Some(auth) = auth {
        tokio::spawn(auth.watch(relay.server().clone()));
    }
    relay.serve_with_shutdown(cli.http.as_str(), terminated()).await
}
