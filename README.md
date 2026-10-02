# zenoh-web-relay

Fans one [zenoh-web](https://github.com/jeff-hykin/zenoh-web) backend (e.g. a robot) out to many browsers while
keeping the load off the backend. The relay's own CPU/GPU is spent instead: it runs on a machine where that is cheap.

```
browsers ──WebRTC──▶ zenoh-web-relay (public host)  ◀──WebRTC (1 stream per camera)── backend (robot, no inbound ports)
   HTTP signalling ──▶   │ zenoh router :7447       ◀──zenoh (backend dials out; signalling + nothing else)──┘
```

- **The backend needs no inbound ports.** Its zenoh dials out to the relay's zenoh router (tcp/tls). The relay
  signals to the backend's zenoh-web over that link: the backend's server is built with
  `ServerBuilder::zenoh_signalling("<name>")`, which answers offers on the queryable `zenoh-web/<name>/offer`, and
  the relay connects with `zenoh_web::client::Client::connect_zenoh` (zenoh-web SPEC "Signalling over zenoh"). The
  WebRTC media then flows from the backend to the relay's public address: the backend sends the first UDP packets.
- **Each camera is pulled once,** at best quality (`maxQuality: 1`, `maxBitrate` 8 Mbit/s by default), however
  many viewers watch it, so the backend runs one encoder per camera. Upstream subscriptions open with a camera's
  first viewer and close one second after its last.
- **The relay decodes once and re-encodes per quality bucket.** Each camera is decoded once (openh264) and the
  pictures feed the relay's embedded zenoh-web `Server` as a video codec of the same name, whose encode sessions
  are shared by viewers at similar grants and whose allocator fits each viewer's link. Re-encoding uses a hardware
  encoder when one works (`--video-encoder auto`: VideoToolbox, or GStreamer's `nvv4l2h264enc` / `nvh264enc` /
  VAAPI, from [zenoh-web-encoders](https://github.com/jeff-hykin/zenoh-web-encoders)), else openh264.
- **Audio codecs too:** the backend's Opus is decoded once (libopus) and the PCM feeds an audio codec of the same
  name, which the relay's server encodes to Opus per viewer.
- **Data topics are pulled once and fanned out.** Fields and data codecs (depth, point clouds) are passed through
  at full quality without re-encoding (zstd per message if it shrinks it); raw topics too.
- **Viewers' puts, queries and leases go to the backend** through the relay's one connection: a put becomes a put
  of the relay's publisher on that key (reliable if the viewer's was), a viewer's lease is taken on the backend by
  the relay (refused or ended there, it ends for the viewer too).
- **Auth is the relay's:** viewers present tokens to the relay (`--auth-file`, or an authorize hook when embedded);
  the relay presents its own token to the backend (`--backend-token`), so the backend's grant for the relay bounds
  every viewer.
- **listTopics** on the relay shows the backend's topics (refreshed every 3 s).
- **The backend may come and go.** The relay notices within a second when the backend's zenoh-web leaves zenoh
  (its signalling queryable is gone), or after 5 s of unanswered pings, reconnects when it is back, and pulls again
  what viewers still watch; viewers stay connected to the relay meanwhile.

## Command

```sh
zenoh-web-relay --listen tls/0.0.0.0:7447 --http 0.0.0.0:7448 --backend-name robot \
    --backend-token "$RELAY_TOKEN" --auth-file viewers.json5 \
    [--zenoh-config router.json5] [--ice-server turn:user:pass@turn.example.com:3478] \
    [--udp-ports 50000-50100] [--upstream-max-bitrate 8e6] [--video-encoder auto] [--serve ./viewer-page]
```

- `--listen` (repeatable): the router's endpoints for the backend. TLS needs certificates in `--zenoh-config`.
- `--http`: viewers' signalling (`POST /offer`, as for any zenoh-web server), static files (`--serve`), and
  `GET /zenoh-web-relay/stats` (the backend connection, what is pulled and how much, viewers' subscriptions,
  forwarded puts, and the backend's stats of the relay's connection).
- `--auth-file`: zenoh-web-cli's format, `{ tokens: { "<token>": "read" | "write" | "lease" | <grant> },
  leaseGroups: { "<group>": ["<key expr>"] } }`, re-read when it changes (removed or changed tokens are revoked).
- `--video-encoder`: `auto` (default), `software`, `videotoolbox` or `gstreamer`, as zenoh-web-cli's.
- `--ice-server` / `--udp-ports`: the viewers' side; `--upstream-ice-server`: the relay → backend connection
  (default: the backend's own ICE servers).

Viewers use the zenoh-web browser client unchanged: `connect("https://relay.example.com", { token })`, then subscribe
with the backend's codec names (`codec: "ros2-image"` etc.).

## Deployment example

Relay on a public host `relay.example.com` (open TCP 7447 for zenoh, TCP 7448 or a TLS reverse proxy for HTTP, and
the UDP range for WebRTC):

```json5
// router.json5: TLS, and zenoh user/password for the backend
{
  transport: {
    link: { tls: { listen_certificate: "/etc/relay/cert.pem", listen_private_key: "/etc/relay/key.pem" } },
    auth: { usrpwd: { user: "relay", password: "...", dictionary_file: "/etc/relay/zenoh-users.txt" } },
  },
}
```

```sh
zenoh-web-relay --zenoh-config router.json5 --listen tls/0.0.0.0:7447 --http 0.0.0.0:7448 \
    --backend-name robot --backend-token "$RELAY_TOKEN" --auth-file viewers.json5 --udp-ports 50000-50100
```

Backend (the robot): its zenoh-web server dials out with this zenoh config and answers signalling over zenoh, e.g.

```json5
{
  mode: "peer",
  connect: { endpoints: ["tls/relay.example.com:7447"] },
  transport: {
    link: { tls: { root_ca_certificate: "/etc/robot/relay-ca.pem" } },
    auth: { usrpwd: { user: "robot", password: "..." } },
  },
}
```

```rust
let server = zenoh_web::Server::builder()
    .zenoh_config_file("robot-zenoh.json5")?
    .zenoh_signalling("robot")
    .authorize(|token, _| if token == Some(RELAY_TOKEN) { Ok(zenoh_web::Grant::all()) } else { Err("unknown token".into()) })
    .build()
    .await?; // no bind(): no HTTP listener on the robot
```

or with the stock command: `zenoh-web --zenoh-config robot-zenoh.json5 --zenoh-signalling robot --no-http
--auth-file relay-token.json5` (zenoh-web-cli; the auth file holds the relay's token).

## Library

```rust
let relay = zenoh_web_relay::Relay::builder("robot")
    .listen("tls/0.0.0.0:7447")
    .zenoh_config(router_config)
    .backend_token("relay-secret")
    .upstream_max_bitrate(8e6)
    .viewers(move |server| {
        let server = server.authorize(my_hook).ice_servers(ice).udp_ports(50000..=50100);
        // hardware re-encoding, as --video-encoder auto
        match zenoh_web_relay::zenoh_web_encoders::select(zenoh_web_relay::zenoh_web_encoders::Backend::Auto) {
            Ok(selected) => match selected.factory { Some(factory) => server.video_encoder(factory), None => server },
            Err(_) => server,
        }
    })
    .build()
    .await?;                                 // waits for the backend's first connection
relay.serve_with_shutdown(("0.0.0.0", 7448), shutdown_signal).await?;
// or mount relay.router() in your axum app; relay.server() is the viewers' zenoh_web::Server, relay.stats().await
```

## Measured (test/e2e.js)

`deno task e2e` builds the relay and `examples/test_backend.rs` (a zenoh-web server with no HTTP listener whose
zenoh dials out to the relay: 2 cameras at 640x480 30 fps through a video codec, a raw data topic and a fields
topic at 10 Hz), then opens 1 and then 3 headless Chrome viewers. Each viewer subscribes to both cameras (shown in
`<video>` elements) and both data topics, and puts once. Apple M-series laptop, 10 s windows:

| viewers | backend subscriptions | backend encoders | backend frames encoded/s | backend CPU | relay CPU (openh264) | relay CPU (VideoToolbox) | each viewer |
|---|---|---|---|---|---|---|---|
| 1 | 4 (1 per camera + 2 data) | 2 | 54 | 7.3-9.2 % | 10-12 % | 6.2-6.6 % | 27 fps per camera decoded in `<video>`, data 8-10 Hz |
| 3 | 4 (1 per camera + 2 data) | 2 | 60 | 7.7-9.7 % | 14-17 % | 11.6-11.9 % | 30 fps per camera decoded in `<video>`, data 10 Hz |

(four runs: two with the relay re-encoding in software, two with `--video-encoder auto` picking VideoToolbox; CPU is %
of one core. The backend's work does not grow with viewers: the same 2 encoders and 60 frames/s; the 1-viewer window
still includes the first second's ramp to 30 fps. The relay's work does grow.)

It also checks the viewers' puts reach the backend, listTopics, that a bad or missing token is refused, and that the
backend's subscriptions close after the last viewer leaves.

## Limits

- **One backend per relay**, chosen by `--backend-name`. The viewers' codecs mirror the backend's at its first
  connection (the relay waits for it before serving viewers); codecs added on a reconnect are not picked up.
- **Decoding is software H.264 (openh264): video codecs must produce H.264.** A backend whose encoder sends VP8,
  VP9 or AV1 is not decoded. Audio is decoded at 48 kHz (the rate zenoh-web's Opus tracks use).
- When embedding, use the re-exported `zenoh_web_relay::zenoh_web` and `zenoh_web_relay::zenoh_web_encoders` (other
  revisions of them are different crates to cargo).
- **Wildcard viewer subscriptions** expand over the backend's listed topics (liveliness tokens by default; keys that
  only appear when published need `topic_probe_ms`), so a wildcard sees a new topic within ~3 s.
- **Data passes through at full quality**: viewers' allocators trade Hz, not size, for fields/data topics.
  Raw and data-codec topics are pulled with `delivery: "latest"`: a reliable viewer subscription is reliable from the
  relay, not end to end.
- **Deadmen live at the relay.** A viewer's deadman fires at the relay (its put is forwarded); if the relay's link
  to the backend dies, the backend sees one client (the relay) go and fires nothing of the viewers'. Use zenoh-side
  timeouts on the robot for safety-critical commands.
- **Leases**: the backend sees one client, the relay; viewers' leases are exclusive among the relay's viewers and
  forwarded so that the relay holds them on the backend (taken within ~0.5 s of the viewer's; a refusal ends the
  viewer's lease after it was granted). A put blocked by another backend client's lease is dropped without the
  viewer being told.
- **Queries** (`get`) are forwarded with a 2 s timeout.
- **NAT**: the backend opens the UDP path to the relay's public address; if the relay is itself behind NAT, give
  both sides a TURN server (`--upstream-ice-server` and the backend's `ice_servers`).

## Building

`cargo build --release` (binary `target/release/zenoh-web-relay`). zenoh-web is a git dependency at a pinned
revision plus its crates.io version, as in the other zenoh-web repos. There is no Nix flake yet; for one (crate2nix,
like the other repos): the build scripts of `openh264-sys2`, `zstd-sys` and `ring` (rustls) compile C/assembly, so a
cross build needs a C toolchain for the target, and nothing is needed at runtime beyond the system libraries. The
e2e test additionally needs Deno and downloads Chrome (astral).

## License

Copyright (c) 2026 Jeff Hykin. All rights reserved (see LICENSE); future releases are likely to be licensed under
the Apache License 2.0.
