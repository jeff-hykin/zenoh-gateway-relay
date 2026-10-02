//! The relay's side of each backend codec: same name, reading what the relay puts under `@relay/<codec>/<key>`.

use anyhow::{Result, ensure};
use zenoh_web::{Codec, CodecOutput, CodecSample, Compress, DecodedFrame, VideoImage};

/// Where the relay puts what it pulled through `codec` (see `Codec::key_prefix`).
pub fn prefix(codec: &str) -> String {
    format!("{}/{codec}", crate::RELAY_PREFIX)
}

/// A backend video codec: the relay decodes the backend's stream once and puts the pictures
/// (`u32 width | u32 height | I420`, little endian); the viewers' server re-encodes them per viewer.
pub struct RelayVideo {
    name: String,
    prefix: String,
}

impl RelayVideo {
    pub fn new(name: &str) -> Self {
        RelayVideo { name: name.to_owned(), prefix: prefix(name) }
    }
}

/// `u32 width | u32 height | I420` for [`RelayVideo`].
pub fn picture_payload(width: u32, height: u32, i420: &[u8]) -> Vec<u8> {
    [&width.to_le_bytes()[..], &height.to_le_bytes(), i420].concat()
}

impl Codec for RelayVideo {
    fn name(&self) -> &str {
        &self.name
    }

    fn output(&self) -> CodecOutput {
        CodecOutput::Video
    }

    fn key_prefix(&self) -> Option<&str> {
        Some(&self.prefix)
    }

    fn decode(&self, sample: &CodecSample<'_>) -> Result<DecodedFrame> {
        let payload = sample.payload;
        ensure!(payload.len() >= 8, "not a relay picture");
        let size = |at: usize| u32::from_le_bytes(payload[at..at + 4].try_into().unwrap());
        Ok(DecodedFrame::Video(VideoImage::i420(size(0), size(4), payload[8..].to_vec())?))
    }
}

/// A backend fields or data codec: its output at full quality, passed through unchanged (viewers'
/// bandwidth is then shared by Hz, not quality).
pub struct RelayData {
    name: String,
    prefix: String,
    output: CodecOutput,
}

impl RelayData {
    pub fn new(name: &str, output: CodecOutput) -> Self {
        RelayData { name: name.to_owned(), prefix: prefix(name), output }
    }
}

impl Codec for RelayData {
    fn name(&self) -> &str {
        &self.name
    }

    fn output(&self) -> CodecOutput {
        self.output
    }

    fn key_prefix(&self) -> Option<&str> {
        Some(&self.prefix)
    }

    /// The backend's own codecs pick per type; zstd is only kept when it shrinks a message.
    fn default_compress(&self) -> Compress {
        Compress::Zstd
    }

    fn decode(&self, sample: &CodecSample<'_>) -> Result<DecodedFrame> {
        Ok(DecodedFrame::data(sample.payload.to_vec()))
    }

    fn encode(&self, frame: &DecodedFrame, _quality: f64) -> Result<Vec<u8>> {
        Ok(frame.downcast::<Vec<u8>>()?.clone())
    }

    fn estimated_bytes(&self, payload_bytes: usize, _quality: f64) -> f64 {
        payload_bytes as f64
    }
}
