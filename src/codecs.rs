//! The relay's side of each backend message encoding: same name, reading what the relay puts under
//! `@relay/<encoding>/<key>`.

use anyhow::{Result, ensure};
use zenoh_web::{AudioPcm, Channel, Compress, DecodedFrame, EncodeOptions, EncodingOutput, EncodingSample, MessageEncoding, VideoImage};

/// Where the relay puts what it pulled through `codec` (see `MessageEncoding::key_prefix`).
pub fn prefix(codec: &str) -> String {
    format!("{}/{codec}", crate::RELAY_PREFIX)
}

/// A backend video encoding: the relay decodes the backend's stream once and puts the pictures
/// (`u32 width | u32 height | I420`, little endian); the viewers' server re-encodes them per viewer, on whichever video
/// channel each asks for.
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

impl MessageEncoding for RelayVideo {
    fn name(&self) -> &str {
        &self.name
    }

    fn output(&self) -> EncodingOutput {
        EncodingOutput::Video
    }

    fn key_prefix(&self) -> Option<&str> {
        Some(&self.prefix)
    }

    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        let payload = sample.payload;
        ensure!(payload.len() >= 8, "not a relay picture");
        let size = |at: usize| u32::from_le_bytes(payload[at..at + 4].try_into().unwrap());
        Ok(DecodedFrame::Video(VideoImage::i420(size(0), size(4), payload[8..].to_vec())?))
    }
}

/// A backend fields or data encoding: its output at full quality with no encodeOptions, passed through unchanged
/// (viewers' bandwidth is then shared by Hz, not quality).
pub struct RelayData {
    name: String,
    prefix: String,
    output: EncodingOutput,
}

impl RelayData {
    pub fn new(name: &str, output: EncodingOutput) -> Self {
        RelayData { name: name.to_owned(), prefix: prefix(name), output }
    }
}

impl MessageEncoding for RelayData {
    fn name(&self) -> &str {
        &self.name
    }

    fn output(&self) -> EncodingOutput {
        self.output
    }

    fn key_prefix(&self) -> Option<&str> {
        Some(&self.prefix)
    }

    /// The backend's own encodings pick per type; zstd is only kept when it shrinks a message.
    fn default_compress(&self) -> Compress {
        Compress::Zstd
    }

    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        Ok(DecodedFrame::data(sample.payload.to_vec()))
    }

    fn encode(&self, frame: &DecodedFrame, _options: &EncodeOptions) -> Result<Vec<u8>> {
        Ok(frame.downcast::<Vec<u8>>()?.clone())
    }

    fn estimated_bytes(&self, payload_bytes: usize, _options: &EncodeOptions) -> f64 {
        payload_bytes as f64
    }
}

/// A backend audio encoding: the relay decodes the backend's Opus once and puts the PCM (`u8 channels | i16 samples`,
/// interleaved, little endian, 48 kHz); the viewers' server encodes it to Opus again per viewer.
pub struct RelayAudio {
    name: String,
    prefix: String,
}

impl RelayAudio {
    pub fn new(name: &str) -> Self {
        RelayAudio { name: name.to_owned(), prefix: prefix(name) }
    }
}

/// `u8 channels | i16 samples` for [`RelayAudio`].
pub fn pcm_payload(channels: u8, samples: &[i16]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + samples.len() * 2);
    payload.push(channels);
    payload.extend(samples.iter().flat_map(|sample| sample.to_le_bytes()));
    payload
}

impl MessageEncoding for RelayAudio {
    fn name(&self) -> &str {
        &self.name
    }

    fn output(&self) -> EncodingOutput {
        EncodingOutput::Audio
    }

    fn key_prefix(&self) -> Option<&str> {
        Some(&self.prefix)
    }

    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        let (channels, samples) = sample.payload.split_first().ok_or_else(|| anyhow::anyhow!("not relay PCM"))?;
        let samples = samples.as_chunks::<2>().0.iter().map(|pair| i16::from_le_bytes(*pair)).collect();
        Ok(DecodedFrame::Audio(AudioPcm::new(48_000, *channels, samples)?))
    }
}
