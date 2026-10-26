//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID — Platform for Modern Intelligence and Data
//
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0;
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! Production compression for immutable columnar chunks.
//!
//! Compression happens at exactly one place in the pipeline — after a chunk's
//! values are materialized and before the segment is verified, synced, and
//! published:
//!
//! ```text
//! materialized values -> value encoding -> codec -> frame-validated payload
//! ```
//!
//! # Two layers
//!
//! * A **value encoding** rewrites the `[length][value]` entry stream into a more
//!   compact shape that still reproduces the original bytes exactly. PLOMID
//!   ships four, chosen by measurement against its own workloads rather than for
//!   breadth: [`ColumnEncoding::Raw`] (the version-1 layout, kept as the
//!   baseline), [`ColumnEncoding::DeltaBitpack`] for integer and time-series
//!   columns, [`ColumnEncoding::Dictionary`] for low-cardinality columns, and
//!   [`ColumnEncoding::Rle`] for columns with long runs.
//! * A **codec** is the existing byte-oriented [`ChunkCompression`] — `None` or
//!   the feature-gated ZSTD frame — applied to the encoded bytes. No second
//!   byte-level compressor is introduced.
//!
//! [`ColumnEncoding::Auto`] measures every applicable encoding on the chunk in
//! hand and keeps the smallest stored payload, so a chunk never grows because it
//! was encoded. Ties keep the earlier candidate, and `Raw` is always first, so
//! data that does not benefit keeps the legacy layout byte for byte.
//!
//! # Self-describing framing
//!
//! A chunk that uses a value encoding stores `codec(frame_header || body)`. The
//! frame records everything a reader needs to validate the payload's boundaries
//! without trusting the chunk header that points at it: encoding and codec
//! identities, both versions, the value count, the raw value-stream length, and
//! the body length. The frame lives inside the chunk payload, so the existing
//! CRC32C chunk checksum covers it; it is checked before any body byte is
//! interpreted, and the decoded value stream is then re-validated entry by entry
//! against the frame's declared length.
//!
//! `Raw` chunks carry no frame: their payload is the value stream itself, which
//! is exactly the layout version-1 segments already use, so any segment written
//! before this module existed still decodes.
//!
//! # Safety
//!
//! Every decode allocation is sized from bytes that were already bounds-checked,
//! and every declared length must agree with the bytes actually present. The
//! decoder rejects an unsupported codec or encoding, an unsupported frame or
//! encoding version, non-zero reserved bytes, impossible or inconsistent
//! lengths, truncation, trailing bytes, out-of-range dictionary indices, and
//! corruption, and it never allocates more than the format's declared chunk
//! bounds regardless of what a payload claims.

pub mod bitpack;
pub mod delta;
pub mod dictionary;
pub mod entries;
pub mod rle;

use crate::chunk::ChunkCompression;
use crate::column::ColumnType;
use crate::compression;
use crate::encoding::bitpack::{packed_len, width_for_max};
use crate::format;
use crate::layout::{corruption, get_slice, get_u64, get_u8, invalid, put_u64, put_u8, to_usize};
use crate::statistics::HllEstimator;
use plomid_core::Result;
use plomid_core::{
    CODEC_VERSION_NONE, CODEC_VERSION_ZSTD, COLUMNAR_ENCODING_FRAME_SIZE,
    COLUMNAR_ENCODING_FRAME_VERSION, COLUMNAR_ENCODING_MAGIC, COLUMNAR_MAX_CHUNK_RAW_SIZE,
    COLUMNAR_MAX_CHUNK_UNCOMPRESSED_SIZE, COLUMNAR_MAX_DICTIONARY_ENTRIES,
    ENCODING_DELTA_BITPACK_TAG, ENCODING_DICTIONARY_TAG, ENCODING_RAW_TAG, ENCODING_RLE_TAG,
    ENCODING_VERSION_DELTA_BITPACK, ENCODING_VERSION_DICTIONARY, ENCODING_VERSION_RAW,
    ENCODING_VERSION_RLE,
};
use std::cmp::Ordering;

/// Value encoding applied to a chunk before its codec runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ColumnEncoding {
    /// Choose the smallest applicable encoding for each chunk by measurement.
    Auto,
    /// Store the value stream verbatim, exactly as version-1 segments do.
    Raw,
    /// Run-length encode the value stream.
    Rle,
    /// Delta-encode and bit-pack fixed-width 64-bit integer values.
    DeltaBitpack,
    /// Store each distinct value once and bit-pack per-row indices.
    Dictionary,
}

impl ColumnEncoding {
    /// Encodings [`ColumnEncoding::Auto`] tries, in deterministic preference
    /// order. Earlier candidates win ties, so `Raw` wins when nothing shrinks.
    pub const CANDIDATES: [ColumnEncoding; 4] = [
        ColumnEncoding::Raw,
        ColumnEncoding::DeltaBitpack,
        ColumnEncoding::Dictionary,
        ColumnEncoding::Rle,
    ];

    /// Returns the persisted tag for a concrete encoding, or `None` for
    /// [`ColumnEncoding::Auto`], which is a writer policy and never persisted.
    #[must_use]
    pub fn tag(self) -> Option<u8> {
        match self {
            ColumnEncoding::Auto => None,
            ColumnEncoding::Raw => Some(ENCODING_RAW_TAG),
            ColumnEncoding::Rle => Some(ENCODING_RLE_TAG),
            ColumnEncoding::DeltaBitpack => Some(ENCODING_DELTA_BITPACK_TAG),
            ColumnEncoding::Dictionary => Some(ENCODING_DICTIONARY_TAG),
        }
    }

    /// Decodes a persisted encoding tag.
    #[must_use]
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            ENCODING_RAW_TAG => Some(ColumnEncoding::Raw),
            ENCODING_RLE_TAG => Some(ColumnEncoding::Rle),
            ENCODING_DELTA_BITPACK_TAG => Some(ColumnEncoding::DeltaBitpack),
            ENCODING_DICTIONARY_TAG => Some(ColumnEncoding::Dictionary),
            _ => None,
        }
    }

    /// Returns the body version this encoding is written in.
    #[must_use]
    pub fn version(self) -> Option<u8> {
        match self {
            ColumnEncoding::Auto => None,
            ColumnEncoding::Raw => Some(ENCODING_VERSION_RAW),
            ColumnEncoding::Rle => Some(ENCODING_VERSION_RLE),
            ColumnEncoding::DeltaBitpack => Some(ENCODING_VERSION_DELTA_BITPACK),
            ColumnEncoding::Dictionary => Some(ENCODING_VERSION_DICTIONARY),
        }
    }

    /// Returns true when the encoding stores a framed body.
    #[must_use]
    pub fn is_framed(self) -> bool {
        !matches!(self, ColumnEncoding::Auto | ColumnEncoding::Raw)
    }

    /// Encodes one value stream, or `None` when the encoding cannot represent it.
    fn encode_body(self, raw: &[u8], value_count: u64) -> Result<Option<Vec<u8>>> {
        match self {
            ColumnEncoding::Auto | ColumnEncoding::Raw => Ok(None),
            ColumnEncoding::Rle => rle::encode(raw, value_count),
            ColumnEncoding::DeltaBitpack => delta::encode(raw, value_count),
            ColumnEncoding::Dictionary => dictionary::encode(raw, value_count),
        }
    }

    /// Decodes one framed body back into the value stream.
    fn decode_body(self, body: &[u8], value_count: u64, raw_len: u64) -> Result<Vec<u8>> {
        match self {
            ColumnEncoding::Auto | ColumnEncoding::Raw => Err(corruption(format!(
                "{self:?} chunks do not carry an encoded body"
            ))),
            ColumnEncoding::Rle => rle::decode(body, value_count, raw_len),
            ColumnEncoding::DeltaBitpack => delta::decode(body, value_count, raw_len),
            ColumnEncoding::Dictionary => dictionary::decode(body, value_count, raw_len),
        }
    }
}

/// Returns the byte-level frame version of a codec.
#[must_use]
pub fn codec_version(codec: ChunkCompression) -> u8 {
    match codec {
        ChunkCompression::None => CODEC_VERSION_NONE,
        ChunkCompression::Zstd => CODEC_VERSION_ZSTD,
    }
}

/// The self-describing header that every encoded chunk payload begins with.
///
/// The frame is stored inside the chunk payload, ahead of the encoded body, so
/// the chunk's CRC32C covers it and a reader can validate the payload's
/// boundaries before interpreting a single body byte.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodingFrame {
    /// Version of the frame layout itself.
    pub frame_version: u8,
    /// Value encoding the body was written in.
    pub encoding: ColumnEncoding,
    /// Version of that encoding's body layout.
    pub encoding_version: u8,
    /// Byte codec that was applied to `frame || body`.
    pub codec: ChunkCompression,
    /// Version of that codec's byte-level frame.
    pub codec_version: u8,
    /// Number of logical values (rows) the body reproduces.
    pub value_count: u64,
    /// Length in bytes of the value stream the body expands to.
    pub raw_len: u64,
    /// Length in bytes of the encoded body that follows this frame.
    pub encoded_len: u64,
}

impl EncodingFrame {
    /// Encoded size of a frame in bytes.
    pub const ENCODED_LEN: usize = COLUMNAR_ENCODING_FRAME_SIZE;

    /// Encodes the frame into its fixed-size little-endian image.
    #[must_use]
    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut bytes = [0_u8; Self::ENCODED_LEN];
        bytes[plomid_core::ENCODING_FRAME_OFF_MAGIC..plomid_core::ENCODING_FRAME_OFF_FRAME_VERSION]
            .copy_from_slice(&COLUMNAR_ENCODING_MAGIC);
        put_u8(
            &mut bytes,
            plomid_core::ENCODING_FRAME_OFF_FRAME_VERSION,
            self.frame_version,
        );
        put_u8(
            &mut bytes,
            plomid_core::ENCODING_FRAME_OFF_ENCODING,
            self.encoding.tag().unwrap_or(ENCODING_RAW_TAG),
        );
        put_u8(
            &mut bytes,
            plomid_core::ENCODING_FRAME_OFF_ENCODING_VERSION,
            self.encoding_version,
        );
        put_u8(
            &mut bytes,
            plomid_core::ENCODING_FRAME_OFF_CODEC,
            self.codec.tag(),
        );
        put_u8(
            &mut bytes,
            plomid_core::ENCODING_FRAME_OFF_CODEC_VERSION,
            self.codec_version,
        );
        put_u64(
            &mut bytes,
            plomid_core::ENCODING_FRAME_OFF_VALUE_COUNT,
            self.value_count,
        );
        put_u64(
            &mut bytes,
            plomid_core::ENCODING_FRAME_OFF_RAW_LEN,
            self.raw_len,
        );
        put_u64(
            &mut bytes,
            plomid_core::ENCODING_FRAME_OFF_ENCODED_LEN,
            self.encoded_len,
        );
        bytes
    }

    /// Decodes and validates a frame image.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let frame = get_slice(bytes, 0, Self::ENCODED_LEN, "encoding frame")?;
        if frame
            [plomid_core::ENCODING_FRAME_OFF_MAGIC..plomid_core::ENCODING_FRAME_OFF_FRAME_VERSION]
            != COLUMNAR_ENCODING_MAGIC
        {
            return Err(corruption("encoding frame magic does not match"));
        }
        let frame_version = get_u8(
            frame,
            plomid_core::ENCODING_FRAME_OFF_FRAME_VERSION,
            "encoding frame version",
        )?;
        if frame_version != COLUMNAR_ENCODING_FRAME_VERSION {
            return Err(plomid_core::PlomidError::new(
                plomid_core::ErrorKind::Unsupported,
                format!("unsupported encoding frame version {frame_version}"),
            ));
        }
        let encoding_tag = get_u8(
            frame,
            plomid_core::ENCODING_FRAME_OFF_ENCODING,
            "encoding tag",
        )?;
        let encoding = ColumnEncoding::from_tag(encoding_tag)
            .ok_or_else(|| corruption(format!("unknown value encoding tag {encoding_tag}")))?;
        let codec_tag = get_u8(frame, plomid_core::ENCODING_FRAME_OFF_CODEC, "codec tag")?;
        let codec = ChunkCompression::from_tag(codec_tag)
            .ok_or_else(|| corruption(format!("unknown codec tag {codec_tag}")))?;
        for offset in [
            plomid_core::ENCODING_FRAME_OFF_RESERVED0,
            plomid_core::ENCODING_FRAME_OFF_RESERVED1,
            plomid_core::ENCODING_FRAME_OFF_RESERVED2,
        ] {
            if get_u8(frame, offset, "reserved byte")? != 0 {
                return Err(corruption("encoding frame reserved byte is not zero"));
            }
        }
        Ok(Self {
            frame_version,
            encoding,
            encoding_version: get_u8(
                frame,
                plomid_core::ENCODING_FRAME_OFF_ENCODING_VERSION,
                "encoding version",
            )?,
            codec,
            codec_version: get_u8(
                frame,
                plomid_core::ENCODING_FRAME_OFF_CODEC_VERSION,
                "codec version",
            )?,
            value_count: get_u64(
                frame,
                plomid_core::ENCODING_FRAME_OFF_VALUE_COUNT,
                "value count",
            )?,
            raw_len: get_u64(frame, plomid_core::ENCODING_FRAME_OFF_RAW_LEN, "raw length")?,
            encoded_len: get_u64(
                frame,
                plomid_core::ENCODING_FRAME_OFF_ENCODED_LEN,
                "encoded length",
            )?,
        })
    }

    /// Checks this frame against the framing that referenced it.
    fn validate_against(
        &self,
        encoding: ColumnEncoding,
        codec: ChunkCompression,
        value_count: u64,
        declared_uncompressed: u64,
    ) -> Result<()> {
        if self.encoding != encoding {
            return Err(corruption(format!(
                "encoding frame declares {:?} but the chunk header declares {encoding:?}",
                self.encoding
            )));
        }
        if self.codec != codec {
            return Err(corruption(format!(
                "encoding frame declares codec {:?} but the chunk header declares {codec:?}",
                self.codec
            )));
        }
        if self.encoding_version != encoding.version().unwrap_or(self.encoding_version) {
            return Err(plomid_core::PlomidError::new(
                plomid_core::ErrorKind::Unsupported,
                format!(
                    "unsupported {:?} body version {}",
                    self.encoding, self.encoding_version
                ),
            ));
        }
        if self.codec_version != codec_version(codec) {
            return Err(plomid_core::PlomidError::new(
                plomid_core::ErrorKind::Unsupported,
                format!("unsupported {codec:?} codec version {}", self.codec_version),
            ));
        }
        if self.value_count != value_count {
            return Err(corruption(format!(
                "encoding frame holds {} values but the chunk header declares {value_count}",
                self.value_count
            )));
        }
        let body_len = declared_uncompressed
            .checked_sub(Self::ENCODED_LEN as u64)
            .ok_or_else(|| corruption("chunk is smaller than its encoding frame"))?;
        if self.encoded_len != body_len {
            return Err(corruption(format!(
                "encoding frame declares {} body bytes but {body_len} are framed",
                self.encoded_len
            )));
        }
        if self.raw_len > COLUMNAR_MAX_CHUNK_RAW_SIZE {
            return Err(corruption(format!(
                "encoding frame declares {} value-stream bytes, beyond this format's limit",
                self.raw_len
            )));
        }
        // Every framed encoding reproduces at least a four-byte length prefix per
        // value, so a value count above this bound is impossible for a well-formed
        // body. Rejecting it keeps every later allocation bounded by `raw_len`.
        let minimum = u64::try_from(format::COLUMNAR_VALUE_LENGTH_PREFIX)
            .map_err(|_| corruption("value length prefix does not fit in u64"))?
            .checked_mul(value_count)
            .ok_or_else(|| corruption("declared value count overflows"))?;
        if self.raw_len < minimum {
            return Err(corruption(format!(
                "encoding frame declares {} value-stream bytes for {value_count} values",
                self.raw_len
            )));
        }
        Ok(())
    }
}

/// One chunk that has been value-encoded and passed through a codec.
#[derive(Clone, Debug)]
pub struct EncodedChunk {
    /// Encoding the stored payload uses.
    pub encoding: ColumnEncoding,
    /// Version of that encoding's body layout.
    pub encoding_version: u8,
    /// Codec applied to the encoded bytes.
    pub codec: ChunkCompression,
    /// Version of the codec's byte-level frame.
    pub codec_version: u8,
    /// Number of logical values (rows) the payload holds.
    pub value_count: u64,
    /// Length in bytes of the value stream the payload reproduces.
    pub raw_len: u64,
    /// Length in bytes of what the codec was given.
    pub uncompressed_len: u64,
    /// Length in bytes of the encoded body, `0` for [`ColumnEncoding::Raw`].
    pub encoded_len: u64,
    /// Bytes to persist for this chunk.
    pub payload: Vec<u8>,
}

impl EncodedChunk {
    /// Returns true when the payload carries an encoding frame.
    #[must_use]
    pub fn is_framed(&self) -> bool {
        self.encoding.is_framed()
    }

    /// Returns the number of bytes this chunk stores.
    #[must_use]
    pub fn stored_len(&self) -> usize {
        self.payload.len()
    }
}

/// Value-encodes and codecs one chunk's value stream.
///
/// `raw` is the chunk's value stream (`[length][value]` entries in row order)
/// and `value_count` is the exact number of entries it must hold. The returned
/// payload is the exact byte range to persist in the chunk table.
///
/// Encoding choice is per chunk and never grows it:
///
/// * [`ColumnEncoding::Raw`] stores the value stream verbatim;
/// * [`ColumnEncoding::Auto`] measures every applicable encoding and keeps the
///   smallest payload;
/// * a specific encoding prefers itself, and falls back to
///   [`ColumnEncoding::Raw`] when it does not apply to the chunk (a
///   `DeltaBitpack` setting over a string column, for example) or when applying
///   it would store more bytes than the raw stream would.
///
/// Ties keep the earlier candidate, and `Raw` is always last resort, so a column
/// whose values do not fit the configured encoding is still storable rather than
/// failing the flush.
pub fn encode_chunk(
    raw: &[u8],
    value_count: u64,
    encoding: ColumnEncoding,
    codec: ChunkCompression,
) -> Result<EncodedChunk> {
    entries::validate_stream(raw, value_count)?;
    if !compression::is_compression_available(codec) {
        return Err(invalid(
            "the requested chunk codec is not available in this build",
        ));
    }

    let preferred = [encoding, ColumnEncoding::Raw];
    let candidates: &[ColumnEncoding] = match encoding {
        ColumnEncoding::Auto => &ColumnEncoding::CANDIDATES,
        ColumnEncoding::Raw => &preferred[..1],
        _ => &preferred,
    };

    let mut best: Option<EncodedChunk> = None;
    for candidate in candidates {
        let Some(encoded) = try_candidate(raw, value_count, *candidate, codec)? else {
            continue;
        };
        // Strictly smaller wins, so the first candidate of equal size is kept
        // and `Raw` — always first for `Auto`, always last for a specific
        // encoding — is the encoding of record whenever nothing wins.
        if best
            .as_ref()
            .is_none_or(|current| encoded.payload.len() < current.payload.len())
        {
            best = Some(encoded);
        }
    }
    best.ok_or_else(|| {
        invalid(format!(
            "the requested {encoding:?} encoding cannot represent this chunk"
        ))
    })
}

/// Produces one candidate encoding, or `None` when it does not apply.
fn try_candidate(
    raw: &[u8],
    value_count: u64,
    encoding: ColumnEncoding,
    codec: ChunkCompression,
) -> Result<Option<EncodedChunk>> {
    let raw_len = u64::try_from(raw.len())
        .map_err(|_| corruption("chunk value stream length exceeds u64"))?;
    if encoding == ColumnEncoding::Raw {
        check_uncompressed_bound(raw_len)?;
        let payload = compression::compress(raw, codec)?;
        return Ok(Some(EncodedChunk {
            encoding,
            encoding_version: encoding.version().unwrap_or(ENCODING_VERSION_RAW),
            codec,
            codec_version: codec_version(codec),
            value_count,
            raw_len,
            uncompressed_len: raw_len,
            encoded_len: 0,
            payload,
        }));
    }

    let Some(body) = encoding.encode_body(raw, value_count)? else {
        return Ok(None);
    };
    let encoded_len =
        u64::try_from(body.len()).map_err(|_| corruption("encoded body length exceeds u64"))?;
    let frame = EncodingFrame {
        frame_version: COLUMNAR_ENCODING_FRAME_VERSION,
        encoding,
        encoding_version: encoding
            .version()
            .ok_or_else(|| invalid("only concrete encodings produce framed chunks"))?,
        codec,
        codec_version: codec_version(codec),
        value_count,
        raw_len,
        encoded_len,
    };
    let mut uncompressed = Vec::with_capacity(EncodingFrame::ENCODED_LEN + body.len());
    uncompressed.extend_from_slice(&frame.encode());
    uncompressed.extend_from_slice(&body);
    let uncompressed_len = u64::try_from(uncompressed.len())
        .map_err(|_| corruption("framed payload length exceeds u64"))?;
    check_uncompressed_bound(uncompressed_len)?;
    let payload = compression::compress(&uncompressed, codec)?;
    Ok(Some(EncodedChunk {
        encoding,
        encoding_version: frame.encoding_version,
        codec,
        codec_version: frame.codec_version,
        value_count,
        raw_len,
        uncompressed_len,
        encoded_len,
        payload,
    }))
}

/// Rejects a payload that would exceed the format's per-chunk bound.
fn check_uncompressed_bound(length: u64) -> Result<()> {
    if length > COLUMNAR_MAX_CHUNK_UNCOMPRESSED_SIZE {
        return Err(invalid(format!(
            "chunk payload of {length} bytes exceeds the {COLUMNAR_MAX_CHUNK_UNCOMPRESSED_SIZE}-byte chunk limit"
        )));
    }
    Ok(())
}

/// Writer-side preference for one column's value encoding.
///
/// [`ColumnEncoding`] is the per-chunk persisted choice; this is the
/// per-column flush policy that selects it. [`ColumnEncodingPreference::Auto`]
/// (the default) means "no per-column override": the flush falls back to the
/// segment-wide [`FlushConfig::encoding`](crate::flush::FlushConfig::encoding)
/// for that column. Any other variant pins the column to that encoding, which
/// still falls back to [`ColumnEncoding::Raw`] for a chunk it cannot
/// represent, exactly like an explicit [`ColumnEncoding`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum ColumnEncodingPreference {
    /// No per-column override; use the segment-wide encoding.
    #[default]
    Auto,
    /// Store the value stream verbatim.
    Raw,
    /// Prefer run-length encoding, falling back to raw.
    Rle,
    /// Prefer delta-plus-bitpack, falling back to raw.
    DeltaBitpack,
    /// Prefer dictionary encoding, falling back to raw.
    Dictionary,
}

impl ColumnEncodingPreference {
    /// Resolves the preference against the segment-wide fallback.
    ///
    /// [`ColumnEncodingPreference::Auto`] yields `fallback`; any concrete
    /// preference yields its own encoding.
    #[must_use]
    pub fn resolve(self, fallback: ColumnEncoding) -> ColumnEncoding {
        match self {
            ColumnEncodingPreference::Auto => fallback,
            ColumnEncodingPreference::Raw => ColumnEncoding::Raw,
            ColumnEncodingPreference::Rle => ColumnEncoding::Rle,
            ColumnEncodingPreference::DeltaBitpack => ColumnEncoding::DeltaBitpack,
            ColumnEncodingPreference::Dictionary => ColumnEncoding::Dictionary,
        }
    }

    /// Returns the concrete encoding, or `None` for
    /// [`ColumnEncodingPreference::Auto`], which never persists.
    #[must_use]
    pub fn as_encoding(self) -> Option<ColumnEncoding> {
        match self {
            ColumnEncodingPreference::Auto => None,
            ColumnEncodingPreference::Raw => Some(ColumnEncoding::Raw),
            ColumnEncodingPreference::Rle => Some(ColumnEncoding::Rle),
            ColumnEncodingPreference::DeltaBitpack => Some(ColumnEncoding::DeltaBitpack),
            ColumnEncodingPreference::Dictionary => Some(ColumnEncoding::Dictionary),
        }
    }
}

impl From<ColumnEncoding> for ColumnEncodingPreference {
    /// Maps a chunk encoding to its column-policy counterpart.
    ///
    /// [`ColumnEncoding::Auto`] maps to [`ColumnEncodingPreference::Auto`];
    /// every concrete encoding maps to the preference that pins it.
    fn from(encoding: ColumnEncoding) -> Self {
        match encoding {
            ColumnEncoding::Auto => ColumnEncodingPreference::Auto,
            ColumnEncoding::Raw => ColumnEncodingPreference::Raw,
            ColumnEncoding::Rle => ColumnEncodingPreference::Rle,
            ColumnEncoding::DeltaBitpack => ColumnEncodingPreference::DeltaBitpack,
            ColumnEncoding::Dictionary => ColumnEncodingPreference::Dictionary,
        }
    }
}

/// Cheap per-column structural hints folded during the flush.
///
/// The flush already walks every value once to derive statistics; these hints
/// ride that same pass (see `collect_column_statistics` in `flush.rs`), so
/// adaptive selection costs no second scan. The hints are advisory only: they
/// reorder trials and skip provably doomed candidates, but every trial still
/// measures its real stored size and decoding still validates every byte, so a
/// wrong hint can only cost a missed saving, never wrong data.
///
/// The hints describe the whole column while encoding trials run per chunk, so
/// chunk shapes that diverge from their column (a sorted run inside an
/// otherwise random column) simply trial in a less-than-ideal order. Worst
/// case is extra trial CPU, bounded by the legacy brute-force cost.
#[derive(Clone, Debug)]
pub struct EncodingHints {
    /// Rows folded, NULLs included.
    pub rows: u64,
    /// Rows whose null bit is set.
    pub nulls: u64,
    /// Number of runs of adjacent equal entries (NULLs compare as empty).
    pub run_count: u64,
    /// True when values arrived in non-decreasing order (vacuously true when
    /// fewer than two values were observed).
    pub sorted_ascending: bool,
    /// True when every observed value had the same byte length.
    pub width_constant: bool,
    /// Byte length of the first observed value, when any row was folded.
    pub first_len: Option<usize>,
    /// Sum of non-NULL value bytes folded.
    pub total_value_bytes: u64,
    /// Live distinct-value sketch over non-NULL values, if any were seen.
    pub distinct_estimator: Option<HllEstimator>,
    /// Value bytes of the previously folded row (empty for NULL).
    last: Vec<u8>,
    /// True once at least one row has been folded.
    has_last: bool,
}

impl EncodingHints {
    /// Creates empty hints: zero rows, order and width vacuously true.
    #[must_use]
    pub fn new() -> Self {
        Self {
            rows: 0,
            nulls: 0,
            run_count: 0,
            sorted_ascending: true,
            width_constant: true,
            first_len: None,
            total_value_bytes: 0,
            distinct_estimator: None,
            last: Vec::new(),
            has_last: false,
        }
    }

    /// Folds one row into the hints.
    ///
    /// `None` denotes a NULL row, which contributes a zero-length entry to the
    /// persisted stream and compares as empty for run and order tracking.
    /// Integer pairs compare numerically; every other shape compares by its
    /// raw bytes, matching the order the statistics layer proves bounds with.
    pub fn observe(&mut self, value: Option<&[u8]>, column_type: ColumnType) {
        self.rows = self.rows.saturating_add(1);
        let bytes: &[u8] = match value {
            Some(bytes) => bytes,
            None => {
                self.nulls = self.nulls.saturating_add(1);
                &[]
            }
        };
        match self.first_len {
            None => self.first_len = Some(bytes.len()),
            Some(first) if first != bytes.len() => self.width_constant = false,
            Some(_) => {}
        }
        self.total_value_bytes = self.total_value_bytes.saturating_add(bytes.len() as u64);
        if self.has_last {
            if self.last.as_slice() != bytes {
                self.run_count = self.run_count.saturating_add(1);
            }
            if self.sorted_ascending
                && compare_hint_values(&self.last, bytes, column_type) == Ordering::Greater
            {
                self.sorted_ascending = false;
            }
        } else {
            self.run_count = self.run_count.saturating_add(1);
            self.has_last = true;
        }
        if value.is_some() {
            self.distinct_estimator
                .get_or_insert_with(HllEstimator::new)
                .observe(hint_value_hash(column_type, bytes));
        }
        self.last.clear();
        self.last.extend_from_slice(bytes);
    }

    /// Returns the fraction of folded rows that are NULL.
    #[must_use]
    pub fn null_fraction(&self) -> f64 {
        if self.rows == 0 {
            0.0
        } else {
            self.nulls as f64 / self.rows as f64
        }
    }

    /// Returns the number of folded non-NULL rows.
    #[must_use]
    pub fn non_null_rows(&self) -> u64 {
        self.rows.saturating_sub(self.nulls)
    }

    /// Returns the distinct-value estimate, when a non-NULL value was seen.
    #[must_use]
    pub fn distinct_estimate(&self) -> Option<u64> {
        self.distinct_estimator.as_ref().map(HllEstimator::estimate)
    }

    /// Returns the mean non-NULL value length in bytes.
    #[must_use]
    pub fn avg_value_len(&self) -> f64 {
        let non_null = self.non_null_rows();
        if non_null == 0 {
            0.0
        } else {
            self.total_value_bytes as f64 / non_null as f64
        }
    }

    /// Returns true when runs average at least four rows: RLE territory.
    #[must_use]
    pub fn runs_dense(&self) -> bool {
        self.rows > 1 && self.run_count.saturating_mul(4) <= self.rows
    }

    /// Returns true when distinct values are at most a quarter of non-NULL
    /// rows: dictionary territory.
    #[must_use]
    pub fn low_cardinality(&self) -> bool {
        match self.distinct_estimate() {
            Some(distinct) => {
                let non_null = self.non_null_rows();
                non_null > 0 && distinct.saturating_mul(4) <= non_null
            }
            None => false,
        }
    }

    /// Returns true when every entry is exactly one 64-bit integer image: the
    /// only shape [`ColumnEncoding::DeltaBitpack`] can represent.
    #[must_use]
    pub fn fixed_integer_width(&self) -> bool {
        self.width_constant && self.first_len == Some(8)
    }
}

impl Default for EncodingHints {
    fn default() -> Self {
        Self::new()
    }
}

/// Orders two consecutive hint values.
///
/// Integer images compare as `i64`; every other shape compares
/// lexicographically by bytes. NULL rows arrive as empty slices and sort
/// below every non-empty value, which is only used to clear the
/// sorted-ascending flag — never to prove a bound.
fn compare_hint_values(left: &[u8], right: &[u8], column_type: ColumnType) -> Ordering {
    if column_type == ColumnType::Integer {
        if let (Ok(left), Ok(right)) = (<[u8; 8]>::try_from(left), <[u8; 8]>::try_from(right)) {
            return i64::from_le_bytes(left).cmp(&i64::from_le_bytes(right));
        }
    }
    left.cmp(right)
}

/// Hashes hint bytes exactly the way the statistics layer hashes persisted
/// values (tag-prefixed FNV-1a finished with a SplitMix64 avalanche), so hint
/// distinct estimates agree with [`ColumnStatistics`](crate::statistics::ColumnStatistics).
fn hint_value_hash(column_type: ColumnType, bytes: &[u8]) -> u64 {
    let tag = match column_type {
        ColumnType::Integer => 1,
        ColumnType::Bytes => 2,
        ColumnType::String => 3,
        _ => 0,
    };
    hint_hash_tagged_bytes(tag, bytes)
}

/// FNV-1a over one tag byte plus `bytes`, avalanched for register spread.
fn hint_hash_tagged_bytes(tag: u8, bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    hash ^= u64::from(tag);
    hash = hash.wrapping_mul(PRIME);
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hint_avalanche(hash)
}

/// SplitMix64 final avalanche for full-bit diffusion.
fn hint_avalanche(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    x
}

/// Suggests the trial order for one chunk from its column's hints.
///
/// The order is deterministic in `(hints, ColumnType)`: the same column shape
/// always trials the same sequence, so [`ColumnEncoding::Auto`] stays
/// deterministic. The rules reuse the four existing on-disk codecs only:
///
/// * sorted fixed-width integers (timestamps included) trial
///   [`ColumnEncoding::DeltaBitpack`] first. A regular time series has one
///   repeated delta, which the codec stores once as its minimum and packs
///   every `delta - min` at zero bits: that minimum-plus-packed-residuals
///   layout *is* the delta-of-delta form, with no new codec.
/// * dense runs trial [`ColumnEncoding::Rle`] before
///   [`ColumnEncoding::Dictionary`], sparse runs do the reverse; the pair is
///   ordered by a cheap size estimate (run records versus dictionary plus
///   packed indices) instead of by trial.
/// * [`ColumnType::Bool`] trials [`ColumnEncoding::Dictionary`] first: two
///   distinct one-byte values cost one index bit per row, which *is* the
///   boolean bitpack form, with [`ColumnEncoding::Rle`] covering long runs.
///   Deltas never apply to booleans and trial last (skipped via
///   [`should_skip_encoding`]).
/// * narrow integer ranges need no new frame-of-reference codec either: for
///   ordered data [`ColumnEncoding::DeltaBitpack`]'s minimum-delta base packs
///   the same spread the frame would subtract, and unordered narrow data
///   routes to [`ColumnEncoding::Dictionary`] by the cardinality estimate.
/// * featureless data (unique values, no runs, unordered) keeps the legacy
///   [`ColumnEncoding::CANDIDATES`] order with [`ColumnEncoding::Raw`] first,
///   so ties still keep the version-1 layout byte for byte.
#[must_use]
pub fn suggest_encoding(hints: &EncodingHints, column_type: ColumnType) -> [ColumnEncoding; 4] {
    use ColumnEncoding::{DeltaBitpack, Dictionary, Raw, Rle};
    if hints.rows <= 1 {
        return [Raw, Rle, Dictionary, DeltaBitpack];
    }
    if !hints.runs_dense() && !hints.low_cardinality() {
        if column_type == ColumnType::Integer
            && hints.fixed_integer_width()
            && hints.sorted_ascending
        {
            return [DeltaBitpack, Rle, Dictionary, Raw];
        }
        return ColumnEncoding::CANDIDATES;
    }
    let (first, second) = if est_rle_stored(hints) <= est_dict_stored(hints) {
        (Rle, Dictionary)
    } else {
        (Dictionary, Rle)
    };
    if column_type == ColumnType::Integer && hints.fixed_integer_width() && hints.sorted_ascending {
        [DeltaBitpack, first, second, Raw]
    } else if column_type == ColumnType::Bool {
        [first, second, Raw, DeltaBitpack]
    } else {
        [first, second, DeltaBitpack, Raw]
    }
}

/// Estimates the stored size of an RLE body plus its frame.
///
/// `run_count` run records of `12 + avg_len` bytes behind the 8-byte run
/// count, inside the encoding frame. `avg_len` floors the true mean, so the
/// estimate errs low: it is used to order trials and to stop them early only
/// across an order-of-magnitude gap, never to pick a winner.
fn est_rle_stored(hints: &EncodingHints) -> u64 {
    let avg = hints.avg_value_len() as u64;
    hints
        .run_count
        .saturating_mul(12_u64.saturating_add(avg))
        .saturating_add(8)
        .saturating_add(EncodingFrame::ENCODED_LEN as u64)
}

/// Estimates the stored size of a dictionary body plus its frame.
///
/// Dictionary bytes for `distinct` values of mean length, the width byte, and
/// the packed per-row indices, inside the encoding frame. Like
/// [`est_rle_stored`] this errs low and only orders trials.
fn est_dict_stored(hints: &EncodingHints) -> u64 {
    let distinct = hints.distinct_estimate().unwrap_or(1).max(1);
    let avg = hints.avg_value_len() as u64;
    let width = width_for_max(distinct.saturating_sub(1));
    let packed = packed_len(
        hints
            .rows
            .saturating_mul(width as u64)
            .min(usize::MAX as u64) as usize,
    ) as u64;
    distinct
        .saturating_mul(4_u64.saturating_add(avg))
        .saturating_add(8)
        .saturating_add(1)
        .saturating_add(packed)
        .saturating_add(EncodingFrame::ENCODED_LEN as u64)
}

/// Returns true when trialing `candidate` is provably wasted.
///
/// * [`ColumnEncoding::DeltaBitpack`] cannot represent anything but 8-byte
///   entries, which the hints report exactly (`first_len == Some(8)`).
/// * [`ColumnEncoding::Dictionary`] declines past
///   `COLUMNAR_MAX_DICTIONARY_ENTRIES` distinct values inside
///   `dictionary::encode` after building a 64K-entry map; the hint estimate
///   skips that work. A column whose estimate claims every non-NULL value is
///   distinct provably stores more as a dictionary than verbatim (each value
///   once plus per-row indices plus framing), so the `None`-codec trial is
///   skipped as well.
/// * [`ColumnEncoding::Rle`] stores `12 + value` bytes per run against raw's
///   `4 + value`: with fewer than two rows per run on average it is strictly
///   larger before the codec runs, so the `None`-codec trial is skipped.
/// * single-row and empty chunks always store the frame (40 bytes) on top of
///   any body and trial raw only.
///
/// Skips are conservative: an underestimated distinct count or an overestimated
/// run density only misses a skip (extra trial CPU), while an overestimated
/// distinct count can only skip a dictionary trial that would have lost to raw
/// (the estimate claims unique-or-worse, where the dictionary is all overhead).
/// Under the ZSTD codec the size-based RLE and dictionary skips are disabled:
/// the byte codec can overturn uncompressed-size reasoning, so only
/// inapplicability skips apply there.
fn should_skip_encoding(
    candidate: ColumnEncoding,
    hints: &EncodingHints,
    column_type: ColumnType,
    codec: ChunkCompression,
) -> bool {
    match candidate {
        ColumnEncoding::Auto | ColumnEncoding::Raw => false,
        ColumnEncoding::DeltaBitpack => {
            hints.rows <= 1 || column_type != ColumnType::Integer || !hints.fixed_integer_width()
        }
        ColumnEncoding::Dictionary => {
            if hints.rows <= 1 {
                return true;
            }
            match hints.distinct_estimate() {
                Some(distinct) if distinct > COLUMNAR_MAX_DICTIONARY_ENTRIES as u64 => true,
                Some(distinct)
                    if codec == ChunkCompression::None
                        && hints.non_null_rows() > 0
                        && distinct >= hints.non_null_rows() =>
                {
                    true
                }
                Some(_) | None => false,
            }
        }
        ColumnEncoding::Rle => {
            if hints.rows <= 1 {
                return true;
            }
            codec == ChunkCompression::None
                && hints.rows > 0
                && hints.run_count.saturating_mul(2) > hints.rows
        }
    }
}

/// Value-encodes one chunk using column hints to order and prune trials.
///
/// This is the flush live path: `flush.rs` folds [`EncodingHints`] over the
/// column in its statistics pass and hands them here per chunk. `None` hints
/// behave exactly like [`encode_chunk`]. An explicit (non-`Auto`) encoding
/// keeps the legacy `[requested, Raw]` pair, skipping the requested trial
/// when [`should_skip_encoding`] proves it doomed. `Auto` trials
/// [`suggest_encoding`] order and stops early on a Steven's-law bound (see
/// below), keeping the smallest stored payload trialed — never larger than
/// raw, since `Raw` is always trialed unless an encoded candidate already
/// stores strictly fewer bytes (the `None` codec stores raw verbatim, so the
/// comparison needs no trial).
///
/// # Steven's-law early stop (uncompressed codec only)
///
/// Trial CPU has diminishing returns once the gap is an order of magnitude:
/// after each trial, trialing stops when every untried candidate's cheap
/// estimate exceeds twice the current best — the remaining codecs cannot close
/// a 2x gap in practice. When raw itself is best, trialing stops once every
/// remaining estimate sits within 5% of raw at or above it: a smaller saving
/// is not worth a more expensive decoder (raw decodes as a memcpy while every
/// framed encoding pays frame validation plus body expansion). Both bounds use
/// estimates that err low and apply only under [`ChunkCompression::None`];
/// under ZSTD every non-skipped candidate still trials.
pub fn encode_chunk_with_hints(
    raw: &[u8],
    value_count: u64,
    encoding: ColumnEncoding,
    codec: ChunkCompression,
    hints: Option<&EncodingHints>,
    column_type: ColumnType,
) -> Result<EncodedChunk> {
    let Some(hints) = hints else {
        return encode_chunk(raw, value_count, encoding, codec);
    };
    if encoding != ColumnEncoding::Auto {
        if should_skip_encoding(encoding, hints, column_type, codec) {
            return encode_chunk(raw, value_count, ColumnEncoding::Raw, codec);
        }
        return encode_chunk(raw, value_count, encoding, codec);
    }
    entries::validate_stream(raw, value_count)?;
    if !compression::is_compression_available(codec) {
        return Err(invalid(
            "the requested chunk codec is not available in this build",
        ));
    }
    let raw_len = u64::try_from(raw.len())
        .map_err(|_| corruption("chunk value stream length exceeds u64"))?;
    let order = suggest_encoding(hints, column_type);
    let mut best: Option<EncodedChunk> = None;
    for (index, candidate) in order.iter().enumerate() {
        if should_skip_encoding(*candidate, hints, column_type, codec) {
            continue;
        }
        if *candidate == ColumnEncoding::Raw && codec == ChunkCompression::None {
            // Raw under the `None` codec stores the stream verbatim: an
            // encoded candidate that already stores strictly fewer bytes wins
            // without a trial, and a tie keeps raw by skipping nothing here
            // (the trial runs and strictly-smaller-wins keeps raw).
            if best
                .as_ref()
                .is_some_and(|current| (current.payload.len() as u64) < raw_len)
            {
                continue;
            }
        }
        let Some(encoded) = try_candidate(raw, value_count, *candidate, codec)? else {
            continue;
        };
        if best
            .as_ref()
            .is_none_or(|current| encoded.payload.len() < current.payload.len())
        {
            best = Some(encoded);
        }
        if codec == ChunkCompression::None
            && early_stop(
                &best,
                &order[index + 1..],
                hints,
                column_type,
                codec,
                raw_len,
            )
        {
            break;
        }
    }
    best.ok_or_else(|| {
        invalid(format!(
            "the requested {encoding:?} encoding cannot represent this chunk"
        ))
    })
}

/// Checks the Steven's-law stop bound after one trial.
///
/// `remaining` are the still-untried candidates in trial order. Candidates
/// [`should_skip_encoding`] already rules out are ignored. The delta codec has
/// no cheap estimate (its width needs the delta spread), so any untried delta
/// trial vetoes the stop. Otherwise the stop needs an order-of-magnitude gap:
/// every remaining estimate above twice the best, or — when raw itself leads —
/// every remaining estimate within 5% of raw or worse, where the cheaper raw
/// decoder wins on merit.
fn early_stop(
    best: &Option<EncodedChunk>,
    remaining: &[ColumnEncoding],
    hints: &EncodingHints,
    column_type: ColumnType,
    codec: ChunkCompression,
    raw_len: u64,
) -> bool {
    let Some(best) = best.as_ref() else {
        return false;
    };
    let best_len = best.payload.len() as u64;
    let mut floor = u64::MAX;
    let mut veto = false;
    for candidate in remaining {
        if should_skip_encoding(*candidate, hints, column_type, codec) {
            continue;
        }
        let estimate = match candidate {
            ColumnEncoding::Auto => return false,
            ColumnEncoding::Raw => raw_len,
            ColumnEncoding::Rle => est_rle_stored(hints),
            ColumnEncoding::Dictionary => est_dict_stored(hints),
            ColumnEncoding::DeltaBitpack => {
                veto = true;
                continue;
            }
        };
        floor = floor.min(estimate);
    }
    if veto {
        return false;
    }
    if floor == u64::MAX {
        return true;
    }
    if best.encoding == ColumnEncoding::Raw {
        // Raw leads: stop when nothing remaining promises more than a 5%
        // saving over raw (`floor >= 95% of raw`).
        floor.saturating_mul(100) >= raw_len.saturating_mul(95)
    } else {
        // An encoded candidate leads: stop only across a 2x gap.
        floor > best_len.saturating_mul(2)
    }
}

/// Decodes a persisted chunk payload back into its value stream.
///
/// `declared_uncompressed` and `value_count` come from the chunk header that
/// referenced the payload. Every bound is enforced before it is used: the
/// declared length is limited by the format, the codec is asked to produce at
/// most that many bytes, the resulting length must match the declaration
/// exactly, the frame (when present) must agree with the header on encoding,
/// codec, both versions, value count, and body length, and the decoded value
/// stream must then validate entry by entry against the frame's declared length.
pub fn decode_chunk(
    payload: &[u8],
    encoding: ColumnEncoding,
    codec: ChunkCompression,
    declared_uncompressed: u64,
    value_count: u64,
) -> Result<Vec<u8>> {
    if declared_uncompressed > COLUMNAR_MAX_CHUNK_UNCOMPRESSED_SIZE {
        return Err(corruption(format!(
            "chunk declares {declared_uncompressed} bytes, beyond this format's limit"
        )));
    }
    let declared = to_usize(declared_uncompressed, "chunk uncompressed size")?;
    let bytes = compression::decompress(payload, declared_uncompressed, codec)?;
    if bytes.len() != declared {
        return Err(corruption(format!(
            "chunk decompressed to {} bytes but declares {declared}",
            bytes.len()
        )));
    }

    if encoding == ColumnEncoding::Raw {
        entries::validate_stream(&bytes, value_count)?;
        return Ok(bytes);
    }
    if !encoding.is_framed() {
        return Err(invalid(
            "an automatic encoding cannot be decoded: a chunk must persist a concrete encoding",
        ));
    }

    let frame = EncodingFrame::decode(&bytes)?;
    frame.validate_against(encoding, codec, value_count, declared_uncompressed)?;
    let body = bytes
        .get(EncodingFrame::ENCODED_LEN..)
        .ok_or_else(|| corruption("chunk is smaller than its encoding frame"))?;
    let raw = encoding.decode_body(body, value_count, frame.raw_len)?;
    if raw.len() as u64 != frame.raw_len {
        return Err(corruption(format!(
            "encoding body expanded to {} bytes but the frame declares {}",
            raw.len(),
            frame.raw_len
        )));
    }
    entries::validate_stream(&raw, value_count)?;
    Ok(raw)
}
