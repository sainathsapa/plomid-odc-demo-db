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
// Moved from src/encoding/mod.rs inline #[cfg(test)].
use plomid_columnar::chunk::ChunkCompression;
use plomid_columnar::column::ColumnType;
use plomid_columnar::encoding::entries::append_entry;
use plomid_columnar::encoding::*;
use plomid_core::ErrorKind;
use plomid_core::Result;
use plomid_core::{COLUMNAR_ENCODING_FRAME_VERSION, ENCODING_VERSION_DELTA_BITPACK};

fn stream(values: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        append_entry(&mut out, value).expect("append");
    }
    out
}

fn int_stream(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        append_entry(&mut out, &value.to_le_bytes()).expect("append");
    }
    out
}

fn bytes_stream(values: &[Vec<u8>]) -> Vec<u8> {
    stream(&values.iter().map(Vec::as_slice).collect::<Vec<&[u8]>>())
}

/// Builds an unframed payload whose frame carries caller-chosen fields.
fn craft(encoding: ColumnEncoding, value_count: u64, raw_len: u64, body: &[u8]) -> Vec<u8> {
    let frame = EncodingFrame {
        frame_version: COLUMNAR_ENCODING_FRAME_VERSION,
        encoding,
        encoding_version: encoding.version().expect("concrete encoding"),
        codec: ChunkCompression::None,
        codec_version: codec_version(ChunkCompression::None),
        value_count,
        raw_len,
        encoded_len: body.len() as u64,
    };
    let mut out = Vec::new();
    out.extend_from_slice(&frame.encode());
    out.extend_from_slice(body);
    out
}

/// Datasets exercising the shapes the encodings target, with their row counts.
fn datasets() -> Vec<(&'static str, Vec<u8>, u64)> {
    let low_cardinality: Vec<&[u8]> = (0..512)
        .map(|i| {
            if i % 3 == 0 {
                &b"alpha"[..]
            } else {
                &b"beta"[..]
            }
        })
        .collect();
    let long_strings = bytes_stream(&vec![vec![b'q'; 256]; 64]);
    let random_bytes = bytes_stream(
        &(0..256u32)
            .map(|seed| {
                let mut state = seed.wrapping_mul(2_654_435_761);
                let mut bytes = Vec::with_capacity(16);
                for _ in 0..16 {
                    state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    bytes.push((state >> 24) as u8);
                }
                bytes
            })
            .collect::<Vec<Vec<u8>>>(),
    );
    vec![
        ("empty", stream(&[]), 0),
        ("one", stream(&[b"x"]), 1),
        (
            "monotonic-integers",
            int_stream(&(0..512).collect::<Vec<i64>>()),
            512,
        ),
        ("constant-integers", int_stream(&vec![9_i64; 512]), 512),
        ("low-cardinality-strings", stream(&low_cardinality), 512),
        ("long-strings", long_strings, 64),
        ("random-bytes", random_bytes, 256),
    ]
}

#[test]
fn auto_never_grows_a_chunk_and_round_trips_exactly() {
    for (name, raw, count) in datasets() {
        let encoded = encode_chunk(&raw, count, ColumnEncoding::Auto, ChunkCompression::None)
            .expect("encode");
        assert!(
            encoded.payload.len() <= raw.len(),
            "{name} grew from {} to {} bytes",
            raw.len(),
            encoded.payload.len()
        );
        let back = decode_chunk(
            &encoded.payload,
            encoded.encoding,
            encoded.codec,
            encoded.uncompressed_len,
            count,
        )
        .expect("decode");
        assert_eq!(back, raw, "{name} did not round trip");
    }
}

#[test]
fn auto_is_deterministic() {
    for (name, raw, count) in datasets() {
        let first = encode_chunk(&raw, count, ColumnEncoding::Auto, ChunkCompression::None)
            .expect("encode");
        let second = encode_chunk(&raw, count, ColumnEncoding::Auto, ChunkCompression::None)
            .expect("encode");
        assert_eq!(first.encoding, second.encoding, "{name}");
        assert_eq!(first.payload, second.payload, "{name}");
    }
}

#[test]
fn every_encoding_round_trips_when_it_applies() {
    let integers = int_stream(&(0..300).collect::<Vec<i64>>());
    for encoding in [
        ColumnEncoding::Raw,
        ColumnEncoding::Rle,
        ColumnEncoding::DeltaBitpack,
        ColumnEncoding::Dictionary,
        ColumnEncoding::Auto,
    ] {
        let encoded =
            encode_chunk(&integers, 300, encoding, ChunkCompression::None).expect("encode");
        let back = decode_chunk(
            &encoded.payload,
            encoded.encoding,
            encoded.codec,
            encoded.uncompressed_len,
            encoded.value_count,
        )
        .expect("decode");
        assert_eq!(back, integers, "{encoding:?}");
    }
}

#[test]
fn explicit_encodings_fall_back_to_raw_when_they_do_not_apply() {
    let strings = stream(&[b"a", b"b"]);
    let encoded = encode_chunk(
        &strings,
        2,
        ColumnEncoding::DeltaBitpack,
        ChunkCompression::None,
    )
    .expect("encode");
    assert_eq!(encoded.encoding, ColumnEncoding::Raw);
    assert_eq!(encoded.payload, strings);
    // The requested encoding is used where it does apply.
    let integers = int_stream(&(0..300).collect::<Vec<i64>>());
    let encoded = encode_chunk(
        &integers,
        300,
        ColumnEncoding::DeltaBitpack,
        ChunkCompression::None,
    )
    .expect("encode");
    assert_eq!(encoded.encoding, ColumnEncoding::DeltaBitpack);
}

#[test]
fn an_unavailable_codec_is_refused() {
    if !plomid_columnar::compression::is_compression_available(ChunkCompression::Zstd) {
        let error = encode_chunk(
            &int_stream(&[1]),
            1,
            ColumnEncoding::Auto,
            ChunkCompression::Zstd,
        )
        .expect_err("zstd is not built in");
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
}

#[test]
fn raw_chunks_keep_the_version_one_layout() {
    let raw = int_stream(&[4, 5, 6]);
    let encoded =
        encode_chunk(&raw, 3, ColumnEncoding::Raw, ChunkCompression::None).expect("encode");
    assert_eq!(encoded.encoding, ColumnEncoding::Raw);
    assert!(!encoded.is_framed());
    assert_eq!(encoded.payload, raw);
    assert_eq!(encoded.uncompressed_len, raw.len() as u64);
    assert_eq!(encoded.encoded_len, 0);
    assert_eq!(encoded.raw_len, raw.len() as u64);
}

#[test]
fn framed_chunks_record_their_encoding_codec_and_lengths() {
    let raw = int_stream(&(0..64).collect::<Vec<i64>>());
    let encoded =
        encode_chunk(&raw, 64, ColumnEncoding::Auto, ChunkCompression::None).expect("encode");
    assert!(encoded.is_framed());
    assert_eq!(encoded.encoding, ColumnEncoding::DeltaBitpack);
    let frame = EncodingFrame::decode(&encoded.payload).expect("frame");
    assert_eq!(frame.encoding, ColumnEncoding::DeltaBitpack);
    assert_eq!(frame.encoding_version, ENCODING_VERSION_DELTA_BITPACK);
    assert_eq!(frame.codec, ChunkCompression::None);
    assert_eq!(frame.value_count, 64);
    assert_eq!(frame.raw_len, raw.len() as u64);
    assert_eq!(frame.encoded_len, encoded.encoded_len);
    assert_eq!(
        encoded.uncompressed_len,
        EncodingFrame::ENCODED_LEN as u64 + frame.encoded_len
    );
}

/// A valid framed payload for mutation tests. With the `None` codec the
/// stored payload is the frame followed by the body, so frame offsets are
/// directly addressable.
fn framed_sample() -> (Vec<u8>, u64, u64) {
    let raw = int_stream(&(0..32).collect::<Vec<i64>>());
    let encoded = encode_chunk(
        &raw,
        32,
        ColumnEncoding::DeltaBitpack,
        ChunkCompression::None,
    )
    .expect("encode");
    (encoded.payload, 32, encoded.uncompressed_len)
}

fn decode_sample(payload: &[u8], declared: u64) -> Result<Vec<u8>> {
    decode_chunk(
        payload,
        ColumnEncoding::DeltaBitpack,
        ChunkCompression::None,
        declared,
        32,
    )
}

#[test]
fn rejects_a_declared_length_that_disagrees_with_the_payload() {
    let (payload, _, declared) = framed_sample();
    assert!(decode_sample(&payload, declared + 1).is_err());
    assert!(decode_sample(&payload, declared - 1).is_err());
    assert!(decode_sample(&payload, 0).is_err());
}

#[test]
fn rejects_truncation_trailing_bytes_and_an_oversized_declaration() {
    let (payload, _, declared) = framed_sample();
    let truncated = &payload[..payload.len() - 1];
    assert!(decode_sample(truncated, declared).is_err());
    let mut padded = payload.clone();
    padded.push(0);
    assert!(decode_sample(&padded, declared).is_err());
    let error = decode_chunk(
        &payload,
        ColumnEncoding::DeltaBitpack,
        ChunkCompression::None,
        plomid_core::COLUMNAR_MAX_CHUNK_UNCOMPRESSED_SIZE + 1,
        32,
    )
    .expect_err("oversized declaration");
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn rejects_an_unsupported_frame_version() {
    let (mut payload, _, declared) = framed_sample();
    payload[plomid_core::ENCODING_FRAME_OFF_FRAME_VERSION] = 9;
    let error = decode_sample(&payload, declared).expect_err("version");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[test]
fn rejects_unknown_frame_encoding_and_codec_tags() {
    let (mut payload, _, declared) = framed_sample();
    payload[plomid_core::ENCODING_FRAME_OFF_ENCODING] = 0x7F;
    assert_eq!(
        decode_sample(&payload, declared)
            .expect_err("encoding")
            .kind(),
        ErrorKind::Corruption
    );
    let (mut payload, _, declared) = framed_sample();
    payload[plomid_core::ENCODING_FRAME_OFF_CODEC] = 0x7F;
    assert_eq!(
        decode_sample(&payload, declared).expect_err("codec").kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn rejects_non_zero_reserved_frame_bytes() {
    for offset in [
        plomid_core::ENCODING_FRAME_OFF_RESERVED0,
        plomid_core::ENCODING_FRAME_OFF_RESERVED1,
        plomid_core::ENCODING_FRAME_OFF_RESERVED2,
    ] {
        let (mut payload, _, declared) = framed_sample();
        payload[offset] = 1;
        assert_eq!(
            decode_sample(&payload, declared)
                .expect_err("reserved")
                .kind(),
            ErrorKind::Corruption,
            "reserved byte at {offset}"
        );
    }
}

#[test]
fn rejects_a_frame_that_disagrees_with_its_chunk_header() {
    let (payload, _, declared) = framed_sample();
    // The chunk header says RLE while the frame says delta+bitpack.
    let error = decode_chunk(
        &payload,
        ColumnEncoding::Rle,
        ChunkCompression::None,
        declared,
        32,
    )
    .expect_err("encoding mismatch");
    assert_eq!(error.kind(), ErrorKind::Corruption);
    // The chunk header's row count disagrees with the frame's value count.
    let error = decode_chunk(
        &payload,
        ColumnEncoding::DeltaBitpack,
        ChunkCompression::None,
        declared,
        31,
    )
    .expect_err("value count mismatch");
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn rejects_impossible_frame_lengths_and_encodings() {
    let (payload, _, declared) = framed_sample();
    let body_len_at = plomid_core::ENCODING_FRAME_OFF_ENCODED_LEN;
    let mut wrong_body = payload.clone();
    wrong_body[body_len_at..body_len_at + 8].copy_from_slice(&1_u64.to_le_bytes());
    assert!(decode_sample(&wrong_body, declared).is_err());
}

#[test]
fn rejects_impossible_frame_raw_lengths() {
    let raw = int_stream(&(0..32).collect::<Vec<i64>>());
    let body = plomid_columnar::encoding::delta::encode(&raw, 32)
        .expect("encode")
        .expect("body");
    let oversized = craft(ColumnEncoding::DeltaBitpack, 32, u64::MAX, &body);
    assert_eq!(
        decode_sample(&oversized, oversized.len() as u64)
            .expect_err("raw length beyond the format limit")
            .kind(),
        ErrorKind::Corruption
    );
    // 32 values need at least 128 raw bytes; four is impossible.
    let impossible = craft(ColumnEncoding::DeltaBitpack, 32, 4, &body);
    assert!(decode_sample(&impossible, impossible.len() as u64).is_err());
}

#[test]
fn rejects_automatic_encoding_in_persisted_framing() {
    let (payload, _, declared) = framed_sample();
    let error = decode_chunk(
        &payload,
        ColumnEncoding::Auto,
        ChunkCompression::None,
        declared,
        32,
    )
    .expect_err("auto is not persistable");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn rejects_a_value_count_that_disagrees_with_the_value_stream() {
    let raw = int_stream(&[1, 2, 3]);
    // Three entries encoded as two values: the stream itself is well formed,
    // so only the count check can catch the disagreement.
    let error = encode_chunk(&raw, 2, ColumnEncoding::Auto, ChunkCompression::None)
        .expect_err("stream and count disagree");
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

/// One adaptive fixture: label, raw value stream, row count, logical
/// type, and per-row values (`None` for NULL) backing the hints fold.
type Fixture<'a> = (&'a str, Vec<u8>, u64, ColumnType, Vec<Option<Vec<u8>>>);

fn fold_hints(values: &[Option<Vec<u8>>], column_type: ColumnType) -> EncodingHints {
    let mut hints = EncodingHints::new();
    for value in values {
        hints.observe(value.as_deref(), column_type);
    }
    hints
}

fn int_values(values: &[i64]) -> Vec<Option<Vec<u8>>> {
    values
        .iter()
        .map(|value| Some(value.to_le_bytes().to_vec()))
        .collect()
}

#[test]
fn hints_fold_tracks_runs_order_width_distinct_and_nulls() {
    let values = vec![
        Some(1_i64.to_le_bytes().to_vec()),
        Some(1_i64.to_le_bytes().to_vec()),
        Some(2_i64.to_le_bytes().to_vec()),
        None,
        None,
        Some(9_i64.to_le_bytes().to_vec()),
    ];
    let hints = fold_hints(&values, ColumnType::Integer);
    assert_eq!(hints.rows, 6);
    assert_eq!(hints.nulls, 2);
    // Runs: [1, 1], [2], [NULL, NULL], [9].
    assert_eq!(hints.run_count, 4);
    // NULL sorts below every value but arrives after 2: order breaks.
    assert!(!hints.sorted_ascending);
    // NULL entries are zero-length against 8-byte integers.
    assert!(!hints.width_constant);
    assert_eq!(hints.first_len, Some(8));
    assert!((hints.null_fraction() - 2.0 / 6.0).abs() < 1e-9);
    // Distinct non-NULL values are {1, 2, 9}.
    assert_eq!(hints.distinct_estimate(), Some(3));
    assert_eq!(hints.non_null_rows(), 4);
}

#[test]
fn hints_report_sorted_fixed_width_integers() {
    let values: Vec<i64> = (0..256).collect();
    let hints = fold_hints(&int_values(&values), ColumnType::Integer);
    assert!(hints.sorted_ascending);
    assert!(hints.fixed_integer_width());
    assert!(!hints.runs_dense());
    assert!(!hints.low_cardinality());
}

#[test]
fn hints_detect_dense_runs_and_low_cardinality() {
    let repeated: Vec<Option<Vec<u8>>> =
        (0..512).map(|i| Some(vec![u8::from(i % 2 == 0)])).collect();
    let hints = fold_hints(&repeated, ColumnType::Bytes);
    // Alternating single bytes: one run per row, two distinct values.
    assert_eq!(hints.run_count, 512);
    assert!(!hints.runs_dense());
    assert!(hints.low_cardinality());
    let blocks: Vec<Option<Vec<u8>>> = (0..512_usize)
        .map(|i| Some(vec![u8::from((i / 64) % 2 == 0)]))
        .collect();
    let hints = fold_hints(&blocks, ColumnType::Bytes);
    assert!(hints.runs_dense());
    assert!(hints.low_cardinality());
}

#[test]
fn suggest_sorted_integers_try_delta_first() {
    let values: Vec<i64> = (0..512).collect();
    let hints = fold_hints(&int_values(&values), ColumnType::Integer);
    assert_eq!(
        suggest_encoding(&hints, ColumnType::Integer),
        [
            ColumnEncoding::DeltaBitpack,
            ColumnEncoding::Rle,
            ColumnEncoding::Dictionary,
            ColumnEncoding::Raw,
        ]
    );
    // Regular timestamps are sorted integers: same delta-first order.
    let stamps: Vec<i64> = (0..512)
        .map(|i| 1_700_000_000_000_000 + i * 1_000)
        .collect();
    let hints = fold_hints(&int_values(&stamps), ColumnType::Integer);
    assert_eq!(
        suggest_encoding(&hints, ColumnType::Integer)[0],
        ColumnEncoding::DeltaBitpack
    );
}

#[test]
fn suggest_dense_runs_try_rle_before_dictionary() {
    // Ten blocks of a constant string: both RLE and dictionary apply,
    // but the run estimate (10 records) beats the dictionary estimate.
    let mut values = Vec::new();
    for block in ["alpha", "beta"].iter().cycle().take(10) {
        values.extend((0..200).map(|_| Some(block.as_bytes().to_vec())));
    }
    let hints = fold_hints(&values, ColumnType::String);
    assert!(hints.runs_dense());
    assert!(hints.low_cardinality());
    let order = suggest_encoding(&hints, ColumnType::String);
    assert_eq!(order[0], ColumnEncoding::Rle);
    assert_eq!(order[1], ColumnEncoding::Dictionary);
}

#[test]
fn suggest_low_cardinality_without_runs_tries_dictionary_first() {
    let values: Vec<Option<Vec<u8>>> = (0..512)
        .map(|i| Some(["red", "green", "blue"][i % 3].as_bytes().to_vec()))
        .collect();
    let hints = fold_hints(&values, ColumnType::String);
    assert!(!hints.runs_dense());
    assert!(hints.low_cardinality());
    let order = suggest_encoding(&hints, ColumnType::String);
    assert_eq!(order[0], ColumnEncoding::Dictionary);
    assert_eq!(order[1], ColumnEncoding::Rle);
}

#[test]
fn suggest_booleans_try_dictionary_first_and_delta_last() {
    // Boolean bitpack is the dictionary with 1-bit indices; RLE covers
    // runs; deltas never apply to one-byte values.
    let values: Vec<Option<Vec<u8>>> = (0..512)
        .map(|i| Some(vec![u8::from((i / 8) % 2 == 0)]))
        .collect();
    let hints = fold_hints(&values, ColumnType::Bool);
    let order = suggest_encoding(&hints, ColumnType::Bool);
    assert_eq!(order[0], ColumnEncoding::Dictionary);
    assert_eq!(order[1], ColumnEncoding::Rle);
    assert_eq!(order[3], ColumnEncoding::DeltaBitpack);
}

#[test]
fn suggest_featureless_data_keeps_the_legacy_order() {
    // Unique unsorted variable-width values: no signal, raw first, so
    // ties still keep the version-1 layout byte for byte.
    let values: Vec<Option<Vec<u8>>> = (0..256)
        .map(|i| Some(format!("key-{i:06}-suffix").into_bytes()))
        .collect();
    let hints = fold_hints(&values, ColumnType::String);
    assert!(!hints.runs_dense());
    assert!(!hints.low_cardinality());
    assert_eq!(
        suggest_encoding(&hints, ColumnType::String),
        ColumnEncoding::CANDIDATES
    );
}

#[test]
fn suggest_tiny_chunks_try_raw_first() {
    for rows in [0, 1] {
        let values = vec![Some(b"x".to_vec()); rows];
        let hints = fold_hints(&values, ColumnType::String);
        assert_eq!(
            suggest_encoding(&hints, ColumnType::String)[0],
            ColumnEncoding::Raw
        );
    }
}

#[test]
fn suggest_is_deterministic_for_identical_hints() {
    let values: Vec<i64> = (0..128).map(|i| i * 3 - 7).collect();
    let first = fold_hints(&int_values(&values), ColumnType::Integer);
    let second = fold_hints(&int_values(&values), ColumnType::Integer);
    assert_eq!(
        suggest_encoding(&first, ColumnType::Integer),
        suggest_encoding(&second, ColumnType::Integer)
    );
    let encoded = encode_chunk_with_hints(
        &int_stream(&values),
        values.len() as u64,
        ColumnEncoding::Auto,
        ChunkCompression::None,
        Some(&first),
        ColumnType::Integer,
    )
    .expect("encode");
    let again = encode_chunk_with_hints(
        &int_stream(&values),
        values.len() as u64,
        ColumnEncoding::Auto,
        ChunkCompression::None,
        Some(&second),
        ColumnType::Integer,
    )
    .expect("encode");
    assert_eq!(encoded.encoding, again.encoding);
    assert_eq!(encoded.payload, again.payload);
}

/// The five adaptive fixtures as raw streams with their column types.
fn adaptive_fixtures() -> Vec<Fixture<'static>> {
    const N: usize = 5_000;
    let sorted: Vec<i64> = (0..N as i64).collect();
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    let random: Vec<i64> = (0..N)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state as i64
        })
        .collect();
    let low_card: Vec<Option<Vec<u8>>> = (0..N)
        .map(|i| Some(["red", "green", "blue"][i % 3].as_bytes().to_vec()))
        .collect();
    let booleans: Vec<Option<Vec<u8>>> = (0..N)
        .map(|i| Some(vec![u8::from((i / 8) % 2 == 0)]))
        .collect();
    let stamps: Vec<i64> = (0..N as i64)
        .map(|i| 1_700_000_000_000_000 + i * 1_000)
        .collect();
    vec![
        (
            "int-sorted",
            int_stream(&sorted),
            N as u64,
            ColumnType::Integer,
            int_values(&sorted),
        ),
        (
            "int-random",
            int_stream(&random),
            N as u64,
            ColumnType::Integer,
            int_values(&random),
        ),
        (
            "low-cardinality-string",
            bytes_stream(
                &low_card
                    .iter()
                    .map(|v| v.clone().expect("non-null"))
                    .collect::<Vec<Vec<u8>>>(),
            ),
            N as u64,
            ColumnType::String,
            low_card,
        ),
        (
            "boolean",
            bytes_stream(
                &booleans
                    .iter()
                    .map(|v| v.clone().expect("non-null"))
                    .collect::<Vec<Vec<u8>>>(),
            ),
            N as u64,
            ColumnType::Bool,
            booleans,
        ),
        (
            "timestamp-regular",
            int_stream(&stamps),
            N as u64,
            ColumnType::Integer,
            int_values(&stamps),
        ),
    ]
}

#[test]
fn adaptive_matches_brute_force_on_every_fixture() {
    for (name, raw, count, column_type, values) in adaptive_fixtures() {
        let hints = fold_hints(&values, column_type);
        let brute = encode_chunk(&raw, count, ColumnEncoding::Auto, ChunkCompression::None)
            .expect("brute force");
        let adaptive = encode_chunk_with_hints(
            &raw,
            count,
            ColumnEncoding::Auto,
            ChunkCompression::None,
            Some(&hints),
            column_type,
        )
        .expect("adaptive");
        assert_eq!(adaptive.encoding, brute.encoding, "{name}: winner");
        assert_eq!(adaptive.payload, brute.payload, "{name}: payload");
    }
}

#[test]
fn adaptive_never_grows_and_round_trips_on_every_fixture() {
    for (name, raw, count, column_type, values) in adaptive_fixtures() {
        let hints = fold_hints(&values, column_type);
        let encoded = encode_chunk_with_hints(
            &raw,
            count,
            ColumnEncoding::Auto,
            ChunkCompression::None,
            Some(&hints),
            column_type,
        )
        .expect("encode");
        assert!(
            encoded.payload.len() <= raw.len(),
            "{name} grew from {} to {} bytes",
            raw.len(),
            encoded.payload.len()
        );
        let back = decode_chunk(
            &encoded.payload,
            encoded.encoding,
            encoded.codec,
            encoded.uncompressed_len,
            count,
        )
        .expect("decode");
        assert_eq!(back, raw, "{name} did not round trip");
    }
}

#[test]
fn every_preference_round_trips_on_every_edge_shape() {
    let shapes: Vec<Fixture<'_>> = vec![
        ("empty", stream(&[]), 0, ColumnType::Integer, Vec::new()),
        (
            "single",
            stream(&[b"solo"]),
            1,
            ColumnType::String,
            vec![Some(b"solo".to_vec())],
        ),
        (
            "all-null",
            stream(&[b"", b"", b"", b""]),
            4,
            ColumnType::Integer,
            vec![None, None, None, None],
        ),
        (
            "all-equal",
            int_stream(&vec![7_i64; 64]),
            64,
            ColumnType::Integer,
            int_values(&vec![7_i64; 64]),
        ),
        (
            "adversarial-unsorted",
            int_stream(&[i64::MAX, i64::MIN, 0, -1, 1, i64::MAX - 1]),
            6,
            ColumnType::Integer,
            int_values(&[i64::MAX, i64::MIN, 0, -1, 1, i64::MAX - 1]),
        ),
        (
            "periodic-nulls",
            int_stream(&[1, 2, 3, 4]),
            4,
            ColumnType::Integer,
            vec![
                Some(1_i64.to_le_bytes().to_vec()),
                None,
                Some(3_i64.to_le_bytes().to_vec()),
                None,
            ],
        ),
    ];
    // The NULL-laden integer stream is only well formed when NULL rows
    // contribute zero-length entries: rebuild it with the right entries.
    let null_shape = {
        let mut raw = Vec::new();
        for value in [
            Some(1_i64.to_le_bytes().to_vec()),
            None,
            Some(3_i64.to_le_bytes().to_vec()),
            None,
        ] {
            append_entry(&mut raw, value.as_deref().unwrap_or(&[])).expect("append");
        }
        raw
    };
    let preferences = [
        ColumnEncodingPreference::Auto,
        ColumnEncodingPreference::Raw,
        ColumnEncodingPreference::Rle,
        ColumnEncodingPreference::DeltaBitpack,
        ColumnEncodingPreference::Dictionary,
    ];
    for (name, raw, count, column_type, values) in shapes {
        let raw = if name == "periodic-nulls" {
            null_shape.clone()
        } else {
            raw
        };
        // The all-null integer stream likewise needs zero-length entries.
        let raw = if name == "all-null" {
            let mut out = Vec::new();
            for _ in 0..4 {
                append_entry(&mut out, &[]).expect("append");
            }
            out
        } else {
            raw
        };
        let hints = fold_hints(&values, column_type);
        for preference in preferences {
            let encoding = preference.resolve(ColumnEncoding::Auto);
            let encoded = encode_chunk_with_hints(
                &raw,
                count,
                encoding,
                ChunkCompression::None,
                Some(&hints),
                column_type,
            )
            .expect("encode");
            let back = decode_chunk(
                &encoded.payload,
                encoded.encoding,
                encoded.codec,
                encoded.uncompressed_len,
                count,
            )
            .expect("decode");
            assert_eq!(back, raw, "{name} {preference:?} did not round trip");
        }
    }
}

#[test]
fn explicit_preferences_fall_back_to_raw_without_hints_regression() {
    let strings = stream(&[b"a", b"b"]);
    let hints = fold_hints(
        &[Some(b"a".to_vec()), Some(b"b".to_vec())],
        ColumnType::String,
    );
    // Delta cannot represent strings: the hinted path must agree with the
    // legacy path and fall back to raw.
    let legacy = encode_chunk(
        &strings,
        2,
        ColumnEncoding::DeltaBitpack,
        ChunkCompression::None,
    )
    .expect("legacy");
    let hinted = encode_chunk_with_hints(
        &strings,
        2,
        ColumnEncoding::DeltaBitpack,
        ChunkCompression::None,
        Some(&hints),
        ColumnType::String,
    )
    .expect("hinted");
    assert_eq!(legacy.encoding, ColumnEncoding::Raw);
    assert_eq!(hinted.encoding, ColumnEncoding::Raw);
    assert_eq!(hinted.payload, legacy.payload);
}
