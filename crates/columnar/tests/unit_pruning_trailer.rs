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
// Moved from src/pruning/trailer.rs inline #[cfg(test)].
use plomid_columnar::column::ColumnType;
use plomid_columnar::layout::{checksum_of, checksum_of_pair, get_u64, put_u32, put_u64};
use plomid_columnar::pruning::brin::{BrinIndex, BrinRange};
use plomid_columnar::pruning::trailer::*;
use plomid_columnar::pruning::zonemap::ZoneMapBuilder;
use plomid_core::{ColumnId, ErrorKind};
use plomid_storage::{Field, Row};

/// A representative small index: two ranges, two columns each.
fn sample_index() -> BrinIndex {
    let mut ranges = Vec::new();
    for range_idx in 0_u64..2 {
        let start = range_idx * 4;
        let mut zones = Vec::new();
        for column in 0_u64..2 {
            let mut builder = ZoneMapBuilder::new(ColumnId::new(column), ColumnType::Integer);
            for row in start..start + 4 {
                builder.observe(row, Some(Field::Integer(row as i64)));
            }
            zones.push(builder.finish());
        }
        ranges.push(BrinRange::new(start, start + 4, zones));
    }
    BrinIndex::from_ranges(8, 4, ranges)
}

fn sample_bytes() -> Vec<u8> {
    encode_trailer(&sample_index())
}

/// Length of the pre-trailer body in the synthetic images below.
fn body_len() -> usize {
    128
}

/// Assembles `body ‖ trailer` the way the flush pipeline does.
fn image_with_trailer() -> Vec<u8> {
    let mut image = vec![0xAB_u8; body_len()];
    image.extend_from_slice(&sample_bytes());
    image
}

/// Rewrites every checksum the format carries so that a byte-level mutation
/// reaches the *structural* validators instead of being caught by the
/// checksums. Without this, corruption tests would only exercise the CRC
/// paths and never the semantic ones.
///
/// Only checksums are rewritten: lengths, counts, versions, flags, and
/// payload bytes are preserved exactly as the mutation left them, so a
/// tampered length still disagrees with its counterpart and a tampered
/// count still describes the wrong structure. The walk is fully
/// bounds-checked and stops at the first unparseable record instead of
/// panicking, so mutating a length to `u32::MAX` cannot crash the helper.
///
/// `bytes` is one whole trailer region: `body ‖ footer`.
fn reseal(bytes: &mut [u8]) {
    if bytes.len() < PRUNING_TRAILER_HEADER_LEN {
        return;
    }
    let footer_start = bytes.len() - PRUNING_TRAILER_HEADER_LEN;
    let mut cursor = 0_usize;
    while cursor < footer_start {
        let header = match bytes.get(cursor..cursor + PRUNING_RANGE_HEADER_LEN) {
            Some(header) => header,
            None => break,
        };
        let zone_count = match header
            .get(RANGE_OFF_ZONE_COUNT..RANGE_OFF_ZONE_COUNT + 4)
            .and_then(|slot| slot.try_into().ok())
            .map(u32::from_le_bytes)
        {
            Some(count) => count.min(MAX_DECODED_ZONES_PER_RANGE as u32) as usize,
            None => break,
        };
        // When the header claims more zones than the cap, the decoder
        // rejects the count; keep the claim intact and stop here so the
        // structural validator is what fires.
        let claimed = u32::from_le_bytes(
            header[RANGE_OFF_ZONE_COUNT..RANGE_OFF_ZONE_COUNT + 4]
                .try_into()
                .unwrap_or([0; 4]),
        ) as usize;
        if claimed != zone_count {
            break;
        }
        cursor += PRUNING_RANGE_HEADER_LEN;
        let mut stop = false;
        for _ in 0..zone_count {
            let zone_header = match bytes.get(cursor..cursor + PRUNING_ZONE_HEADER_LEN) {
                Some(header) => header,
                None => {
                    stop = true;
                    break;
                }
            };
            let min_len = u32::from_le_bytes(
                zone_header[ZONE_OFF_MIN_LEN..ZONE_OFF_MIN_LEN + 4]
                    .try_into()
                    .unwrap_or([0xFF; 4]),
            ) as usize;
            let max_len = u32::from_le_bytes(
                zone_header[ZONE_OFF_MAX_LEN..ZONE_OFF_MAX_LEN + 4]
                    .try_into()
                    .unwrap_or([0xFF; 4]),
            ) as usize;
            let zone_len = match PRUNING_ZONE_HEADER_LEN
                .checked_add(min_len)
                .and_then(|sum| sum.checked_add(max_len))
            {
                Some(len) => len,
                None => {
                    stop = true;
                    break;
                }
            };
            let (prefix, payload) = match (
                bytes.get(cursor..cursor + ZONE_OFF_CRC),
                bytes.get(cursor + PRUNING_ZONE_HEADER_LEN..cursor + zone_len),
            ) {
                (Some(prefix), Some(payload)) => (prefix, payload),
                _ => {
                    stop = true;
                    break;
                }
            };
            let crc = checksum_of_pair(prefix, payload);
            if let Some(slot) = bytes.get_mut(cursor + ZONE_OFF_CRC..cursor + ZONE_OFF_CRC + 4) {
                slot.copy_from_slice(&crc.to_le_bytes());
            } else {
                stop = true;
                break;
            }
            cursor += zone_len;
            if cursor > footer_start {
                stop = true;
                break;
            }
        }
        if stop {
            break;
        }
    }
    let body_crc = checksum_of(&bytes[..footer_start]);
    if let Some(footer) = bytes.get_mut(footer_start..) {
        put_u32(footer, OFF_BODY_CRC, body_crc);
        let crc = framing_checksum(footer);
        put_u32(footer, OFF_HEADER_CRC, crc);
    }
}

/// Applies `mutate` to the trailer of a fresh image, then reseals it so the
/// structural validators are what reject the result.
fn tampered(mutate: impl FnOnce(&mut [u8])) -> Vec<u8> {
    let mut image = image_with_trailer();
    let start = body_len();
    mutate(&mut image[start..]);
    reseal(&mut image[start..]);
    image
}

/// Offset of the first zone blob inside the trailer region.
fn first_zone_offset() -> usize {
    PRUNING_RANGE_HEADER_LEN
}

/// Offset of the footer inside the trailer region.
fn footer_offset() -> usize {
    sample_bytes().len() - PRUNING_TRAILER_HEADER_LEN
}

/// Offset of the second range record inside the trailer body.
fn second_range_offset() -> usize {
    PRUNING_RANGE_HEADER_LEN + 2 * (PRUNING_ZONE_HEADER_LEN + 16)
}

fn expect_corruption(bytes: &[u8], trailer_start: usize, what: &str) {
    match decode_trailer_at(bytes, trailer_start) {
        Err(error) => assert_eq!(error.kind(), ErrorKind::Corruption, "{what}"),
        Ok(other) => panic!("{what}: expected corruption, decoded {other:?}"),
    }
}
#[test]
fn trailer_round_trips_through_both_entry_points() {
    let image = image_with_trailer();
    let expected = sample_index();
    assert!(has_trailer(&image));
    assert_eq!(strip_trailer(&image).len(), body_len());
    assert_eq!(
        decode_trailer_at(&image, body_len())
            .expect("anchored")
            .expect("present"),
        expected
    );
    assert_eq!(
        decode_trailer(&image).expect("tail").expect("present"),
        expected
    );
    let tail = &image[image.len() - 8..];
    assert_eq!(
        u64::from_le_bytes(tail.try_into().unwrap()) as usize,
        sample_bytes().len()
    );
}

#[test]
fn image_without_trailer_reports_absent_metadata() {
    let body = vec![0xCD_u8; body_len()];
    assert!(!has_trailer(&body));
    assert_eq!(strip_trailer(&body), &body[..]);
    assert!(decode_trailer_at(&body, body_len())
        .expect("absent")
        .is_none());
    assert!(decode_trailer(&body).expect("absent").is_none());
    // A body too short to hold even a trailer header is still "absent".
    assert!(decode_trailer_at(&body[..8], 8).expect("absent").is_none());
    assert!(decode_trailer(&body[..8]).expect("absent").is_none());
}

#[test]
fn truncation_is_corruption_not_absent_metadata() {
    let image = image_with_trailer();
    for cut in 1..=PRUNING_TRAILER_HEADER_LEN {
        let truncated = &image[..image.len() - cut];
        expect_corruption(truncated, body_len(), &format!("cut {cut}"));
    }
}

#[test]
fn corrupted_total_length_is_reported_not_swallowed() {
    // If the length anchor is damaged, the tail-based entry point must not
    // conclude "this image has no metadata": that would silently disable
    // pruning for a segment whose metadata is present but untrustworthy.
    let mut image = image_with_trailer();
    let len = image.len();
    let bogus = u64::from_le_bytes(image[len - 8..].try_into().unwrap()) + 8;
    image[len - 8..].copy_from_slice(&bogus.to_le_bytes());
    assert!(decode_trailer(&image).is_err(), "tail lookup must reject");
    expect_corruption(&image, body_len(), "anchored, long total_len");

    let mut image = image_with_trailer();
    let len = image.len();
    image[len - 8..].copy_from_slice(&0_u64.to_le_bytes());
    assert!(decode_trailer(&image).is_err());
    expect_corruption(&image, body_len(), "anchored, zero total_len");
}

#[test]
fn trailer_must_end_exactly_at_the_end_of_the_image() {
    let mut image = image_with_trailer();
    image.push(0); // one trailing byte past the trailer
    assert!(decode_trailer_at(&image, body_len()).is_err());
    // A trailer start before the body end means the metadata does not
    // describe the image it claims to.
    let image = image_with_trailer();
    assert!(decode_trailer_at(&image, body_len() - 1).is_err());
    assert!(decode_trailer_at(&image, image.len() + 1).is_err());
}

#[test]
fn checksum_corruption_in_either_region_is_rejected() {
    // A single flipped byte in the body, without any repair, must fail.
    let mut image = image_with_trailer();
    let target = body_len() + PRUNING_TRAILER_HEADER_LEN + PRUNING_RANGE_HEADER_LEN;
    image[target] ^= 0xFF;
    assert!(decode_trailer_at(&image, body_len()).is_err());
    // So must a flipped byte in the header.
    let mut image = image_with_trailer();
    image[body_len() + footer_offset() + OFF_RANGE_COUNT] ^= 0xFF;
    assert!(decode_trailer_at(&image, body_len()).is_err());
    let mut image = image_with_trailer();
    image[body_len() + footer_offset() + OFF_RESERVED] ^= 0xFF;
    assert!(decode_trailer_at(&image, body_len()).is_err());
}

#[test]
fn unknown_version_is_unsupported_not_trusted() {
    let image = tampered(|trailer| {
        put_u32(
            trailer,
            footer_offset() + OFF_VERSION,
            PRUNING_FORMAT_VERSION + 1,
        );
    });
    let error = decode_trailer_at(&image, body_len()).expect_err("version");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[test]
fn unknown_header_flags_and_nonzero_reserved_bits_are_rejected() {
    expect_corruption(
        &tampered(|trailer| put_u32(trailer, footer_offset() + OFF_FLAGS, 1)),
        body_len(),
        "flags",
    );
    expect_corruption(
        &tampered(|trailer| put_u32(trailer, footer_offset() + OFF_RESERVED2, 0xDEAD)),
        body_len(),
        "reserved2",
    );
}

#[test]
fn malformed_range_counts_are_rejected() {
    expect_corruption(
        &tampered(|trailer| put_u32(trailer, footer_offset() + OFF_RANGE_COUNT, 0)),
        body_len(),
        "under-counted ranges",
    );
    expect_corruption(
        &tampered(|trailer| put_u32(trailer, footer_offset() + OFF_RANGE_COUNT, u32::MAX)),
        body_len(),
        "impossibly large range count",
    );
    expect_corruption(
        &tampered(|trailer| {
            put_u32(trailer, RANGE_OFF_ZONE_COUNT, u32::MAX);
        }),
        body_len(),
        "impossibly large zone count",
    );
}

#[test]
fn body_length_disagreeing_with_total_length_is_rejected() {
    expect_corruption(
        &tampered(|trailer| {
            let len = footer_offset();
            let body_len = get_u64(&trailer[len..], OFF_BODY_LEN, "body len").expect("read") + 1;
            put_u64(&mut trailer[len..], OFF_BODY_LEN, body_len);
        }),
        body_len(),
        "body length mismatch",
    );
}

#[test]
fn impossible_range_tiling_is_rejected() {
    // A gap: the second range is nudged forward, leaving row 3 uncovered.
    let image = tampered(|trailer| {
        put_u64(trailer, second_range_offset() + RANGE_OFF_START_ROW, 5);
    });
    expect_corruption(&image, body_len(), "gapped tiling");
    // An overlap: the second range starts before the first ends.
    let image = tampered(|trailer| {
        put_u64(trailer, second_range_offset() + RANGE_OFF_START_ROW, 1);
    });
    expect_corruption(&image, body_len(), "overlapping tiling");
    // A range that ends before it starts.
    let image = tampered(|trailer| put_u64(trailer, RANGE_OFF_END_ROW, 0));
    expect_corruption(&image, body_len(), "inverted range");
}

#[test]
fn invalid_zone_type_tag_and_null_state_are_rejected() {
    expect_corruption(
        &tampered(|trailer| trailer[first_zone_offset() + ZONE_OFF_TYPE_TAG] = 0xEE),
        body_len(),
        "invalid type tag",
    );
    for bad_state in [3_u8, 0x7F, 0xFF] {
        expect_corruption(
            &tampered(move |trailer| {
                trailer[first_zone_offset() + ZONE_OFF_NULL_STATE] = bad_state;
            }),
            body_len(),
            "invalid null state",
        );
    }
}

#[test]
fn unknown_zone_flags_and_reserved_bits_are_rejected() {
    expect_corruption(
        &tampered(|trailer| {
            trailer[first_zone_offset() + ZONE_OFF_FLAGS] |= 0x80;
        }),
        body_len(),
        "unknown zone flags",
    );
    // Half-present bounds are worse than none: a lone minimum cannot prove
    // a range empty, so the pair must be all-or-nothing.
    expect_corruption(
        &tampered(|trailer| {
            trailer[first_zone_offset() + ZONE_OFF_FLAGS] ^= ZONE_FLAG_MAX_AVAILABLE;
        }),
        body_len(),
        "half-present bounds",
    );
    expect_corruption(
        &tampered(|trailer| {
            trailer[first_zone_offset() + ZONE_OFF_RESERVED] = 1;
        }),
        body_len(),
        "nonzero zone reserved",
    );
}

#[test]
fn crossed_bounds_are_rejected() {
    // Rewrite the minimum bound payload to a value above the maximum while
    // keeping its length (Integer bounds are fixed width), then reseal so
    // the semantic check is what fires.
    let high = Row::new(vec![Field::Integer(9_999)])
        .encode()
        .expect("encode");
    let image = tampered(move |trailer| {
        let base = first_zone_offset() + PRUNING_ZONE_HEADER_LEN;
        trailer[base..base + high.len()].copy_from_slice(&high);
    });
    expect_corruption(&image, body_len(), "min > max");
}

#[test]
fn truncated_or_oversized_bound_payloads_are_rejected() {
    expect_corruption(
        &tampered(|trailer| {
            put_u32(trailer, first_zone_offset() + ZONE_OFF_MIN_LEN, u32::MAX);
        }),
        body_len(),
        "oversized min bound",
    );
    expect_corruption(
        &tampered(|trailer| {
            put_u32(trailer, first_zone_offset() + ZONE_OFF_MAX_LEN, u32::MAX);
        }),
        body_len(),
        "oversized max bound",
    );
    // A declared-but-absent payload (present bits set, lengths zero).
    expect_corruption(
        &tampered(|trailer| {
            put_u32(trailer, first_zone_offset() + ZONE_OFF_MIN_LEN, 0);
        }),
        body_len(),
        "empty bound payload",
    );
}

#[test]
fn null_bound_payloads_are_rejected() {
    let null_bound = Row::new(vec![Field::Null]).encode().expect("encode");
    let image = tampered(move |trailer| {
        let base = first_zone_offset() + PRUNING_ZONE_HEADER_LEN;
        trailer[base..base + null_bound.len()].copy_from_slice(&null_bound);
    });
    expect_corruption(&image, body_len(), "NULL bound");
}

#[test]
fn undecodable_bound_payloads_are_rejected() {
    let image = tampered(|trailer| {
        let base = first_zone_offset() + PRUNING_ZONE_HEADER_LEN;
        // Scramble the encoded value without changing the payload length.
        for byte in &mut trailer[base..base + 16] {
            *byte ^= 0xA5;
        }
    });
    let result = decode_trailer_at(&image, body_len());
    assert!(result.is_err(), "scrambled bound must not decode");
    assert_eq!(result.expect_err("err").kind(), ErrorKind::Corruption);
}

#[test]
fn zone_escaping_its_range_is_rejected() {
    // A zone map whose rows fall outside the range that owns it is
    // corruption: the summary would not describe the rows it is used to
    // prune.
    let image = tampered(|trailer| {
        put_u64(trailer, first_zone_offset() + ZONE_OFF_ROW_COUNT, 4);
        put_u64(trailer, first_zone_offset() + ZONE_OFF_START_ROW, 3);
    });
    expect_corruption(&image, body_len(), "zone escapes range");
}

#[test]
fn corrupted_zone_checksum_is_rejected() {
    // A flipped zone CRC, without any repair, must fail. (Resealing would
    // recompute the CRC, so this test mutates without resealing.)
    let mut image = image_with_trailer();
    let target = body_len() + first_zone_offset() + ZONE_OFF_CRC;
    image[target] ^= 0xFF;
    expect_corruption(&image, body_len(), "zone crc");
}

#[test]
fn trailing_body_bytes_are_rejected() {
    // `range_count` smaller than the body's actual record count leaves
    // unread bytes: never "approximately right" metadata.
    let image = tampered(|trailer| {
        // Reduce the header's range count by one without shortening the
        // body, so the decoder must notice the leftovers.
        put_u32(trailer, footer_offset() + OFF_RANGE_COUNT, 1);
    });
    expect_corruption(&image, body_len(), "trailing body bytes");
}
