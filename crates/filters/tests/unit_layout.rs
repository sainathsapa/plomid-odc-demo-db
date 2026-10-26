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
// Moved from `src/layout.rs` inline `#[cfg(test)]` — public-API only.
use plomid_filters::layout::{frame, unframe, FOOTER_LEN};

const MAGIC: [u8; 4] = *b"PLTB";
const VERSION: u32 = 1;

/// Builds a framed body so damage can be injected at known offsets.
fn framed() -> Vec<u8> {
    frame(MAGIC, VERSION, b"filter body bytes")
}

#[test]
fn round_trip_returns_the_body() {
    let image = framed();
    assert_eq!(image.len(), "filter body bytes".len() + FOOTER_LEN);
    let body = unframe(MAGIC, VERSION, &image).expect("well-formed image must decode");
    assert_eq!(body, b"filter body bytes");
}

#[test]
fn empty_body_is_framed_and_recovered() {
    let image = frame(MAGIC, VERSION, &[]);
    assert_eq!(image.len(), FOOTER_LEN);
    assert_eq!(unframe(MAGIC, VERSION, &image), Ok(&[][..]));
}

#[test]
fn wrong_magic_is_rejected() {
    assert!(unframe(*b"PLXX", VERSION, &framed()).is_err());
}

#[test]
fn unknown_version_is_rejected() {
    assert!(unframe(MAGIC, VERSION + 1, &framed()).is_err());
}

#[test]
fn every_truncated_length_is_rejected() {
    let image = framed();
    for cut in 1..=FOOTER_LEN {
        let short = &image[..image.len() - cut];
        assert!(unframe(MAGIC, VERSION, short).is_err(), "cut {cut}");
    }
}

#[test]
fn any_single_byte_flip_is_detected() {
    let image = framed();
    for index in 0..image.len() {
        let mut damaged = image.clone();
        damaged[index] ^= 0x40;
        assert!(
            unframe(MAGIC, VERSION, &damaged).is_err(),
            "flip at byte {index} must be rejected"
        );
    }
}
