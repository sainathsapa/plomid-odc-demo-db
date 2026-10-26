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
// Moved from `src/page.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{GenerationId, Lsn, PageId, PlomidError};
use plomid_storage::page::*;
use plomid_storage::physical::{PageHeader, PageType};

#[test]
fn page_size_matches_physical_contract() {
    assert_eq!(PAGE_SIZE, 16 * 1024);
    assert_eq!(PAGE_HEADER_SIZE, PageHeader::SIZE);
    assert_eq!(PAGE_TRAILER_SIZE, 4);
    assert_eq!(
        PAGE_DATA_SIZE,
        PAGE_SIZE - PAGE_HEADER_SIZE - PAGE_TRAILER_SIZE
    );
}

#[test]
fn round_trip_preserves_identity_payload_and_generation() {
    let mut page = Page::with_type(PageId::new(7), PageType::Leaf);
    page.set_generation(GenerationId::new(3));
    page.set_lsn(Lsn::new(9));
    page.data_mut()[..5].copy_from_slice(b"hello");
    let bytes = page.to_bytes();
    let decoded = Page::from_bytes(&bytes).expect("decode");
    assert_eq!(decoded.id(), PageId::new(7));
    assert_eq!(decoded.page_type(), PageType::Leaf);
    assert_eq!(decoded.generation(), GenerationId::new(3));
    assert_eq!(decoded.lsn(), Lsn::new(9));
    assert_eq!(&decoded.data()[..5], b"hello");
    decoded.validate().expect("validate");
}

#[test]
fn rejects_invalid_magic() {
    let mut bytes = Page::new(PageId::new(1)).to_bytes();
    bytes[0..4].copy_from_slice(b"XXXX");
    assert!(matches!(Page::from_bytes(&bytes), Err(PlomidError { .. })));
}

#[test]
fn rejects_checksum_mismatch() {
    let mut bytes = Page::new(PageId::new(2)).to_bytes();
    // Flip a payload byte; the trailer checksum no longer matches.
    bytes[100] ^= 0xFF;
    assert!(Page::from_bytes(&bytes).is_err());
}

#[test]
fn rejects_oversized_payload_length() {
    // Craft a valid header with an implausibly large payload length.
    let mut bytes = Page::new(PageId::new(3)).to_bytes();
    let oversized = (PAGE_DATA_SIZE as u32) + 1;
    bytes[36..40].copy_from_slice(&oversized.to_le_bytes());
    assert!(Page::from_bytes(&bytes).is_err());
}

#[test]
fn rejects_unsupported_format_version() {
    let mut bytes = Page::new(PageId::new(4)).to_bytes();
    bytes[4..8].copy_from_slice(&999u32.to_le_bytes());
    assert!(Page::from_bytes(&bytes).is_err());
}
