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
// Moved from `src/physical.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{BlockId, DatabaseId, GenerationId, Lsn, PackId, PageId};
use plomid_storage::physical::{
    check_pack_header, check_page_header, BlockDirectoryEntry, BlockMetadata, IntegrityCheck,
    PackFooter, PackHeader, PageHeader, PageType, PhysicalLocation, BLOCK_FORMAT_VERSION,
    BLOCK_SIZE, DEFAULT_STORAGE_SEGMENT_SIZE_BYTES, EXTENT_SIZE, PACK_FORMAT_VERSION,
    PACK_TARGET_SIZE, PAGES_PER_BLOCK, PAGE_FORMAT_VERSION, PAGE_SIZE,
};

#[test]
fn constants_match_architecture() {
    assert_eq!(PAGE_SIZE, 16 * 1024);
    assert_eq!(BLOCK_SIZE, 256 * 1024);
    assert_eq!(EXTENT_SIZE, 64 * 1024 * 1024);
    assert_eq!(DEFAULT_STORAGE_SEGMENT_SIZE_BYTES, 8 * 1024 * 1024);
    assert_eq!(PACK_TARGET_SIZE, 16 * 1024 * 1024 * 1024);
    assert_eq!(PAGES_PER_BLOCK, 16);
    assert_eq!(BLOCK_SIZE, PAGES_PER_BLOCK * PAGE_SIZE);
    assert_eq!(PAGE_FORMAT_VERSION, 1);
    assert_eq!(BLOCK_FORMAT_VERSION, 1);
    assert_eq!(PACK_FORMAT_VERSION, 1);
}

#[test]
fn page_header_round_trip() {
    let header = PageHeader::new(PageId::new(7), PageType::Leaf).with_checksum();
    let bytes = header.encode();
    let decoded = PageHeader::decode(&bytes).expect("decode must succeed");
    assert_eq!(decoded, header);
}

#[test]
fn page_header_rejects_truncated_bad_magic_and_bad_version() {
    let header = PageHeader::new(PageId::new(7), PageType::Leaf).with_checksum();
    let bytes = header.encode();
    assert!(PageHeader::decode(&bytes[..10]).is_err());
    let mut bad_magic = bytes;
    bad_magic[0] = b'X';
    assert!(PageHeader::decode(&bad_magic).is_err());
    let mut bad_version = bytes;
    bad_version[4] = 0xFF;
    assert!(PageHeader::decode(&bad_version).is_err());
}

#[test]
fn page_header_rejects_corrupt_checksum_and_bad_type() {
    let header = PageHeader::new(PageId::new(7), PageType::Leaf).with_checksum();
    let mut bytes = header.encode();
    bytes[20] ^= 0xFF;
    assert!(PageHeader::decode(&bytes).is_err());
    let mut bad_type = header.encode();
    bad_type[8] = 99;
    assert!(PageHeader::decode(&bad_type).is_err());
}

#[test]
fn block_metadata_round_trip_and_alignment() {
    let mut meta = BlockMetadata::new(BlockId::new(3));
    for (i, page) in meta.pages.iter_mut().enumerate() {
        *page = PageId::new((i as u64) + 1);
    }
    meta.validate().expect("valid block");
    let bytes = meta.encode();
    let decoded = BlockMetadata::decode(&bytes).expect("ok");
    assert_eq!(decoded, meta);
    assert_eq!(BLOCK_SIZE % PAGE_SIZE, 0);
    assert_eq!(EXTENT_SIZE % BLOCK_SIZE, 0);
}

#[test]
fn block_metadata_rejects_zero_page_and_bad_size() {
    let meta = BlockMetadata::new(BlockId::new(3));
    assert!(meta.validate().is_err());
    assert!(BlockMetadata::decode(&[0u8; 8]).is_err());
}

#[test]
fn pack_header_round_trip() {
    let h = PackHeader::new(PackId::new(1), DatabaseId::new(2), 1024).with_checksum();
    let b = h.encode();
    assert_eq!(PackHeader::decode(&b).expect("ok"), h);
}

#[test]
fn pack_header_rejects_bad_inputs() {
    let h = PackHeader::new(PackId::new(1), DatabaseId::new(2), 1024).with_checksum();
    let b = h.encode();
    assert!(PackHeader::decode(&b[..10]).is_err());
    let mut m = b;
    m[0] = b'X';
    assert!(PackHeader::decode(&m).is_err());
    let mut v = b;
    v[4] = 0xFF;
    assert!(PackHeader::decode(&v).is_err());
}

#[test]
fn pack_footer_round_trip_and_rejects_corruption() {
    let f = PackFooter::new(PackId::new(1), DatabaseId::new(2)).with_checksum();
    let b = f.encode();
    assert_eq!(PackFooter::decode(&b).expect("ok"), f);
    let mut bad = b;
    bad[10] ^= 0xFF;
    assert!(PackFooter::decode(&bad).is_err());
    assert!(PackFooter::decode(&b[..10]).is_err());
}

#[test]
fn block_directory_entry_round_trip_and_alignment() {
    let e = BlockDirectoryEntry {
        block_id: BlockId::new(9),
        byte_offset: BLOCK_SIZE,
        generation_id: GenerationId::new(1),
        lsn: Lsn::new(2),
    };
    e.validate().expect("valid");
    let b = e.encode();
    assert_eq!(BlockDirectoryEntry::decode(&b).expect("ok"), e);
    assert!(BlockDirectoryEntry::new(BlockId::new(1), 1)
        .validate()
        .is_err());
    assert!(BlockDirectoryEntry::decode(&[0u8; 4]).is_err());
}

#[test]
fn physical_location_validation_and_alignment() {
    let ok = PhysicalLocation::new(
        0,
        PackId::new(1),
        BlockId::new(2),
        PageId::new(3),
        PAGE_SIZE,
    );
    ok.validate().expect("valid");
    let bad = PhysicalLocation::new(0, PackId::new(1), BlockId::new(2), PageId::new(3), 1);
    assert!(bad.validate().is_err());
    let zp = PhysicalLocation::new(
        0,
        PackId::new(1),
        BlockId::new(2),
        PageId::new(0),
        PAGE_SIZE,
    );
    assert!(zp.validate().is_err());
}

#[test]
fn corruption_hooks_detect_bad_buffers() {
    let h = PageHeader::new(PageId::new(7), PageType::Leaf).with_checksum();
    assert_eq!(check_page_header(&h.encode()), IntegrityCheck::Ok);
    assert!(matches!(
        check_page_header(&[0u8; 4]),
        IntegrityCheck::Corrupt { .. }
    ));
    let p = PackHeader::new(PackId::new(1), DatabaseId::new(2), 8).with_checksum();
    assert_eq!(check_pack_header(&p.encode()), IntegrityCheck::Ok);
    assert!(matches!(
        check_pack_header(&[0u8; 4]),
        IntegrityCheck::Corrupt { .. }
    ));
}
