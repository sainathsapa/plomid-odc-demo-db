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
// Moved from `src/layout/tables.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{ObjectId, TableId};
use plomid_storage::layout::tables::{
    table_dir_name, table_from_dir_name, TableMetadata, TABLE_META_HEADER_LEN,
};

#[test]
fn directory_names_round_trip_through_identity() {
    assert_eq!(table_dir_name(TableId::new(1)), "T-00000000000000000001");
    assert_eq!(
        table_from_dir_name("T-00000000000000000042"),
        Some(TableId::new(42))
    );
    assert_eq!(table_from_dir_name("T-42"), None);
    assert_eq!(table_from_dir_name("I-00000000000000000042"), None);
}

#[test]
fn metadata_round_trips_and_detects_corruption() {
    let record = TableMetadata::new(TableId::new(3), ObjectId::new(3)).expect("record");
    let bytes = record.encode().expect("encode");
    assert_eq!(bytes.len(), TABLE_META_HEADER_LEN);
    assert_eq!(TableMetadata::decode(&bytes).expect("decode"), record);
    let mut bad_magic = bytes.clone();
    bad_magic[0] ^= 0xFF;
    assert!(TableMetadata::decode(&bad_magic).is_err());
    let mut bad_version = bytes.clone();
    bad_version[4] = 0xFF;
    assert!(TableMetadata::decode(&bad_version).is_err());
    let mut bad_checksum = bytes.clone();
    bad_checksum[12] ^= 0x01;
    assert!(TableMetadata::decode(&bad_checksum).is_err());
    assert!(TableMetadata::decode(&bytes[..10]).is_err());
    assert!(TableMetadata::decode(&[bytes.clone(), vec![0]].concat()).is_err());
}

#[test]
fn mismatched_object_identity_is_rejected() {
    assert!(TableMetadata::new(TableId::new(1), ObjectId::new(2)).is_err());
    assert!(TableMetadata::new(TableId::new(0), ObjectId::new(0)).is_err());
}
