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
// Moved from `src/layout/schemas.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{DatabaseId, SchemaId};
use plomid_storage::layout::schemas::{
    schema_dir_name, schema_from_dir_name, SchemaMetadata, SCHEMA_META_HEADER_LEN,
};

#[test]
fn directory_names_round_trip_through_identity() {
    assert_eq!(schema_dir_name(SchemaId::new(1)), "S-00000000000000000001");
    assert_eq!(
        schema_from_dir_name("S-00000000000000000042"),
        Some(SchemaId::new(42))
    );
    assert_eq!(schema_from_dir_name("S-42"), None);
    assert_eq!(schema_from_dir_name("DB-00000000000000000042"), None);
}

#[test]
fn metadata_round_trips_and_detects_corruption() {
    let record = SchemaMetadata::new(SchemaId::new(3), DatabaseId::new(1)).expect("record");
    let bytes = record.encode().expect("encode");
    assert_eq!(bytes.len(), SCHEMA_META_HEADER_LEN);
    assert_eq!(SchemaMetadata::decode(&bytes).expect("decode"), record);
    let mut bad_magic = bytes.clone();
    bad_magic[0] ^= 0xFF;
    assert!(SchemaMetadata::decode(&bad_magic).is_err());
    let mut bad_checksum = bytes.clone();
    bad_checksum[20] ^= 0x01;
    assert!(SchemaMetadata::decode(&bad_checksum).is_err());
    assert!(SchemaMetadata::decode(&bytes[..10]).is_err());
}

#[test]
fn zero_identity_is_rejected() {
    assert!(SchemaMetadata::new(SchemaId::new(0), DatabaseId::new(1)).is_err());
    assert!(SchemaMetadata::new(SchemaId::new(1), DatabaseId::new(0)).is_err());
}
