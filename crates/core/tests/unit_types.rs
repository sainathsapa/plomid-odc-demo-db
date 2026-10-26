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
// Moved from `src/types.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::types::{
    BlockId, CommitTimestamp, DatabaseId, GenerationId, Lsn, ObjectId, PackId, PageId, RowId,
    SegmentId, Timestamp, TxnId, CSN,
};
use std::mem::size_of;

#[test]
fn values_round_trip() {
    assert_eq!(PageId::new(42).get(), 42);
    assert_eq!(Lsn::from_u64(7).as_u64(), 7);
    assert_eq!(TxnId::new(11).get(), 11);
    assert_eq!(Timestamp::from_u64(19).as_u64(), 19);
    assert_eq!(CommitTimestamp::from_u64(23).as_u64(), 23);
}

#[test]
fn lsn_ordering_matches_underlying_counter() {
    assert!(Lsn::new(7) < Lsn::new(8));
    assert!(Lsn::new(8) > Lsn::new(7));
    assert_eq!(Lsn::new(7).cmp(&Lsn::new(7)), std::cmp::Ordering::Equal);
}

#[test]
fn display_is_stable_and_type_specific() {
    assert_eq!(PageId::new(42).to_string(), "PageId(42)");
    assert_eq!(Lsn::new(7).to_string(), "LSN(7)");
    assert_eq!(TxnId::new(11).to_string(), "TxnId(11)");
    assert_eq!(Timestamp::new(19).to_string(), "Timestamp(19)");
    assert_eq!(CommitTimestamp::new(23).to_string(), "CommitTimestamp(23)");
}

#[test]
fn representations_are_zero_cost() {
    assert_eq!(size_of::<PageId>(), size_of::<u64>());
    assert_eq!(size_of::<Lsn>(), size_of::<u64>());
    assert_eq!(size_of::<TxnId>(), size_of::<u64>());
    assert_eq!(size_of::<Timestamp>(), size_of::<u64>());
    assert_eq!(size_of::<CommitTimestamp>(), size_of::<u64>());
}

#[test]
fn id_types_round_trip_through_bytes() {
    assert_eq!(
        PageId::from_le_bytes(PageId::new(42).to_le_bytes()),
        PageId::new(42)
    );
    assert_eq!(
        Lsn::from_le_bytes(Lsn::from_u64(7).to_le_bytes()),
        Lsn::from_u64(7)
    );
    assert_eq!(
        TxnId::from_le_bytes(TxnId::new(11).to_le_bytes()),
        TxnId::new(11)
    );
    assert_eq!(
        Timestamp::from_le_bytes(Timestamp::from_u64(19).to_le_bytes()),
        Timestamp::from_u64(19)
    );
    assert_eq!(
        CommitTimestamp::from_le_bytes(CommitTimestamp::from_u64(23).to_le_bytes()),
        CommitTimestamp::from_u64(23)
    );
    assert_eq!(
        DatabaseId::from_le_bytes(DatabaseId::new(1).to_le_bytes()),
        DatabaseId::new(1)
    );
    assert_eq!(
        ObjectId::from_le_bytes(ObjectId::new(2).to_le_bytes()),
        ObjectId::new(2)
    );
    assert_eq!(
        GenerationId::from_le_bytes(GenerationId::new(3).to_le_bytes()),
        GenerationId::new(3)
    );
    assert_eq!(
        SegmentId::from_le_bytes(SegmentId::new(4).to_le_bytes()),
        SegmentId::new(4)
    );
    assert_eq!(
        PackId::from_le_bytes(PackId::new(5).to_le_bytes()),
        PackId::new(5)
    );
    assert_eq!(
        BlockId::from_le_bytes(BlockId::new(6).to_le_bytes()),
        BlockId::new(6)
    );
    assert_eq!(
        RowId::from_le_bytes(RowId::new(7).to_le_bytes()),
        RowId::new(7)
    );
    assert_eq!(CSN::from_le_bytes(CSN::new(8).to_le_bytes()), CSN::new(8));
}

#[test]
fn zero_detection_is_correct() {
    assert!(PageId::new(0).is_zero());
    assert!(Lsn::new(0).is_zero());
    assert!(TxnId::new(0).is_zero());
    assert!(Timestamp::new(0).is_zero());
    assert!(CommitTimestamp::new(0).is_zero());
    assert!(DatabaseId::new(0).is_zero());
    assert!(ObjectId::new(0).is_zero());
    assert!(GenerationId::new(0).is_zero());
    assert!(SegmentId::new(0).is_zero());
    assert!(PackId::new(0).is_zero());
    assert!(BlockId::new(0).is_zero());
    assert!(RowId::new(0).is_zero());
    assert!(CSN::new(0).is_zero());
}

#[test]
fn non_zero_values_are_detected() {
    assert!(!PageId::new(1).is_zero());
    assert!(!Lsn::new(1).is_zero());
    assert!(!TxnId::new(1).is_zero());
    assert!(!ObjectId::new(1).is_zero());
    assert!(!RowId::new(1).is_zero());
    assert!(!CSN::new(1).is_zero());
}

#[test]
fn id_types_are_zero_cost_representatives() {
    assert_eq!(size_of::<PageId>(), size_of::<u64>());
    assert_eq!(size_of::<Lsn>(), size_of::<u64>());
    assert_eq!(size_of::<TxnId>(), size_of::<u64>());
    assert_eq!(size_of::<Timestamp>(), size_of::<u64>());
    assert_eq!(size_of::<CommitTimestamp>(), size_of::<u64>());
    assert_eq!(size_of::<DatabaseId>(), size_of::<u64>());
    assert_eq!(size_of::<ObjectId>(), size_of::<u64>());
    assert_eq!(size_of::<GenerationId>(), size_of::<u64>());
    assert_eq!(size_of::<SegmentId>(), size_of::<u64>());
    assert_eq!(size_of::<PackId>(), size_of::<u64>());
    assert_eq!(size_of::<BlockId>(), size_of::<u64>());
    assert_eq!(size_of::<RowId>(), size_of::<u64>());
    assert_eq!(size_of::<CSN>(), size_of::<u64>());
}

#[test]
fn all_id_types_display_is_stable_and_type_specific() {
    assert_eq!(PageId::new(42).to_string(), "PageId(42)");
    assert_eq!(Lsn::new(7).to_string(), "LSN(7)");
    assert_eq!(TxnId::new(11).to_string(), "TxnId(11)");
    assert_eq!(Timestamp::new(19).to_string(), "Timestamp(19)");
    assert_eq!(CommitTimestamp::new(23).to_string(), "CommitTimestamp(23)");
    assert_eq!(DatabaseId::new(1).to_string(), "DatabaseId(1)");
    assert_eq!(ObjectId::new(2).to_string(), "ObjectId(2)");
    assert_eq!(GenerationId::new(3).to_string(), "GenerationId(3)");
    assert_eq!(SegmentId::new(4).to_string(), "SegmentId(4)");
    assert_eq!(PackId::new(5).to_string(), "PackId(5)");
    assert_eq!(BlockId::new(6).to_string(), "BlockId(6)");
    assert_eq!(RowId::new(7).to_string(), "RowId(7)");
    assert_eq!(CSN::new(8).to_string(), "CSN(8)");
}
