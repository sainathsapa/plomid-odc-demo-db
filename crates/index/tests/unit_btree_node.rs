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
// Moved from `src/btree/node.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{PageId, RowId};
use plomid_index::btree::constants::{LEAF, NODE_HEADER_SIZE, NODE_MAGIC};
use plomid_index::btree::node::*;

fn rid(v: u64) -> RowId {
    RowId::new(v)
}

fn leaf_entry(key: &[u8], rows: &[RowId]) -> LeafEntry {
    LeafEntry {
        key: key.to_vec(),
        row_ids: rows.to_vec(),
    }
}

#[test]
fn leaf_round_trips_with_links() {
    let node = Node::Leaf {
        prev: Some(PageId::new(3)),
        next: Some(PageId::new(7)),
        entries: vec![
            leaf_entry(b"alpha", &[rid(1)]),
            leaf_entry(b"beta", &[rid(2), rid(5)]),
        ],
    };
    let encoded = encode_node(&node).unwrap();
    assert_eq!(encoded[..4], NODE_MAGIC);
    assert_eq!(encoded[4], LEAF);
    let decoded = decode_node(&encoded).unwrap();
    assert_eq!(decoded, node);
}

#[test]
fn internal_round_trips_with_separators() {
    let node = Node::Internal {
        first: PageId::new(2),
        separators: vec![
            InternalEntry {
                key: b"m".to_vec(),
                child: PageId::new(10),
            },
            InternalEntry {
                key: b"z".to_vec(),
                child: PageId::new(20),
            },
        ],
    };
    let encoded = encode_node(&node).unwrap();
    let decoded = decode_node(&encoded).unwrap();
    assert_eq!(decoded, node);
}

#[test]
fn no_page_link_decodes_to_none() {
    let node = Node::Leaf {
        prev: None,
        next: None,
        entries: vec![leaf_entry(b"k", &[rid(1)])],
    };
    let encoded = encode_node(&node).unwrap();
    let decoded = decode_node(&encoded).unwrap();
    match decoded {
        Node::Leaf { prev, next, .. } => {
            assert!(prev.is_none());
            assert!(next.is_none());
        }
        _ => panic!("expected leaf"),
    }
}

#[test]
fn rejects_bad_magic() {
    let mut data = vec![0u8; NODE_HEADER_SIZE];
    assert!(decode_node(&data).is_err());
    data[..4].copy_from_slice(b"XXXX");
    assert!(decode_node(&data).is_err());
}

#[test]
fn rejects_truncated_node() {
    assert!(decode_node(&[0u8; 8]).is_err());
}

#[test]
fn rejects_unknown_node_kind() {
    let mut data = vec![0u8; NODE_HEADER_SIZE];
    data[..4].copy_from_slice(&NODE_MAGIC);
    data[4] = 99;
    assert!(decode_node(&data).is_err());
}
