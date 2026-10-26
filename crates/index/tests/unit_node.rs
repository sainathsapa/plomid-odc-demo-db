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
// Moved from `src/node.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::RowId;
use plomid_index::constants::{ART_NODE16_CAP, ART_NODE256_CAP, ART_NODE48_CAP, ART_NODE4_CAP};
use plomid_index::error::ArtError;
use plomid_index::leaf::LeafValues;
use plomid_index::node::{Node, NodeKind};

/// A stand-in child node; its contents are irrelevant to these tests.
fn child() -> Box<Node> {
    Box::new(Node::node4(Vec::new()))
}

/// Edge bytes present in a node, in traversal order.
fn edges(node: &Node) -> Vec<u8> {
    let mut found = Vec::new();
    node.for_each_child(|byte, _| found.push(byte));
    found
}

#[test]
fn node4_keeps_edge_bytes_sorted() {
    let mut node = Node::node4(b"pfx".to_vec());
    for byte in [9u8, 3, 7] {
        node.insert_child(byte, child()).expect("space available");
    }
    assert_eq!(node.kind(), NodeKind::Node4);
    assert_eq!(node.count(), 3);
    assert_eq!(node.prefix(), b"pfx");
    // Ascending order regardless of insertion order.
    assert_eq!(edges(&node), vec![3, 7, 9]);
    assert!(node.child(7).is_some());
    assert!(node.child(8).is_none());
}

#[test]
fn occupied_edge_byte_and_full_node_are_rejected() {
    let mut node = Node::node4(Vec::new());
    node.insert_child(5, child()).expect("first insert");
    assert_eq!(
        node.insert_child(5, child()),
        Err(ArtError::InvalidOperation)
    );

    for byte in 6u8..=7 {
        node.insert_child(byte, child()).expect("within capacity");
    }
    // A fourth child fills Node4 exactly.
    node.insert_child(8, child()).expect("capacity is four");
    assert!(node.is_full());
    assert_eq!(
        node.insert_child(9, child()),
        Err(ArtError::InvalidOperation)
    );
    assert_eq!(node.count(), ART_NODE4_CAP);
}

#[test]
fn growth_preserves_every_entry() {
    let mut node = Node::node4(Vec::new());
    // Inserting the fifth child forces Node4 → Node16.
    for byte in 0u8..ART_NODE4_CAP as u8 {
        node.insert_child_growing(byte, child()).expect("insert");
    }
    node.insert_child_growing(4, child())
        .expect("grow to Node16");
    assert_eq!(node.kind(), NodeKind::Node16);
    assert_eq!(node.count(), 5);
    for byte in 0u8..5 {
        assert!(node.child(byte).is_some(), "byte {byte} survived growth");
    }

    // Fill Node16 and force Node16 → Node48.
    for byte in 5u8..ART_NODE16_CAP as u8 {
        node.insert_child_growing(byte, child()).expect("insert");
    }
    node.insert_child_growing(16, child())
        .expect("grow to Node48");
    assert_eq!(node.kind(), NodeKind::Node48);
    assert_eq!(node.count(), 17);
    let expected: Vec<u8> = (0u8..=16).collect();
    assert_eq!(edges(&node), expected);

    // Fill Node48 and force Node48 → Node256.
    for byte in 17u8..ART_NODE48_CAP as u8 {
        node.insert_child_growing(byte, child()).expect("insert");
    }
    node.insert_child_growing(48, child())
        .expect("grow to Node256");
    assert_eq!(node.kind(), NodeKind::Node256);
    assert_eq!(node.count(), 49);
    let expected: Vec<u8> = (0u8..=48).collect();
    assert_eq!(edges(&node), expected);
    for byte in 0u8..=48 {
        assert!(node.child(byte).is_some());
    }
    assert!(node.child(49).is_none());
}

#[test]
fn shrink_preserves_remaining_entries() {
    let mut node = Node::node4(Vec::new());
    for byte in 0u8..=ART_NODE48_CAP as u8 {
        node.insert_child_growing(byte, child()).expect("insert");
    }
    assert_eq!(node.kind(), NodeKind::Node256);

    // 49 children → one removal brings the count to the Node256 shrink
    // threshold (48), which is where Node256 → Node48 happens.
    assert!(node.remove_child(48).is_some());
    let mut node = node.shrink_if_needed();
    assert_eq!(node.kind(), NodeKind::Node48);
    assert_eq!(node.count(), ART_NODE48_CAP);
    assert_eq!(
        edges(&node),
        (0u8..ART_NODE48_CAP as u8).collect::<Vec<_>>()
    );

    // Node48 → Node16 at the 16-child threshold.
    for byte in 16u8..ART_NODE48_CAP as u8 {
        assert!(node.remove_child(byte).is_some());
    }
    let mut node = node.shrink_if_needed();
    assert_eq!(node.kind(), NodeKind::Node16);
    assert_eq!(node.count(), ART_NODE16_CAP);

    // Node16 → Node4 at the 4-child threshold.
    for byte in ART_NODE4_CAP as u8..ART_NODE16_CAP as u8 {
        assert!(node.remove_child(byte).is_some());
    }
    let node = node.shrink_if_needed();
    assert_eq!(node.kind(), NodeKind::Node4);
    assert_eq!(node.count(), ART_NODE4_CAP);
    for byte in 0u8..ART_NODE4_CAP as u8 {
        assert!(node.child(byte).is_some(), "byte {byte} survived shrink");
    }
}

#[test]
fn shrink_is_deferred_above_the_threshold() {
    let mut node = Node::node4(Vec::new());
    for byte in 0u8..=ART_NODE48_CAP as u8 {
        node.insert_child_growing(byte, child()).expect("insert");
    }
    // 49 children is above the Node256 shrink threshold, so the family stays.
    let node = node.shrink_if_needed();
    assert_eq!(node.kind(), NodeKind::Node256);
}

#[test]
fn removal_leaves_no_stale_reference() {
    let mut node = Node::node4(Vec::new());
    for byte in [4u8, 5, 6] {
        node.insert_child(byte, child()).expect("insert");
    }
    assert!(node.remove_child(5).is_some());
    assert_eq!(node.count(), 2);
    assert_eq!(edges(&node), vec![4, 6]);
    assert!(node.child(5).is_none());
    // Removing a byte that is not present changes nothing.
    assert!(node.remove_child(5).is_none());
    assert_eq!(node.count(), 2);
}

#[test]
fn node48_reuses_freed_slots() {
    let mut node = Node::node4(Vec::new());
    for byte in 0u8..=ART_NODE48_CAP as u8 {
        node.insert_child_growing(byte, child()).expect("insert");
    }
    assert_eq!(node.kind(), NodeKind::Node256, "grew past Node48");

    // 49 children → one removal returns to the Node48 threshold, where the
    // node family drops back to Node48.
    assert!(node.remove_child(48).is_some());
    let mut node = node.shrink_if_needed();
    assert_eq!(node.kind(), NodeKind::Node48);
    assert_eq!(node.count(), ART_NODE48_CAP);

    // Free a slot, then reuse it: count stays right and lookups still work.
    assert!(node.remove_child(7).is_some());
    assert_eq!(node.count(), ART_NODE48_CAP - 1);
    node.insert_child_growing(200, child()).expect("freed slot");
    assert_eq!(node.count(), ART_NODE48_CAP);
    assert!(node.child(200).is_some());
    assert!(node.child(7).is_none());
}

#[test]
fn prefix_and_terminal_slots_are_independent_of_children() {
    let mut node = Node::node4(Vec::new());
    node.insert_child(1, child()).expect("insert");
    assert!(node.terminal().is_none());

    node.terminal_entry().insert(RowId::new(11));
    assert_eq!(node.terminal().map(LeafValues::len), Some(1));

    node.set_prefix(b"ab".to_vec());
    node.truncate_prefix(1);
    assert_eq!(node.prefix(), b"a");
    assert_eq!(node.count(), 1);
    assert!(node.take_terminal().is_some());
    assert!(node.terminal().is_none());
    // The child is untouched by terminal and prefix edits.
    assert!(node.child(1).is_some());
    assert!(!node.is_empty());
}

#[test]
fn family_capacities_and_thresholds_are_monotonic() {
    assert_eq!(NodeKind::Node4.capacity(), ART_NODE4_CAP);
    assert_eq!(NodeKind::Node256.capacity(), ART_NODE256_CAP);
    assert!(NodeKind::Node16.shrink_threshold() < NodeKind::Node16.capacity());
    assert!(NodeKind::Node48.shrink_threshold() < NodeKind::Node48.capacity());
    assert!(NodeKind::Node256.shrink_threshold() < NodeKind::Node256.capacity());
    assert_eq!(NodeKind::Node4.shrunk(), None);
    assert_eq!(NodeKind::Node256.grown(), None);
    assert_eq!(NodeKind::Node4.grown(), Some(NodeKind::Node16));
}
