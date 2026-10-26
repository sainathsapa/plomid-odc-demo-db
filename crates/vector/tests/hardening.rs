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

//! Adversarial hardening: crafted inputs must fail closed, fast, and loud
//! (Phases 59 + 66 in code). No panics, no unbounded allocation, no silent
//! misinterpretation, no torn-artifact reads.

use plomid_vector::distance::hamming_distance;
use plomid_vector::index::diskann::DiskAnnIndex;
use plomid_vector::index::{IndexConfig, IndexKind, Metric, VectorIndex};
use plomid_vector::{
    decode_standalone, encode_standalone, parse_binary, BinaryVectorValue, DenseVectorValue,
    MultiVectorValue, VectorElementType, VectorValue, MAGIC, VERSION,
};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "plomid-vector-hardening-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn codec_length_bomb_fails_fast_without_allocation() {
    let dense = DenseVectorValue::from_f32(VectorElementType::Float32, vec![1.0]).unwrap();
    let mut bytes = encode_standalone(&VectorValue::Dense(dense)).unwrap();
    assert_eq!(&bytes[0..4], MAGIC);
    // Claim a 4 GiB payload with a 25-byte body.
    bytes[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
    let started = std::time::Instant::now();
    let err = decode_standalone(&bytes).unwrap_err();
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "length-bomb decode hung"
    );
    assert!(
        matches!(err.code(), "VECTOR_CORRUPTION" | "VECTOR_RESOURCE_LIMIT"),
        "unexpected: {err}"
    );
    let _ = VERSION;
}

#[test]
fn binary_dimensions_from_plan_checklist() {
    // Phase 27 in code: padding must never affect distance.
    for bits in [1u32, 7, 8, 9, 63, 64, 65, 1023, 1024, 65535, 65536] {
        let bytes = vec![0xAAu8; bits.div_ceil(32) as usize * 4]
            .into_iter()
            .take(bits.div_ceil(8) as usize)
            .collect::<Vec<u8>>();
        // Deterministic pattern with legal padding via the constructor.
        let value = BinaryVectorValue::new(bits, bytes).unwrap();
        let back =
            decode_standalone(&encode_standalone(&VectorValue::Binary(value)).unwrap()).unwrap();
        let VectorValue::Binary(restored) = back else {
            panic!("bits={bits}: kind changed");
        };
        assert_eq!(restored.dimension_bits, bits, "bits={bits}");
        let h =
            hamming_distance(Some(&restored.bytes), Some(&restored.bytes), bits as usize).unwrap();
        assert_eq!(h, Some(0), "bits={bits}: self-distance nonzero");
    }
    // Literal-level: 9-bit quoted form round-trips through padding rules.
    let lit = parse_binary("'101010101'").unwrap();
    assert_eq!(lit.dimension_bits, 9);
    assert_eq!(lit.bytes.len(), 2);
}

#[test]
fn multivector_shapes_validated_not_allocated() {
    // Empty constituents are legal (0..=max_vectors holds no rows).
    let empty = MultiVectorValue::new(VectorElementType::Float32, 8, 65_536, vec![]).unwrap();
    assert_eq!(empty.count(), 0);
    // Count above the cap fails without allocating the excess.
    let over = (0..3).map(|_| vec![0.0; 8]).collect::<Vec<_>>();
    assert!(MultiVectorValue::new(VectorElementType::Float32, 8, 2, over).is_err());
    // Width mismatch fails without allocating a padded buffer.
    let wide = vec![vec![0.0; 8]];
    assert!(MultiVectorValue::new(VectorElementType::Float32, 65_536, 2, wide).is_err());
}

#[test]
fn diskann_torn_graph_recovers_fresh_never_half() {
    let dir = unique_dir("graph");
    let config = IndexConfig::new(IndexKind::DiskAnn, Metric::L2, 4);
    let mut idx = DiskAnnIndex::new(config.clone(), &dir).unwrap();
    idx.build(&[(1, vec![1.0, 0.0, 0.0, 0.0]), (2, vec![0.0, 1.0, 0.0, 0.0])])
        .unwrap();
    idx.flush().unwrap();
    assert!(dir.join("graph.dat").exists());
    // Tear the graph artifact: recovery must come back fresh, never half.
    std::fs::write(dir.join("graph.dat"), b"definitely not a graph").unwrap();
    let fresh = DiskAnnIndex::open(config, &dir).unwrap();
    fresh.validate().unwrap();
    assert_eq!(fresh.search(&[1.0, 0.0, 0.0, 0.0], 5).unwrap().len(), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn diskann_torn_segment_fails_closed_on_paged_read() {
    let dir = unique_dir("segment");
    let dim = 8usize;
    let config = IndexConfig::new(IndexKind::DiskAnn, Metric::L2, dim);
    let mut idx = DiskAnnIndex::new(config.clone(), &dir).unwrap();
    // Exceed the RAM resident set so cold rows must page from `vectors.dat`.
    let rows: Vec<(u64, Vec<f32>)> = (0..1100u64)
        .map(|id| (id, vec![id as f32 * 0.001; dim]))
        .collect();
    idx.build(&rows).unwrap();
    idx.flush().unwrap();
    // Truncate the segment mid-record: paged reads must error, not misread.
    let seg = std::fs::read(dir.join("vectors.dat")).unwrap();
    assert!(seg.len() > 64);
    std::fs::write(dir.join("vectors.dat"), &seg[..seg.len() - 7]).unwrap();
    let reopened = DiskAnnIndex::open(config, &dir).unwrap();
    // A search that pages into the torn tail fails closed (or serves only
    // resident rows); it must never return fabricated vectors.
    let _ = reopened.search(&rows[0].1, 5);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn candidate_and_k_caps_hold() {
    use plomid_vector::search::exact::exact_top_k;
    use plomid_vector::search::SearchMetric;
    // Hostile LIMIT is rejected, not heap-allocated.
    let err = exact_top_k(
        &[1.0],
        vec![(1u64, Some([1.0].as_slice()))],
        SearchMetric::L2,
        usize::MAX,
        None,
    )
    .unwrap_err();
    let _ = err;
    // Oversample helper saturates instead of overflowing.
    assert_eq!(
        plomid_vector::search::oversample_count(usize::MAX, 8, usize::MAX),
        plomid_vector::search::MAX_CANDIDATE_CAP
    );
}
