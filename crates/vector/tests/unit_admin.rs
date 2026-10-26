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
// Moved from `src/admin.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::admin::*;
use plomid_vector::r#type::{VectorElementType, VectorKind};
use plomid_vector::r#type::{VectorMetric, VectorType};

fn manifest() -> IndexManifest {
    IndexManifest {
        name: "docs_embedding_hnsw".to_string(),
        state: IndexState::Ready,
        vector_type: VectorType::new(VectorKind::Dense, VectorElementType::Float32, 768).unwrap(),
        metric: VectorMetric::Cosine,
        format_version: 1,
        checkpoint_lsn: 42,
        build_params: "m=16,ef_construction=200".to_string(),
    }
}

#[test]
fn manifest_validates() {
    manifest().validate().unwrap();
    assert!(IndexState::parse("ready").unwrap().is_queryable());
    assert!(!IndexState::Failed.is_queryable());
    let mut bad = manifest();
    bad.format_version = 2;
    assert!(bad.validate().is_err());
}

#[test]
fn tombstone_ratio() {
    let s = IndexStats {
        vector_count: 1000,
        tombstone_count: 100,
        ..Default::default()
    };
    assert_eq!(s.tombstone_ratio_x1000(), 100);
}
