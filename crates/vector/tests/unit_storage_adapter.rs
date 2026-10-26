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
// Moved from `src/storage/adapter.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::r#type::VectorElementType;
use plomid_vector::storage::adapter::*;
use plomid_vector::value::DenseVectorValue;
use plomid_vector::VectorValue;

fn dense(vals: &[f32]) -> VectorValue {
    VectorValue::Dense(
        DenseVectorValue::from_f32(VectorElementType::Float32, vals.to_vec()).unwrap(),
    )
}

#[test]
fn inmemory_roundtrip_and_scan() {
    let mut s = InMemoryVectorStorage::new(Some(2));
    let id0 = s.append_payload(&dense(&[1.0, 2.0])).unwrap();
    let id1 = s.append_payload(&dense(&[3.0, 4.0])).unwrap();
    assert_eq!((id0, id1), (0, 1));
    assert_eq!(s.row_count(), 2);
    assert_eq!(s.read_payload(0).unwrap(), dense(&[1.0, 2.0]));
    s.write_payload(0, &dense(&[5.0, 6.0])).unwrap();
    let batch = s.scan_batch(0, 10).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].0, 0);
    // Wrong dims rejected.
    assert!(s.append_payload(&dense(&[1.0])).is_err());
}
