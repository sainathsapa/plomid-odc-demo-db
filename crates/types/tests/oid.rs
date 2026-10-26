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
// Moved from `src/oid.rs` inline `#[cfg(test)]` — public-API only.
use plomid_types::TypeOid;

#[test]
fn known_oids() {
    assert_eq!(TypeOid::INT4.raw(), 23);
    assert_eq!(TypeOid::TEXT.raw(), 25);
    assert_eq!(TypeOid::NUMERIC.raw(), 1700);
    assert_eq!(TypeOid::JSONB.raw(), 3802);
    assert_eq!(TypeOid::INT4RANGE.raw(), 3904);
    assert_eq!(TypeOid::INT4MULTIRANGE.raw(), 4451);
}
