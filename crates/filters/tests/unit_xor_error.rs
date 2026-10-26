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
// Moved from `src/xor/error.rs` inline `#[cfg(test)]` — public-API only.
use plomid_filters::xor::error::XorError;

#[test]
fn display_states_the_failing_condition() {
    assert_eq!(
        XorError::BuildFailed { attempts: 5 }.to_string(),
        "xor filter construction failed after 5 attempts"
    );
    assert_eq!(
        XorError::TooManyElements {
            requested: 10,
            limit: 4
        }
        .to_string(),
        "xor filter holds 10 elements, limit 4"
    );
}
