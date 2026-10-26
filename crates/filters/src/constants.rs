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
//! Filter format constants.
//!
//! The byte-level constants — magics, format versions, XOR parameters,
//! footer size — live in [`plomid_core`] as the single source of truth for
//! the whole engine and are re-exported here, so a filter module imports
//! exactly one constants module (the same convention the columnar `format`
//! module follows).

pub use plomid_core::{
    XOR_ALPHA_DEN, XOR_ALPHA_NUM, XOR_FILTER_FORMAT_VERSION, XOR_FILTER_MAGIC,
    XOR_FINGERPRINT_BITS, XOR_FINGERPRINT_MODULUS, XOR_FOOTER_LEN, XOR_MAX_BUILD_ATTEMPTS,
    XOR_MAX_ELEMENTS, XOR_MAX_SLOTS, XOR_SLOTS_SLACK,
};
