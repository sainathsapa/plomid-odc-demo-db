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
//! `Send + Sync` eligibility for parallel scan-aggregation building blocks.
//!
//! A parallel scan-aggregation must share these values across worker threads
//! (`&TableSchema` for decoding/evaluation, `Expression` slots and filters,
//! `SqlError` for cross-thread error propagation), so each must be
//! `Send + Sync`. This test pins that property at compile time: removing
//! thread-safety from any of these types breaks the build here.
//!
//! `AggregateSlot` (`crates/executor/src/query/aggregate.rs`) and
//! `ColumnMask` (`crates/executor/src/projection.rs`) are `pub(crate)`, so an
//! integration test cannot name them directly. Their fields are asserted here
//! instead: `AggregateSlot` holds `String`, `Option<Expression>` (`arg`,
//! `second_arg`, `filter`) and `bool` (`distinct`); `ColumnMask` holds
//! `Vec<bool>` and `usize`. Every one of those field types is `Send + Sync`
//! and neither struct contains interior mutability, raw pointers, or other
//! thread-hostile members (verified by source inspection), so both structs
//! are transitively `Send + Sync`.
//!
//! Integer-family column discrimination usable from `crates/executor`:
//!
//! * `plomid_types::TypeOid::INT2` (raw 21, `smallint` / `int2`)
//! * `plomid_types::TypeOid::INT4` (raw 23, `integer` / `int` / `int4`)
//! * `plomid_types::TypeOid::INT8` (raw 20, `bigint` / `int8`)
//!
//! The canonical match site is `columnar_row_to_values` in
//! `crates/executor/src/query/scan.rs`, which reads
//! `schema.columns[index].col_type.type_oid` (where `col_type` is
//! `plomid_types::ColumnType`) and compares it with `==` against those three
//! constants, falling back to text parsing for every other OID.

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn shared_scan_aggregation_state_is_send_sync() {
    // Shared scan context: workers hold `&TableSchema` while decoding rows.
    assert_send_sync::<plomid_sql::TableSchema>();
    assert_send_sync::<&plomid_sql::TableSchema>();
    // Aggregate slots, filters, and group keys cross thread boundaries.
    assert_send_sync::<plomid_sql::Expression>();
    assert_send_sync::<Option<plomid_sql::Expression>>();
    assert_send_sync::<Vec<plomid_sql::Expression>>();
    // Errors propagate back across the worker join boundary.
    assert_send_sync::<plomid_sql::SqlError>();
    assert_send_sync::<plomid_executor::SqlError>();
    // Integer-family OIDs used for eligibility discrimination.
    assert_send_sync::<plomid_types::TypeOid>();
    assert_send_sync::<plomid_types::ColumnType>();

    // Field types comprising the `pub(crate)` parallel-scan structs
    // (`AggregateSlot`: `String` + three `Option<Expression>` + `bool`;
    // `ColumnMask`: `Vec<bool>` + `usize`). See the module docs above.
    assert_send_sync::<String>();
    assert_send_sync::<bool>();
    assert_send_sync::<Vec<bool>>();
    assert_send_sync::<usize>();

    // Trivial runtime assertions so the test does work beyond compiling.
    let expr = plomid_sql::Expression::ColumnRef("v_int".to_string());
    assert!(matches!(expr, plomid_sql::Expression::ColumnRef(_)));
    assert_eq!(plomid_types::TypeOid::INT2.raw(), 21);
    assert_eq!(plomid_types::TypeOid::INT4.raw(), 23);
    assert_eq!(plomid_types::TypeOid::INT8.raw(), 20);
}
