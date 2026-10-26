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
// Moved from `src/parser/types.rs` inline `#[cfg(test)]` — public-API only.
use plomid_sql::parser::types::*;
use plomid_sql::{ColumnType, InMemoryCatalog, Lexer, Parser};
use plomid_types::PgType;

fn column_type_of(sql: &str) -> ColumnType {
    let tokens = Lexer::new(sql).lex().unwrap();
    let mut catalog = InMemoryCatalog::new();
    let mut parser = Parser::new(tokens, &mut catalog);
    parse_column_type(&mut parser).unwrap()
}

#[test]
fn vector_family_type_forms() {
    // Canonical forms pack the full kind/element/dimension identity into
    // the typmod via the authoritative vector type system.
    let canonical = column_type_of("VECTOR(FLOAT32, 4)");
    assert_eq!(canonical.type_oid, PgType::Vector.oid());
    let decoded = plomid_vector::VectorType::typmod_decode(canonical.typmod).unwrap();
    assert_eq!(decoded.dimension, 4);
    assert_eq!(decoded.element, plomid_vector::VectorElementType::Float32);
    assert_eq!(decoded.kind, plomid_vector::VectorKind::Dense);
    // `VECTOR(n)` shorthand keeps working (back-compat -> FLOAT32 dense).
    let shorthand = column_type_of("VECTOR(4)");
    assert_eq!(shorthand.type_oid, PgType::Vector.oid());
    assert_eq!(shorthand.typmod, canonical.typmod);
    // Other kinds/elements round-trip their own identity.
    let sparse = column_type_of("SPARSE_VECTOR(FLOAT16, 8)");
    let decoded_sparse = plomid_vector::VectorType::typmod_decode(sparse.typmod).unwrap();
    assert_eq!(decoded_sparse.dimension, 8);
    assert_eq!(decoded_sparse.kind, plomid_vector::VectorKind::Sparse);
    assert_eq!(
        decoded_sparse.element,
        plomid_vector::VectorElementType::Float16
    );
    let binary = column_type_of("VECTOR(BINARY, 64)");
    let decoded_binary = plomid_vector::VectorType::typmod_decode(binary.typmod).unwrap();
    assert_eq!(decoded_binary.kind, plomid_vector::VectorKind::Binary);
}
