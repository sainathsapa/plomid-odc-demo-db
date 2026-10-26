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
// Moved from `src/registry.rs` inline `#[cfg(test)]` — public-API only.
use plomid_types::registry::{FIRST_USER_OID, NO_TYPEMOD};
use plomid_types::{PgType, SerialKind, TypeDef, TypeRegistry};

#[test]
fn builtin_lookup_by_name_and_alias() {
    assert_eq!(PgType::by_name("integer"), Some(PgType::Int4));
    assert_eq!(PgType::by_name("INT8"), Some(PgType::Int8));
    assert_eq!(PgType::by_name("boolean"), Some(PgType::Bool));
    assert_eq!(PgType::by_name("double precision"), Some(PgType::Float8));
    assert_eq!(PgType::by_name("bit varying"), Some(PgType::VarBit));
    assert_eq!(PgType::by_name("timestamptz"), Some(PgType::Timestamptz));
    assert_eq!(PgType::by_name("nonexistent"), None);
}

#[test]
fn all_builtin_oids_are_unique() {
    let mut oids: Vec<u32> = PgType::all().map(|t| t.oid().raw()).collect();
    oids.sort_unstable();
    let len = oids.len();
    oids.dedup();
    assert_eq!(oids.len(), len, "duplicate OIDs in builtin type list");
}

#[test]
fn oids_match_postgresql() {
    assert_eq!(PgType::Int4.oid().raw(), 23);
    assert_eq!(PgType::Text.oid().raw(), 25);
    assert_eq!(PgType::Numeric.oid().raw(), 1700);
    assert_eq!(PgType::Jsonb.oid().raw(), 3802);
    assert_eq!(PgType::Int4Range.oid().raw(), 3904);
    assert_eq!(PgType::Int4MultiRange.oid().raw(), 4451);
    assert_eq!(PgType::Uuid.oid().raw(), 2950);
    assert_eq!(PgType::RegClass.oid().raw(), 2205);
    assert_eq!(PgType::Timestamptz.oid().raw(), 1184);
    assert_eq!(PgType::Macaddr8.oid().raw(), 774);
    assert_eq!(PgType::Int4.array_oid().unwrap().raw(), 1007);
}

#[test]
fn pseudo_types_cannot_be_columns() {
    assert!(PgType::Any.is_pseudo());
    assert!(!PgType::Any.can_be_column_type());
    assert!(PgType::Trigger.is_pseudo());
    assert!(PgType::Int4.can_be_column_type());
    assert!(PgType::Cstring.is_pseudo());
}

#[test]
fn range_elements() {
    assert_eq!(PgType::Int4Range.range_element(), Some(PgType::Int4.oid()));
    assert_eq!(
        PgType::NumMultiRange.range_element(),
        Some(PgType::Numeric.oid())
    );
    assert_eq!(PgType::Text.range_element(), None);
}

#[test]
fn serial_kinds() {
    assert_eq!(SerialKind::by_name("serial"), Some(SerialKind::Regular));
    assert_eq!(SerialKind::by_name("serial2"), Some(SerialKind::Small));
    assert_eq!(SerialKind::by_name("bigserial"), Some(SerialKind::Big));
    assert_eq!(SerialKind::Regular.base(), PgType::Int4);
    let registry = TypeRegistry::new();
    assert!(matches!(
        registry.resolve("bigserial").unwrap().def,
        TypeDef::Serial(SerialKind::Big)
    ));
}

#[test]
fn registry_resolve_and_register_user_types() {
    let mut registry = TypeRegistry::new();
    assert!(matches!(
        registry.resolve("text").unwrap().def,
        TypeDef::Builtin(PgType::Text)
    ));
    let enum_oid = registry
        .register_enum("mood", vec!["sad".into(), "happy".into()])
        .expect("register enum");
    assert!(enum_oid.raw() >= FIRST_USER_OID);
    assert!(registry.resolve("mood").is_ok());
    assert_eq!(registry.enum_labels(enum_oid).unwrap().len(), 2);
    let domain_oid = registry
        .register_domain("posint", PgType::Int4.oid(), NO_TYPEMOD, true)
        .expect("register domain");
    assert_eq!(
        registry.domain_base(domain_oid),
        Some((PgType::Int4.oid(), NO_TYPEMOD, true))
    );
    assert!(registry.resolve("nosuchtype").is_err());
    assert!(registry.register_enum("mood", vec!["x".into()]).is_err());
}
