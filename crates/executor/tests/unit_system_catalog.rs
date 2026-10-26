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
// Moved from src/system_catalog.rs inline #[cfg(test)].
use plomid_executor::system_catalog::*;

#[test]
fn is_relation_handles_case_insensitive_schema() {
    assert!(SystemCatalog::is_relation("information_schema.tables"));
    assert!(SystemCatalog::is_relation("INFORMATION_SCHEMA.TABLES"));
    assert!(SystemCatalog::is_relation("Information_Schema.Tables"));
    assert!(SystemCatalog::is_relation("pg_catalog.pg_class"));
    assert!(SystemCatalog::is_relation("PG_CATALOG.PG_CLASS"));
    assert!(SystemCatalog::is_relation("INFORMATION_SCHEMA.ROUTINES"));
    // pg_default_acl should be recognized as a system catalog relation
    assert!(SystemCatalog::is_relation("pg_default_acl"));
    assert!(SystemCatalog::is_relation("PG_DEFAULT_ACL"));
    assert!(SystemCatalog::is_relation("pg_catalog.pg_default_acl"));
}

#[test]
fn normalize_relation_folds_case_and_strips_schema() {
    // System / information-schema views match by bare lowercase table name.
    assert_eq!(normalize_relation("INFORMATION_SCHEMA.TABLES"), "tables");
    assert_eq!(normalize_relation("PG_CATALOG.PG_CLASS"), "pg_class");
    assert_eq!(normalize_relation("routines"), "routines");
    assert_eq!(
        normalize_relation("INFORMATION_SCHEMA.ROUTINES"),
        "routines"
    );
}
