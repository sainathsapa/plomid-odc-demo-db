//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID — Platform for Modern Intelligence and Data
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0.

//! Privilege taxonomy: the complete vocabulary of grantable rights.
//!
//! Bit values are part of the persisted catalog encoding (see
//! `plomid-sql` meta sections): never renumber, only append.

/// Table-level privileges (`GRANT ... ON TABLE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TablePrivilege {
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
}

impl TablePrivilege {
    /// Stable bit for catalog persistence. Never renumber.
    pub fn bit(self) -> u16 {
        match self {
            Self::Select => 1,
            Self::Insert => 2,
            Self::Update => 4,
            Self::Delete => 8,
            Self::Truncate => 16,
        }
    }

    /// All table privileges (owner-equivalent set, `GRANT ALL`).
    pub fn all_bits() -> u16 {
        Self::Select.bit()
            | Self::Insert.bit()
            | Self::Update.bit()
            | Self::Delete.bit()
            | Self::Truncate.bit()
    }

    /// Parses one privilege keyword (case-insensitive). `ALL [PRIVILEGES]`
    /// expands to every table privilege.
    pub fn parse_list(words: &[String]) -> Option<Vec<Self>> {
        let mut out = Vec::new();
        for word in words {
            match word.to_ascii_uppercase().as_str() {
                "SELECT" => out.push(Self::Select),
                "INSERT" => out.push(Self::Insert),
                "UPDATE" => out.push(Self::Update),
                "DELETE" => out.push(Self::Delete),
                "TRUNCATE" => out.push(Self::Truncate),
                "ALL" | "ALL PRIVILEGES" | "ALL PRIVILEGE" => {
                    return Some(vec![
                        Self::Select,
                        Self::Insert,
                        Self::Update,
                        Self::Delete,
                        Self::Truncate,
                    ])
                }
                _ => return None,
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    /// True when `held` (a stored bit set) includes this privilege.
    pub fn held_by(self, held: u16) -> bool {
        held & self.bit() != 0
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Truncate => "TRUNCATE",
        }
    }
}

/// Schema-level privileges (`GRANT ... ON SCHEMA`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaPrivilege {
    Usage,
    Create,
}

impl SchemaPrivilege {
    /// Stable bit for catalog persistence. Never renumber.
    pub fn bit(self) -> u16 {
        match self {
            Self::Usage => 1,
            Self::Create => 2,
        }
    }

    pub fn all_bits() -> u16 {
        Self::Usage.bit() | Self::Create.bit()
    }

    pub fn parse_list(words: &[String]) -> Option<Vec<Self>> {
        let mut out = Vec::new();
        for word in words {
            match word.to_ascii_uppercase().as_str() {
                "USAGE" => out.push(Self::Usage),
                "CREATE" => out.push(Self::Create),
                "ALL" | "ALL PRIVILEGES" | "ALL PRIVILEGE" => {
                    return Some(vec![Self::Usage, Self::Create])
                }
                _ => return None,
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    pub fn held_by(self, held: u16) -> bool {
        held & self.bit() != 0
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Usage => "USAGE",
            Self::Create => "CREATE",
        }
    }
}

/// Database-level privileges (`GRANT ... ON DATABASE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatabasePrivilege {
    Connect,
    Create,
}

impl DatabasePrivilege {
    /// Stable bit for catalog persistence. Never renumber.
    pub fn bit(self) -> u16 {
        match self {
            Self::Connect => 1,
            Self::Create => 2,
        }
    }

    pub fn all_bits() -> u16 {
        Self::Connect.bit() | Self::Create.bit()
    }

    pub fn parse_list(words: &[String]) -> Option<Vec<Self>> {
        let mut out = Vec::new();
        for word in words {
            match word.to_ascii_uppercase().as_str() {
                "CONNECT" => out.push(Self::Connect),
                "CREATE" => out.push(Self::Create),
                "ALL" | "ALL PRIVILEGES" | "ALL PRIVILEGE" => {
                    return Some(vec![Self::Connect, Self::Create])
                }
                _ => return None,
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    pub fn held_by(self, held: u16) -> bool {
        held & self.bit() != 0
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "CONNECT",
            Self::Create => "CREATE",
        }
    }
}

/// Role attributes that gate administrative operations. Never inherited
/// through membership: only the role's own row confers them (superusers
/// additionally imply every attribute).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RoleAttribute {
    Superuser,
    CreateRole,
    CreateDatabase,
}

impl RoleAttribute {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Superuser => "SUPERUSER",
            Self::CreateRole => "CREATEROLE",
            Self::CreateDatabase => "CREATEDB",
        }
    }
}

/// Which object class a `GRANT`/`REVOKE` targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantObject {
    Table(String),
    Schema(String),
    Database(String),
    /// `ALL TABLES IN SCHEMA <schema>`: expand at execution time to every
    /// table currently in that schema (PostgreSQL does not make the grant
    /// apply to tables created later).
    AllTablesInSchema(String),
}

impl GrantObject {
    pub fn describe(&self) -> String {
        match self {
            Self::Table(name) => format!("TABLE {name}"),
            Self::Schema(name) => format!("SCHEMA {name}"),
            Self::Database(name) => format!("DATABASE {name}"),
            Self::AllTablesInSchema(schema) => format!("ALL TABLES IN SCHEMA {schema}"),
        }
    }
}
