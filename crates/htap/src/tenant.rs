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

//! Minimal multi-tenant hooks: per-tenant budgets and concurrency.
//!
//! The default is single-tenant unlimited: unknown tenants are admitted
//! without quota checks. Registering a [`TenantQuota`] opts that tenant
//! into hard budget / concurrency enforcement inside
//! [`ResourceGovernor::admit_for_tenant`](crate::governor::ResourceGovernor::admit_for_tenant).

use std::collections::HashMap;

/// Tenant identity (opaque string).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TenantId(pub String);

impl TenantId {
    /// New tenant id.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The implicit single tenant used when tenancy is not configured.
    pub fn default_tenant() -> Self {
        Self("default".to_string())
    }

    /// Borrow the raw id.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for TenantId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

/// Per-tenant hard limits. `None` = unlimited on that axis. Fully
/// configurable per tenant; default is unlimited on both axes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantQuota {
    /// Max bytes this tenant may hold across pools. `None` = unlimited.
    pub budget_bytes: Option<u64>,
    /// Max concurrent workers for this tenant. `None` = unlimited.
    pub max_concurrency: Option<usize>,
}

impl Default for TenantQuota {
    fn default() -> Self {
        Self::unlimited()
    }
}

impl TenantQuota {
    /// No limits.
    pub fn unlimited() -> Self {
        Self {
            budget_bytes: None,
            max_concurrency: None,
        }
    }

    /// Limits on both axes.
    pub fn new(budget_bytes: u64, max_concurrency: usize) -> Self {
        Self {
            budget_bytes: Some(budget_bytes),
            max_concurrency: Some(max_concurrency),
        }
    }
}

/// Registry of tenant quotas plus live per-tenant usage.
#[derive(Debug, Default)]
pub struct TenantRegistry {
    quotas: HashMap<TenantId, TenantQuota>,
    used_bytes: HashMap<TenantId, u64>,
    used_slots: HashMap<TenantId, usize>,
}

impl TenantRegistry {
    /// Empty registry: every tenant is unlimited.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or overwrite) a tenant quota.
    pub fn register(&mut self, tenant: TenantId, quota: TenantQuota) {
        self.quotas.insert(tenant, quota);
    }

    /// Remove a tenant's quota and usage.
    pub fn remove(&mut self, tenant: &TenantId) {
        self.quotas.remove(tenant);
        self.used_bytes.remove(tenant);
        self.used_slots.remove(tenant);
    }

    /// Effective quota: the registered one, or unlimited when unknown.
    pub fn quota_for(&self, tenant: &TenantId) -> TenantQuota {
        self.quotas
            .get(tenant)
            .cloned()
            .unwrap_or_else(TenantQuota::unlimited)
    }

    /// Current `(bytes, slots)` usage for a tenant.
    pub fn usage(&self, tenant: &TenantId) -> (u64, usize) {
        (
            self.used_bytes.get(tenant).copied().unwrap_or(0),
            self.used_slots.get(tenant).copied().unwrap_or(0),
        )
    }

    /// Check a request against the tenant quota. Unknown tenants pass.
    pub fn check(&self, tenant: &TenantId, bytes: u64, workers: usize) -> Result<(), String> {
        let quota = self.quota_for(tenant);
        let (used_bytes, used_slots) = self.usage(tenant);
        if let Some(budget) = quota.budget_bytes {
            if used_bytes.saturating_add(bytes) > budget {
                return Err(format!(
                    "tenant '{tenant}' budget exhausted: {used_bytes}+{bytes} > {budget}"
                ));
            }
        }
        if let Some(cap) = quota.max_concurrency {
            if used_slots + workers > cap {
                return Err(format!(
                    "tenant '{tenant}' concurrency exhausted: {used_slots}+{workers} > {cap}"
                ));
            }
        }
        Ok(())
    }

    /// Record an admitted request.
    pub fn record(&mut self, tenant: &TenantId, bytes: u64, workers: usize) {
        *self.used_bytes.entry(tenant.clone()).or_insert(0) += bytes;
        *self.used_slots.entry(tenant.clone()).or_insert(0) += workers;
    }

    /// Return resources. Saturates at zero.
    pub fn release(&mut self, tenant: &TenantId, bytes: u64, workers: usize) {
        if let Some(used) = self.used_bytes.get_mut(tenant) {
            *used = used.saturating_sub(bytes);
        }
        if let Some(slots) = self.used_slots.get_mut(tenant) {
            *slots = slots.saturating_sub(workers);
        }
    }
}
