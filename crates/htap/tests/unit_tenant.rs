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
// Moved from `src/tenant.rs` inline `#[cfg(test)]` — public-API only.
use plomid_htap::tenant::*;

#[test]
fn unknown_tenants_are_unlimited() {
    let registry = TenantRegistry::new();
    let tenant = TenantId::new("ghost");
    assert!(registry.check(&tenant, u64::MAX / 2, 1000).is_ok());
    assert_eq!(registry.usage(&tenant), (0, 0));
}

#[test]
fn quotas_enforce_budget_and_concurrency() {
    let mut registry = TenantRegistry::new();
    let tenant = TenantId::new("t1");
    registry.register(tenant.clone(), TenantQuota::new(100, 2));
    assert!(registry.check(&tenant, 150, 1).is_err());
    assert!(registry.check(&tenant, 60, 3).is_err());
    assert!(registry.check(&tenant, 60, 1).is_ok());
    registry.record(&tenant, 60, 1);
    assert_eq!(registry.usage(&tenant), (60, 1));
    assert!(registry.check(&tenant, 50, 1).is_err());
    registry.release(&tenant, 60, 1);
    assert_eq!(registry.usage(&tenant), (0, 0));
    assert!(registry.check(&tenant, 50, 1).is_ok());
}
