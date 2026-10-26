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
//! Device registry: which physical devices exist and are they usable.
//!
//! The registry resolves [`DeviceId`] to a [`DeviceEntry`] describing where the
//! device lives on the filesystem and the device record that identifies it. It
//! *discovers* devices from the layout directory tree and validates their
//! records; it never chooses which device data is placed on (that is the
//! allocator's job) and it never reads or writes physical pages.
//!
//! ```text
//! DeviceRegistry
//!     ├── discover()       enumerate and validate all registered devices
//!     ├── get(id)          resolve one device by identity
//!     └── entries()        all known devices
//! ```

use crate::device::metadata::{read_device_record, DeviceRecord};
use crate::device::physical::Lifecycle;
use crate::device::state::{read_device_state_optional, DeviceStateRecord};
use crate::layout::DatabaseLayout;
use plomid_core::{DeviceId, ErrorKind, PlomidError, Result};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// A device known to the registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceEntry {
    /// Logical device identity.
    pub device_id: DeviceId,
    /// Path of the device's container directory (`devices/D-*`).
    pub dir: PathBuf,
    /// Path of the device's physical container file (`devices/D-*/packs/*`).
    pub physical_file: PathBuf,
    /// Durable identity and capacity record (`devices/D-*/DEVICE.dat`).
    pub record: DeviceRecord,
    /// Last durable lifecycle state, if one was recorded.
    pub state: Option<DeviceStateRecord>,
}

impl DeviceEntry {
    /// Returns true when the device is currently online or recovering.
    pub fn is_usable(&self) -> bool {
        match self.state {
            Some(s) => matches!(s.state, Lifecycle::Online | Lifecycle::Recovering),
            // No state record: the physical header is authoritative.
            None => true,
        }
    }

    /// Returns the device's configured capacity in bytes.
    pub fn capacity(&self) -> u64 {
        self.record.capacity
    }
}

/// The set of physical devices known to a database.
#[derive(Clone, Debug, Default)]
pub struct DeviceRegistry {
    devices: BTreeMap<DeviceId, DeviceEntry>,
}

impl DeviceRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns an iterator over all known device entries, in identity order.
    pub fn iter(&self) -> impl Iterator<Item = &DeviceEntry> {
        self.devices.values()
    }

    /// Returns the number of registered devices.
    #[must_use]
    pub fn len(&self) -> usize {
        self.devices.len()
    }

    /// Returns true when no devices are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.devices.is_empty()
    }

    /// Returns the entry for `device_id`, if registered.
    pub fn get(&self, device_id: DeviceId) -> Option<&DeviceEntry> {
        self.devices.get(&device_id)
    }

    /// Returns true if a device with `device_id` is registered.
    #[must_use]
    pub fn contains(&self, device_id: DeviceId) -> bool {
        self.devices.contains_key(&device_id)
    }

    /// Total capacity across all registered devices.
    #[must_use]
    pub fn total_capacity(&self) -> u64 {
        self.devices.values().map(|e| e.capacity()).sum()
    }

    /// Total capacity across registered devices whose state is usable.
    #[must_use]
    pub fn usable_capacity(&self) -> u64 {
        self.devices
            .values()
            .filter(|e| e.is_usable())
            .map(|e| e.capacity())
            .sum()
    }

    /// Lowest device identity that is not registered yet.
    ///
    /// Device identities are logical and must be stable, so a database that
    /// registers a device for itself takes the lowest unused identity instead of
    /// assuming any particular device number exists or is free.
    #[must_use]
    pub fn next_free_id(&self) -> DeviceId {
        let mut candidate = 1_u64;
        for device_id in self.devices.keys() {
            if device_id.get() == candidate {
                candidate = candidate.saturating_add(1);
            }
        }
        DeviceId::new(candidate)
    }

    /// Discovers and validates every device directory the layout exposes.
    ///
    /// Discovery reads only directory listings and the small metadata records
    /// that identify a device: it never scans `packs/`, `free/`, or physical
    /// page content.
    ///
    /// A device directory that is missing its record, or whose record is
    /// corrupt or describes a different identity, is a controlled error: the
    /// registry never silently ignores durable physical capacity, because doing
    /// so would make the database appear smaller than it really is.
    pub fn discover(layout: &DatabaseLayout) -> Result<Self> {
        let ids = layout.discover_device_ids()?;
        let mut devices = BTreeMap::new();
        for device_id in ids {
            let dir = layout.device_dir(device_id);
            let record_path = layout.device_meta_path(device_id);
            let record = read_device_record(&record_path).map_err(|error| {
                PlomidError::with_detail(
                    ErrorKind::Corruption,
                    "device record could not be read",
                    format!(
                        "device_id={} path={} error={}",
                        device_id.get(),
                        record_path.display(),
                        error.message()
                    ),
                )
            })?;
            // The identity in the directory name must match the identity in
            // the record. This is the core invariant that prevents a renamed or
            // swapped directory from being mistaken for a different device.
            if record.device_id != device_id {
                return Err(PlomidError::with_detail(
                    ErrorKind::Corruption,
                    "device identity in DEVICE.dat does not match its directory",
                    format!(
                        "device_id={} on_disk={}",
                        device_id.get(),
                        record.device_id.get(),
                    ),
                ));
            }
            let physical_file = layout.device_physical_path(device_id);
            let state_path = layout.device_state_path(device_id);
            let state = read_device_state_optional(&state_path).ok().flatten();
            devices.insert(
                device_id,
                DeviceEntry {
                    device_id,
                    dir,
                    physical_file,
                    record,
                    state,
                },
            );
        }
        Ok(Self { devices })
    }

    /// Takes ownership of a device entry out of the registry.
    ///
    /// Returns `None` if the device is not present.
    pub fn take(&mut self, device_id: DeviceId) -> Option<DeviceEntry> {
        self.devices.remove(&device_id)
    }
}

impl<'a> IntoIterator for &'a DeviceRegistry {
    type Item = &'a DeviceEntry;
    type IntoIter = std::collections::btree_map::Values<'a, DeviceId, DeviceEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.devices.values()
    }
}
