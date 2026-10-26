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

//! Live-path test harness: temp engine + Executor::execute_all choke point.
use plomid_executor::Executor;
use plomid_txn::PlomidStorageEngine;
use std::path::PathBuf;
use std::time::Instant;

pub struct LiveDb {
    pub executor: Executor<PlomidStorageEngine>,
    pub storage_dir: PathBuf,
    pub wal_dir: PathBuf,
}

pub fn open_live_db(tag: &str) -> LiveDb {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let storage_dir = std::env::temp_dir().join(format!("plomid-gauntlet-{tag}-{pid}-{nanos}"));
    let wal_dir = std::env::temp_dir().join(format!("plomid-gauntlet-{tag}-wal-{pid}-{nanos}"));
    let _ = std::fs::remove_dir_all(&storage_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let engine = PlomidStorageEngine::create(&storage_dir, &wal_dir, 32)
        .expect("gauntlet engine create must succeed");
    let executor = Executor::new(engine).expect("gauntlet executor open must succeed");
    LiveDb {
        executor,
        storage_dir,
        wal_dir,
    }
}

impl LiveDb {
    pub fn exec(&mut self, sql: &str) {
        self.executor
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql} failed: {e}"));
    }

    pub fn timed_exec(&mut self, sql: &str) -> f64 {
        let t = Instant::now();
        self.exec(sql);
        t.elapsed().as_secs_f64() * 1000.0
    }

    pub fn teardown(self) {
        drop(self.executor);
        let _ = std::fs::remove_dir_all(&self.storage_dir);
        let _ = std::fs::remove_dir_all(&self.wal_dir);
    }
}
