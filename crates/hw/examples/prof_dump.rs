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

fn main() {
    let p = plomid_hw::HardwareProfile::detect();
    println!(
        "arch={} isa={} logical={} physical={:?}",
        p.arch,
        p.vector_isa(),
        p.logical_cpus,
        p.physical_cpus
    );
    println!(
        "cpu: scalar={} sse42={} avx2={} avx512={} neon={}",
        p.cpu.scalar, p.cpu.sse42, p.cpu.avx2, p.cpu.avx512, p.cpu.neon
    );
    println!("cache={:?}", p.cache_bytes);
    println!(
        "ram_total={:?} ram_avail={:?}",
        p.ram_total_bytes, p.ram_available_bytes
    );
    println!(
        "storage={:?} bw={:?} iops={:?} numa={:?}",
        p.storage, p.storage_bandwidth_bps, p.storage_iops_estimate, p.numa_nodes
    );
    println!(
        "workers={} batch_rows={} small={}",
        p.worker_budget(),
        p.batch_rows(),
        p.is_small_device()
    );
    for n in &p.notes {
        println!("note: {n}");
    }
}
