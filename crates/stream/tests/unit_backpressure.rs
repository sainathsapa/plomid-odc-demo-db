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
// Moved from `src/backpressure.rs` inline `#[cfg(test)]` — public-API only.
use plomid_stream::backpressure::*;

#[test]
fn peak_never_exceeds_capacity_through_hooks() {
    let controller = BackpressureController::new(2);
    controller.notify_send(0);
    controller.notify_send(0);
    assert_eq!(controller.buffered_batches(), 2);
    assert_eq!(controller.peak_buffered_batches(), 2);
    controller.notify_receive();
    assert_eq!(controller.buffered_batches(), 1);
}

#[test]
fn receive_without_send_saturates_at_zero() {
    let controller = BackpressureController::new(2);
    controller.notify_receive();
    assert_eq!(controller.buffered_batches(), 0);
}
