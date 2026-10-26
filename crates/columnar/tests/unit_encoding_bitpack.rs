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
// Moved from src/encoding/bitpack.rs inline #[cfg(test)].
use plomid_columnar::encoding::bitpack::{packed_len, width_for_max, BitReader, BitWriter};
use plomid_core::ErrorKind;

#[test]
fn packs_and_unpacks_lsb_first() {
    let mut writer = BitWriter::new();
    writer.write(0b1, 1);
    writer.write(0b10, 2);
    writer.write(0x1FF, 9);
    let bytes = writer.finish();
    // bit0 = 1; bits1..3 = 0b10; bits3..12 = 0b1_1111_1111
    assert_eq!(bytes.len(), 2);
    assert_eq!(bytes[0], 0b1111_1101);
    assert_eq!(bytes[1], 0b0000_1111);
    let mut reader = BitReader::new(&bytes);
    assert_eq!(reader.read(1).expect("bit"), 0b1);
    assert_eq!(reader.read(2).expect("bits"), 0b10);
    assert_eq!(reader.read(9).expect("bits"), 0x1FF);
    assert_eq!(reader.consumed(), bytes.len());
}

#[test]
fn round_trips_every_width() {
    for width in 0..=64_u32 {
        let limit = if width == 0 {
            0
        } else {
            (1_u64 << width.min(63)) - 1
        };
        let values = [0_u64, 1, limit, limit / 3];
        let mut writer = BitWriter::new();
        for value in values {
            writer.write(value, width);
        }
        let bytes = writer.finish();
        assert_eq!(bytes.len(), packed_len(values.len() * width as usize));
        let mut reader = BitReader::new(&bytes);
        for value in values {
            assert_eq!(reader.read(width).expect("read") & limit, value & limit);
        }
        assert_eq!(reader.consumed(), bytes.len());
    }
}

#[test]
fn zero_width_writes_nothing() {
    let mut writer = BitWriter::new();
    writer.write(u64::MAX, 0);
    assert!(writer.finish().is_empty());
}

#[test]
fn reading_past_the_end_is_corruption() {
    let mut reader = BitReader::new(&[0xFF, 0x00]);
    assert_eq!(reader.read(8).expect("first byte"), 0xFF);
    assert_eq!(reader.read(8).expect("second byte"), 0x00);
    let error = reader.read(1).expect_err("truncated stream");
    assert_eq!(error.kind(), ErrorKind::Corruption);
    assert!(BitReader::new(&[]).read(1).is_err());
}

#[test]
fn width_for_max_reports_minimal_bits() {
    assert_eq!(width_for_max(0), 0);
    assert_eq!(width_for_max(1), 1);
    assert_eq!(width_for_max(2), 2);
    assert_eq!(width_for_max(3), 2);
    assert_eq!(width_for_max(u64::MAX), 64);
}
