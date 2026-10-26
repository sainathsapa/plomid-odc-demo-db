#!/usr/bin/env python3
"""Minimal repro: storage B+Tree internal split fails INSERT on mixed-size separators.

Fixture: a fresh table with a dense VECTOR(FLOAT32, 128) column. CREATE TABLE
auto-creates the HNSW index, so every row stages one index entry whose key is
the vector rendered as SQL text (~1.3 KB) alongside small PK index keys.

  4 batches x 1000 distinct clustered vectors, single autocommit statements:
    - batches 0..2 succeed
    - the batch around rows 3000-4000 fails with
        PL-INVALID-ARGUMENT: B+Tree node exceeds page payload

Root cause (measured with a temporarily instrumented build, reverted):
crates/storage/src/btree.rs splits internal nodes at a COUNT midpoint
(`let middle = separators.len() / 2`) while splits of leaves are byte-aware.
An internal node mixing large separators with small ones can pass the fit
check with 12 large + 15 small separators (15,880 + 360 bytes), and the next
large separator overflows the 16,332-byte payload. The count midpoint then
lands at 14 of 28: the left half keeps 13 large + 1 small separator =
17,234 bytes > 16,332 cap, and write_node rejects it.

Instrumented evidence (one run):
  page=39 total=28 middle=14 left_len=17234 right_len=328
  lens=[1312 x13, 14 x15]

Usage:
  python3 storage_split_repro.py [DSN]
  python3 storage_split_repro.py [DSN] --control   # drop auto index first
"""

import sys

import psycopg

DSN = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else (
    "postgresql://plomid:plomid@127.0.0.1:6100/plomid"
)
CONTROL = "--control" in sys.argv
DIM = 128
BATCHES = 4
ROWS_PER_BATCH = 1000


class Rng:
    """Deterministic LCG identical to tests/python/vector_lifecycle_bench.py."""

    def __init__(self, seed: int):
        self.state = seed & 0xFFFF_FFFF_FFFF_FFFF

    def next(self) -> float:
        self.state = (
            self.state * 6364136223846793005 + 1442695040888963407
        ) & 0xFFFF_FFFF_FFFF_FFFF
        return (self.state >> 33) / 4294967295.0 * 2.0 - 1.0


def rows_for(count: int, dim: int) -> list[str]:
    rng = Rng(0x51D ^ count)
    centers = [[round(rng.next(), 4) for _ in range(dim)] for _ in range(8)]
    out = []
    for i in range(count):
        center = centers[i % 8]
        lanes = [round(center[j] + rng.next() * 0.05, 6) for j in range(dim)]
        out.append(f"({i}, [{','.join(map(str, lanes))}])")
    return out


def main() -> int:
    rows = rows_for(BATCHES * ROWS_PER_BATCH, DIM)
    table = "split_repro"
    failures = 0
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(
                f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, {DIM}))"
            )
            if CONTROL:
                cur.execute(f"DROP INDEX {table}_e_vector_idx")
                print("control: auto index dropped before the load")
            for batch in range(BATCHES):
                statement = (
                    f"INSERT INTO {table} VALUES "
                    + ",".join(rows[batch * ROWS_PER_BATCH : (batch + 1) * ROWS_PER_BATCH])
                )
                try:
                    cur.execute(statement)
                    count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    print(f"batch {batch}: OK (rows={count})")
                except Exception as error:  # noqa: BLE001 - report the server error
                    failures += 1
                    print(f"batch {batch}: FAIL: {str(error).splitlines()[0]}")
                    break
    if CONTROL:
        return 0 if failures == 0 else 1
    return 0 if failures == 1 else 1


if __name__ == "__main__":
    sys.exit(main())
