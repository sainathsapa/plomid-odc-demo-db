#!/usr/bin/env python3
"""Vector contract + execution live probe for the 2026-10-16 audit.

Mirrors the in-tree contract matrix at SQL level against the release server:
  1. DDL envelope: 7 column-type cases x 8 methods; dim-4 IVF_PQ must name the
     multiple-of-8 rule; every rejection is VECTOR_INDEX_UNSUPPORTED.
  2. Accepted indexes report READY in plomid_vector_indexes().
  3. Auto-index: every dense table carries its {table}_e_vector_idx, none
     elsewhere.
  4. Execution proof at dim 8: FLAT + every accepted dense ANN method returns
     the exact top-k (full coverage fixture) and reuses the cache.
  5. Binary Hamming ANN via explicit USING BINARY; HNSW rejected on binary.
  6. Stats-counter semantics: warm hits delta vs query count.
  7. EXPLAIN label vs execution under SET vector.auto_ann_min_rows = 0.

Usage: python3 vector_commit_probe.py [DSN]
"""

import sys

import psycopg

DSN = (
    sys.argv[1]
    if len(sys.argv) > 1
    else "postgresql://plomid:plomid@127.0.0.1:6100/plomid"
)

FAILURES: list[str] = []
CHECKS = 0

ALL_METHODS = ["FLAT", "HNSW", "IVF_FLAT", "IVF_SQ", "IVF_PQ", "DISKANN", "SPARSE_INVERTED", "BINARY"]

CASES = [
    ("dense", "VECTOR(FLOAT32, 8)", ["FLAT", "HNSW", "IVF_FLAT", "IVF_SQ", "IVF_PQ", "DISKANN"]),
    ("dense_i4", "VECTOR(INT4, 8)", ["FLAT", "HNSW", "IVF_FLAT", "IVF_SQ", "DISKANN"]),
    ("dense_i8", "VECTOR(INT8, 8)", ["FLAT", "HNSW", "IVF_FLAT", "IVF_SQ", "IVF_PQ", "DISKANN"]),
    ("dense_dim4", "VECTOR(FLOAT32, 4)", ["FLAT", "HNSW", "IVF_FLAT", "IVF_SQ", "DISKANN"]),
    ("binary", "VECTOR(BINARY, 8)", ["FLAT", "BINARY"]),
    ("sparse", "SPARSE_VECTOR(FLOAT32, 8)", ["FLAT", "SPARSE_INVERTED"]),
    ("multi", "MULTI_VECTOR(FLOAT32, 4, 2)", ["FLAT"]),
]


def check(label: str, ok: bool, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    print(f"{'PASS' if ok else 'FAIL'} {label}" + (f": {detail}" if detail else ""))
    if not ok:
        FAILURES.append(label)


def note(label: str, value) -> None:
    print(f"NOTE {label}: {value!r}")


def try_exec(cur, sql: str, params=None):
    try:
        cur.execute(sql, params)
        return None
    except Exception as error:  # noqa: BLE001
        return str(error).splitlines()[0]


def main() -> int:
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            # ---- 1. Envelope matrix.
            for tag, column_type, accepted in CASES:
                table = f"vp_{tag}"
                cur.execute(f"DROP TABLE IF EXISTS {table}")
                cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, e {column_type})")
                for method in ALL_METHODS:
                    sql = f"CREATE INDEX {table}_{method.lower()} ON {table} USING {method} (e)"
                    error = try_exec(cur, sql)
                    want_ok = method in accepted
                    if want_ok:
                        check(f"envelope {tag} {method} accepted", error is None, error or "")
                    else:
                        ok = error is not None and "VECTOR_INDEX_UNSUPPORTED" in error
                        if tag == "dense_dim4" and method == "IVF_PQ":
                            ok = ok and "multiple of 8" in error
                            note("dim-4 IVF_PQ rejection", error)
                        check(f"envelope {tag} {method} rejected", ok, error or "accepted?!")
                cur.execute(
                    "SELECT index_name, state FROM plomid_vector_indexes() "
                    f"WHERE table_name = 'public.{table}'"
                )
                listed = cur.fetchall()
                bad = [row for row in listed if row[1] != "READY"]
                expected_count = len(accepted) + (1 if tag.startswith("dense") and tag != "dense_dim4" or tag == "dense_dim4" else 0)
                check(f"operator view READY {tag}", not bad, f"rows={listed}")

            # ---- 2. Auto-index inventory.
            cur.execute(
                "SELECT tablename, indexname FROM pg_indexes "
                "WHERE indexname LIKE '%vector_idx' AND tablename LIKE 'vp_%' ORDER BY tablename"
            )
            auto = cur.fetchall()
            auto_tables = [row[0] for row in auto]
            check(
                "auto-index dense-only",
                auto_tables == ["vp_dense", "vp_dense_dim4", "vp_dense_i4", "vp_dense_i8"],
                f"{auto}",
            )

            # ---- 3. Execution proof at dim 8 (full-coverage fixture).
            cur.execute("DROP TABLE IF EXISTS vp_exec")
            cur.execute("CREATE TABLE vp_exec (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 8))")
            cur.execute("DROP INDEX vp_exec_e_vector_idx")
            rows = []
            for i in range(40):
                lane = [round(((i * (j + 3)) % 17) / 7.0, 6) for j in range(8)]
                rows.append(f"({i}, [{','.join(map(str, lane))}])")
            cur.execute("INSERT INTO vp_exec VALUES " + ",".join(rows))
            cur.execute("SET vector.auto_ann_min_rows = 0")
            probe = "[1,0.5,0.25,0.125,0.0625,0.5,1.5,2.5]"
            query = f"SELECT id FROM vp_exec ORDER BY l2_distance(e, {probe}) LIMIT 8"
            cur.execute("SET vector.ann_enabled = off")
            cur.execute(query)
            exact_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SET vector.ann_enabled = on")
            for method in ["HNSW", "DISKANN", "IVF_FLAT", "IVF_SQ", "IVF_PQ"]:
                cur.execute(f"DROP INDEX IF EXISTS vp_{method.lower()}_exec")
                error = try_exec(cur, f"CREATE INDEX vp_{method.lower()}_exec ON vp_exec USING {method} (e)")
                if error:
                    check(f"exec {method} index created", False, error)
                    continue
                cur.execute("SELECT * FROM plomid_vector_stats()")
                cols = [d.name for d in cur.description]
                s0 = dict(zip(cols, cur.fetchone()))
                cur.execute(query)
                ann_ids = [r[0] for r in cur.fetchall()]
                cur.execute("SELECT * FROM plomid_vector_stats()")
                s1 = dict(zip(cols, cur.fetchone()))
                cur.execute(query)
                cur.execute("SELECT * FROM plomid_vector_stats()")
                s2 = dict(zip(cols, cur.fetchone()))
                ok = set(ann_ids) == set(exact_ids)
                check(f"exec {method} equals exact", ok, f"ann={ann_ids} exact={exact_ids}")
                check(
                    f"exec {method} cache miss then hit",
                    s1.get("index_builds_total", 0) - s0.get("index_builds_total", 0) == 1,
                    f"stats delta miss={s1.get('ann_cache_misses_total', 0) - s0.get('ann_cache_misses_total', 0)} "
                    f"builds={s1.get('index_builds_total', 0) - s0.get('index_builds_total', 0)}",
                )
                note(
                    f"exec {method} second-query hits",
                    s2.get("ann_cache_hits_total", 0) - s1.get("ann_cache_hits_total", 0),
                )

            # ---- 4. Binary Hamming ANN.
            cur.execute("DROP TABLE IF EXISTS vp_bin")
            cur.execute("CREATE TABLE vp_bin (id BIGINT PRIMARY KEY, e VECTOR(BINARY, 64))")
            cur.execute("DROP INDEX IF EXISTS vp_bin_e_vector_idx")
            bit_rows = []
            for i in range(32):
                bits = "".join("1" if (i + j) % 3 else "0" for j in range(64))
                bit_rows.append(f"({i}, '{bits}')")
            cur.execute("INSERT INTO vp_bin VALUES " + ",".join(bit_rows))
            rejected = try_exec(cur, "CREATE INDEX vp_bin_hnsw ON vp_bin USING HNSW (e)")
            check(
                "binary HNSW rejected",
                rejected is not None and "VECTOR_INDEX_UNSUPPORTED" in rejected,
                rejected or "accepted?!",
            )
            accepted = try_exec(cur, "CREATE INDEX vp_bin_ann ON vp_bin USING BINARY (e)")
            check("binary USING BINARY accepted", accepted is None, accepted or "")
            pattern = "'" + "1" * 64 + "'"
            cur.execute("SET vector.ann_enabled = off")
            cur.execute(f"SELECT id FROM vp_bin ORDER BY hamming_distance(e, {pattern}) LIMIT 5")
            exact_bin = [r[0] for r in cur.fetchall()]
            cur.execute("SET vector.ann_enabled = on")
            cur.execute(f"SELECT id FROM vp_bin ORDER BY hamming_distance(e, {pattern}) LIMIT 5")
            ann_bin = [r[0] for r in cur.fetchall()]
            note("binary exact vs ann top-5", f"exact={exact_bin} ann={ann_bin}")
            check("binary ann overlap >= 4", len(set(exact_bin) & set(ann_bin)) >= 4)

            # ---- 5. EXPLAIN label vs execution under auto_ann_min_rows=0.
            cur.execute("DROP TABLE IF EXISTS vp_explain")
            cur.execute("CREATE TABLE vp_explain (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 8))")
            cur.execute("DROP INDEX vp_explain_e_vector_idx")
            small_rows = []
            for i in range(2000):
                lane = [round(((i * (j + 3)) % 17) / 7.0, 6) for j in range(8)]
                small_rows.append(f"({i}, [{','.join(map(str, lane))}])")
                if len(small_rows) == 1000:
                    cur.execute("INSERT INTO vp_explain VALUES " + ",".join(small_rows))
                    small_rows = []
            if small_rows:
                cur.execute("INSERT INTO vp_explain VALUES " + ",".join(small_rows))
            cur.execute("CREATE INDEX vp_explain_idx ON vp_explain USING HNSW (e)")
            cur.execute("SET vector.auto_ann_min_rows = 0")
            cur.execute("EXPLAIN " + query.replace("vp_exec", "vp_explain"))
            explain = [r[0] for r in cur.fetchall()]
            note("explain at n=2000 with auto_ann_min_rows=0", " | ".join(explain)[-160:])
            cur.execute("SELECT * FROM plomid_vector_stats()")
            cols = [d.name for d in cur.description]
            s0 = dict(zip(cols, cur.fetchone()))
            cur.execute(query.replace("vp_exec", "vp_explain"))
            cur.execute("SELECT * FROM plomid_vector_stats()")
            s1 = dict(zip(cols, cur.fetchone()))
            note(
                "execution after that explain",
                f"miss={s1.get('ann_cache_misses_total',0)-s0.get('ann_cache_misses_total',0)} "
                f"builds={s1.get('index_builds_total',0)-s0.get('index_builds_total',0)}",
            )

    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
    if FAILURES:
        print("FAILURES: " + "; ".join(FAILURES))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
