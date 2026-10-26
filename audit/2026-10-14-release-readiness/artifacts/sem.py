import psycopg
c = psycopg.connect("postgresql://plomid:plomid@127.0.0.1:55433/plomid", autocommit=True)
def run(sql):
    try:
        with c.cursor() as cur:
            cur.execute(sql)
            try: return ("OK", cur.fetchall())
            except Exception: return ("OK", None)
    except Exception as e:
        return ("ERR", str(e).splitlines()[0][:150])
tests = [
    ("ILIKE case-insensitive", "SELECT 'ABC' ILIKE 'abc';", ("OK", [(True,)])),
    ("LIKE case-sensitive", "SELECT 'ABC' LIKE 'abc';", ("OK", [(False,)])),
    ("FK declare", "DROP TABLE IF EXISTS fk_c; DROP TABLE IF EXISTS fk_p; CREATE TABLE fk_p (id BIGINT PRIMARY KEY); CREATE TABLE fk_c (id BIGINT PRIMARY KEY, pid BIGINT REFERENCES fk_p(id));", None),
    ("FLOAT typename", "DROP TABLE IF EXISTS ft; CREATE TABLE ft (x FLOAT);", None),
    ("CHECKPOINT stmt", "CHECKPOINT;", None),
    ("SHOW ALL rows>0", "SHOW ALL;", "ROWS"),
    ("null 3VL", "SELECT NULL AND TRUE, NULL OR FALSE, NOT NULL;", ("OK", [(None, False, None)])),
    ("count distinct nulls", "SELECT COUNT(DISTINCT x) FROM (SELECT 1 AS x UNION ALL SELECT NULL UNION ALL SELECT 1) t;", ("OK", [(1,)])),
    ("update returning", "DROP TABLE IF EXISTS ur; CREATE TABLE ur (id BIGINT PRIMARY KEY, v INT); INSERT INTO ur VALUES (1,1); UPDATE ur SET v=2 WHERE id=1 RETURNING v;", ("OK", [[(2,)] ] and None)),
    ("join null keys", "DROP TABLE IF EXISTS j1; DROP TABLE IF EXISTS j2; CREATE TABLE j1 (a INT, b INT); CREATE TABLE j2 (a INT, c INT); INSERT INTO j1 VALUES (1,10),(NULL,20),(1,30); INSERT INTO j2 VALUES (1,100),(NULL,200); SELECT COUNT(*) FROM j1 JOIN j2 ON j1.a=j2.a;", ("OK", [(2,)])),
    ("group having", "SELECT a, COUNT(*) FROM j1 GROUP BY a HAVING COUNT(*) > 1;", "ROWS"),
    ("limit offset", "SELECT a FROM j1 ORDER BY a NULLS LAST LIMIT 2 OFFSET 1;", "ROWS"),
    ("delete using?", "DELETE FROM j1 USING j2 WHERE j1.a = j2.a;", None),
    ("insert select", "DROP TABLE IF EXISTS isel; CREATE TABLE isel AS SELECT * FROM j1;", None),
]
for name, sql, want in tests:
    st, got = run(sql)
    if want is None:
        print(f"[INFO] {name}: {st} {got if st=='ERR' else 'ok'}")
    elif want == "ROWS":
        print(f"[INFO] {name}: {st} rows={got}")
    else:
        print(f"[{'PASS' if (st,got)==want else 'FAIL'}] {name}: got {(st,got)} want {want}")
