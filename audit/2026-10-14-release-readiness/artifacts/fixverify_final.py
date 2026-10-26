import psycopg, subprocess
S = "postgresql://plomid:plomid@127.0.0.1:55478/plomid"
ok = True
def check(name, cond, detail=""):
    global ok
    ok = ok and cond
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
def run(dsn, sql):
    c = psycopg.connect(dsn, autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute(sql)
            try: return ("OK", cur.fetchall(), None)
            except Exception: return ("OK", None, None)
    except psycopg.Error as e:
        return ("ERR", str(e).splitlines()[0][:110], e.sqlstate)
    finally: c.close()
run(S, "CREATE ROLE lp2 WITH LOGIN PASSWORD 'x';")
st, _, _ = run("postgresql://lp2:x@127.0.0.1:55478/plomid", "CREATE DATABASE e9;")
check("P0 lowpriv CREATE denied", st == "ERR", st)
run(S, "CREATE DATABASE v9;")
st, _, _ = run("postgresql://lp2:x@127.0.0.1:55478/plomid", "DROP DATABASE v9;")
check("P0 lowpriv DROP denied", st == "ERR", st)
st, m, code = run(S, "CREATE DATABASE e9;")
run(S, "CREATE DATABASE e9x;")
st, m, code = run(S, "CREATE DATABASE e9x;")
check("P1 duplicate -> 42P04", code == "42P04", code)
st, m, code = run(S, "DROP DATABASE nosuch9;")
check("P1 missing -> 3D000", code == "3D000", code)
st, m, code = run(S, "DROP DATABASE plomid;")
check("P1 default DROP -> 42501", code == "42501", code)
st, m, code = run("postgresql://lp2:x@127.0.0.1:55478/plomid", "CREATE DATABASE e10;")
check("P0 lowpriv denial code 42501", code == "42501", code)
for sql, want in [("SELECT 'ABC' ILIKE 'abc';", True), ("SELECT 'ABC' LIKE 'abc';", False),
    ("SELECT 'AbC' ILIKE 'a%c';", True), ("SELECT 'ABC' NOT ILIKE 'xyz';", True),
    ("SELECT 'ÄPFEL' ILIKE 'äpfel';", True)]:
    st, rows, _ = run(S, sql)
    check(f"P1 {sql[8:34]}", st == "OK" and rows[0][0] == want, rows)
print("ALL PASS" if ok else "SOME FAILED")
