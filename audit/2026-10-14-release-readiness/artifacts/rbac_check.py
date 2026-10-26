import psycopg
SUPER = "postgresql://plomid:plomid@127.0.0.1:55432/plomid"
LOW = "postgresql://lowpriv:x@127.0.0.1:55432/plomid"

def run(dsn, sql):
    """Returns ('OK', None) or ('ERR', message). Never swallows server errors."""
    conn = psycopg.connect(dsn, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            try:
                rows = cur.fetchall()
            except Exception:
                rows = None
        return ("OK", rows)
    except Exception as e:
        return ("ERR", f"{type(e).__name__}: {e}".splitlines()[0][:160])
    finally:
        conn.close()

def dbs():
    st, rows = run(SUPER, "SHOW DATABASES;")
    return sorted(r[0] for r in rows) if st == "OK" else st

print("before:", dbs())
print("lowpriv CREATE DATABASE rbac_evil:", run(LOW, "CREATE DATABASE rbac_evil;")[0], run(LOW, "CREATE DATABASE rbac_evil;")[1] if False else "")
st, msg = run(LOW, "CREATE DATABASE rbac_evil;")
print("lowpriv CREATE DATABASE rbac_evil ->", st, msg)
print("after create attempt:", dbs())
st, msg = run(LOW, "DROP DATABASE rbac_evil;")
print("lowpriv DROP DATABASE rbac_evil ->", st, msg)
print("after drop attempt:", dbs())
st, msg = run(SUPER, "DROP DATABASE rbac_evil;")
print("superuser DROP DATABASE rbac_evil ->", st, msg)
print("final:", dbs())
