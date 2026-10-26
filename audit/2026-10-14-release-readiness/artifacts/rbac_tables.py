import psycopg
SUPER = "postgresql://plomid:plomid@127.0.0.1:55432/plomid"
LOW = "postgresql://lowpriv:x@127.0.0.1:55432/plomid"
def run(dsn, sql):
    conn = psycopg.connect(dsn, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            try: rows = cur.fetchall()
            except Exception: rows = None
        return ("OK", rows)
    except Exception as e:
        return ("ERR", f"{type(e).__name__}: {str(e).splitlines()[0][:120]}")
    finally: conn.close()
print("super create t:", run(SUPER, "DROP TABLE IF EXISTS rbac_t; CREATE TABLE rbac_t (id BIGINT PRIMARY KEY);")[0])
print("lowpriv SELECT no-grant:", run(LOW, "SELECT * FROM rbac_t;"))
print("lowpriv INSERT no-grant:", run(LOW, "INSERT INTO rbac_t VALUES (1);"))
print("super GRANT:", run(SUPER, "GRANT SELECT ON rbac_t TO lowpriv;")[0])
print("lowpriv SELECT after-grant:", run(LOW, "SELECT COUNT(*) FROM rbac_t;"))
print("lowpriv wrong-pw:", run("postgresql://lowpriv:WRONG@127.0.0.1:55432/plomid", "SELECT 1;"))
