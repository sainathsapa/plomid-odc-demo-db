import psycopg, time, random
c = psycopg.connect("postgresql://plomid:plomid@127.0.0.1:55433/plomid", autocommit=True)
cur = c.cursor()
cur.execute("DROP TABLE IF EXISTS bulk; CREATE TABLE bulk (id BIGINT PRIMARY KEY, g INT, v DOUBLE PRECISION, t TEXT);")
rng = random.Random(7)
N = 100000
rows = [(i, i % 97, (i * 13) % 1000 / 7.0, ("txt%03d" % (i % 211)) if i % 11 else None) for i in range(N)]
t0 = time.time()
B = 2000
for s in range(0, N, B):
    cur.execute("INSERT INTO bulk VALUES " + ",".join(
        "(%d,%d,%f,%s)" % (i, g, v, ("'%s'" % t) if t is not None else "NULL") for (i, g, v, t) in rows[s:s+B]) + ";")
t1 = time.time()
print(f"insert {N} rows: {t1-t0:.2f}s ({N/(t1-t0):,.0f} rows/s)")
for sql, oracle in [
    ("SELECT COUNT(*) FROM bulk;", N),
    ("SELECT SUM(id) FROM bulk;", N*(N-1)//2),
    ("SELECT COUNT(*) FROM bulk WHERE g = 5;", sum(1 for r in rows if r[1]==5)),
    ("SELECT COUNT(DISTINCT g) FROM bulk;", 97),
]:
    t0=time.time(); got = cur.execute(sql).fetchall()[0][0]; dt=time.time()-t0
    print(f"{'PASS' if got==oracle else 'FAIL'} {sql} -> {got} (want {oracle}) [{dt*1000:.0f}ms]")
t0=time.time()
gb = cur.execute("SELECT g, COUNT(*), SUM(v) FROM bulk GROUP BY g ORDER BY g;").fetchall(); dt=time.time()-t0
from collections import defaultdict
exp = defaultdict(lambda: [0,0.0])
for (i,g,v,t) in rows: exp[g][0]+=1; exp[g][1]+=v
ok = len(gb)==97 and all(c==exp[g][0] and abs(s-exp[g][1])<1e-6 for (g,c,s) in gb)
print(f"{'PASS' if ok else 'FAIL'} group-by 97 groups exact [{dt*1000:.0f}ms]")
