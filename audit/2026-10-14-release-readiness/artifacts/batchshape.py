import psycopg, time
c = psycopg.connect("postgresql://plomid:plomid@127.0.0.1:55433/plomid", autocommit=True)
cur = c.cursor()
for B in (100, 500, 2000):
    cur.execute(f"DROP TABLE IF EXISTS bs; CREATE TABLE bs (id BIGINT PRIMARY KEY, v INT);")
    vals = ",".join(f"({i},{i})" for i in range(2000))
    t0=time.time(); n=0
    for s in range(0, 2000, B):
        chunk = ",".join(f"({i},{i})" for i in range(s, min(s+B,2000)))
        cur.execute(f"INSERT INTO bs VALUES {chunk};"); n+=1
    dt=time.time()-t0
    print(f"batch={B} stmts={n} total={dt:.2f}s per-stmt={dt/n*1000:.0f}ms")
