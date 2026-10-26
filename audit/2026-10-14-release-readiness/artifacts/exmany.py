import psycopg, time
c = psycopg.connect("postgresql://plomid:plomid@127.0.0.1:55433/plomid", autocommit=True)
cur = c.cursor()
cur.execute("DROP TABLE IF EXISTS em; CREATE TABLE em (id BIGINT PRIMARY KEY, name TEXT, age INT);")
payload=[(i,f"val{i}",20+(i%50)) for i in range(2000)]
t0=time.time(); cur.executemany("INSERT INTO em (id,name,age) VALUES (%s,%s,%s)", payload)
print(f"executemany 2000x1-row: {time.time()-t0:.2f}s")
