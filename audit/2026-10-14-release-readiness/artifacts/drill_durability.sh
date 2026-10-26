#!/bin/bash
# Durability / crash / backup drill, 2026-10-14 audit. All dirs disposable.
set -u
A=/tmp/plomid-audit-20261014
BIN=/Users/sainathsapa/dsk/PloMID/plomid/target/debug/plomid-server
PSQL="psql postgresql://plomid:plomid@127.0.0.1:55432/plomid"
log() { echo "[drill] $*"; }

$PSQL -c "DROP TABLE IF EXISTS dur_grace; CREATE TABLE dur_grace (id BIGINT PRIMARY KEY, v TEXT); INSERT INTO dur_grace VALUES (1, 'committed-before-graceful-stop');" | head -1

SRV=$(pgrep -f "plomid-server --data $A/data1" | head -1)
log "stopping server pid $SRV gracefully (SIGTERM)"
kill -TERM "$SRV"
for i in $(seq 1 30); do pgrep -f "plomid-server --data $A/data1" >/dev/null || break; sleep 1; done
pgrep -f "plomid-server --data $A/data1" >/dev/null && { log "server did not stop"; exit 1; }
log "restarting"
($BIN --data $A/data1 --host 127.0.0.1 --port 55432 --username plomid --password plomid >$A/server2.log 2>&1 &) 
sleep 4
$PSQL -tAc "SELECT v FROM dur_grace WHERE id = 1;"
log "graceful-restart sentinel check above (expect committed-before-graceful-stop)"

$PSQL -c "DROP TABLE IF EXISTS dur_crash; CREATE TABLE dur_crash (id BIGINT PRIMARY KEY, v TEXT); INSERT INTO dur_crash VALUES (1, 'committed-before-kill9');" | head -1
python3 - "$A" <<'EOF'
import psycopg, sys
c = psycopg.connect("postgresql://plomid:plomid@127.0.0.1:55432/plomid")
c.execute("BEGIN; INSERT INTO dur_crash VALUES (2, 'never-committed');")
print("open txn staged row 2, leaving uncommitted (connection held, no commit)")
import time; time.sleep(60)
EOF
echo "background uncommitted-txn holder started" &
sleep 2
SRV=$(pgrep -f "plomid-server --data $A/data1" | head -1)
log "SIGKILL server pid $SRV with open txn outstanding"
kill -KILL "$SRV"
sleep 2
wait 2>/dev/null
log "restarting after kill -9"
($BIN --data $A/data1 --host 127.0.0.1 --port 55432 --username plomid --password plomid >$A/server3.log 2>&1 &)
sleep 5
log "dur_crash contents (expect id=1 present, id=2 absent):"
$PSQL -c "SELECT * FROM dur_crash ORDER BY id;"
log "p_vec count after crash (expect 201: 200 + zero-vector row):"
$PSQL -tAc "SELECT COUNT(*) FROM p_vec;"
