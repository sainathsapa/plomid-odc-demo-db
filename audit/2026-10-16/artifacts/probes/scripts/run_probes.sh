#!/bin/zsh
# Audit probe runner for the 2026-10-16 audit. Serial, logged, disposable dirs.
REPO=/Users/sainathsapa/dsk/PloMID/plomid
BIN=$REPO/target/release/plomid-server
STAGE=/private/var/folders/49/bjpspnbx333d6_2g0tyjh7540000gn/T/opencode/audit16-stage
RUN=/private/var/folders/49/bjpspnbx333d6_2g0tyjh7540000gn/T/opencode/audit16-run
OUT=$RUN/logs
mkdir -p $OUT

wait_ready() {
  python3 - "$1" <<'EOF'
import sys, time, psycopg
dsn = sys.argv[1]
for _ in range(300):
    try:
        with psycopg.connect(dsn, connect_timeout=2) as c:
            with c.cursor() as cur:
                cur.execute("SELECT 1")
        sys.exit(0)
    except Exception:
        time.sleep(0.2)
sys.exit(1)
EOF
}

boot_server() { # dir port logfile
  rm -rf "$1"; mkdir -p "$1"
  "$BIN" --data "$1" --host 127.0.0.1 --port "$2" --username plomid --password plomid --log-level warn > "$3" 2>&1 &
  echo $!
}

echo "=== [$(date +%H:%M:%S)] 1. storage split repro (port 6120)" | tee -a $OUT/run_probes.progress
D=$RUN/split
SRV=$(boot_server $D 6120 $OUT/split-server.log)
wait_ready "postgresql://plomid:plomid@127.0.0.1:6120/plomid"
python3 $STAGE/storage_split_repro.py "postgresql://plomid:plomid@127.0.0.1:6120/plomid" > $OUT/storage_split_fail.log 2>&1
echo "EXIT=$?" >> $OUT/storage_split_fail.log
python3 $STAGE/storage_split_repro.py "postgresql://plomid:plomid@127.0.0.1:6120/plomid" --control > $OUT/storage_split_control.log 2>&1
echo "EXIT=$?" >> $OUT/storage_split_control.log
kill $SRV 2>/dev/null; wait $SRV 2>/dev/null

echo "=== [$(date +%H:%M:%S)] 2. shared-server probes (port 6130)" | tee -a $OUT/run_probes.progress
D=$RUN/s1
SRV=$(boot_server $D 6130 $OUT/s1-server.log)
wait_ready "postgresql://plomid:plomid@127.0.0.1:6130/plomid"
for probe in p0_revalidation param_avg_ilike_probe txn_mvcc_probe workload_probe vector_commit_probe; do
  echo "--- $probe [$(date +%H:%M:%S)]" | tee -a $OUT/run_probes.progress
  python3 $STAGE/$probe.py "postgresql://plomid:plomid@127.0.0.1:6130/plomid" > $OUT/$probe.log 2>&1
  echo "EXIT=$?" >> $OUT/$probe.log
done
kill $SRV 2>/dev/null; wait $SRV 2>/dev/null

echo "=== [$(date +%H:%M:%S)] 3. self-booting probes" | tee -a $OUT/run_probes.progress
for probe in catalog_ddl_probe backup_crash_probe rbac_probe scale_probe; do
  echo "--- $probe [$(date +%H:%M:%S)]" | tee -a $OUT/run_probes.progress
  python3 $STAGE/$probe.py --bin $BIN > $OUT/$probe.log 2>&1
  echo "EXIT=$?" >> $OUT/$probe.log
done

echo "=== [$(date +%H:%M:%S)] 4. qualify gate (port 6000)" | tee -a $OUT/run_probes.progress
D=$RUN/qual
SRV=$(boot_server $D 6000 $OUT/qualify-server.log)
wait_ready "postgresql://plomid:plomid@127.0.0.1:6000/plomid"
cd $REPO
DB_DSN="postgresql://plomid:plomid@127.0.0.1:6000/plomid" python3 tests/python/vector_qualify.py > $OUT/qualify_commit.log 2>&1
echo "EXIT=$?" >> $OUT/qualify_commit.log
kill $SRV 2>/dev/null; wait $SRV 2>/dev/null

echo "=== [$(date +%H:%M:%S)] ALL DONE" | tee -a $OUT/run_probes.progress
echo DONE > $OUT/run_probes.done
