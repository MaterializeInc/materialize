#!/usr/bin/env python3
"""Swap eval sweep: run AuctionScenario on one size, measure peak RSS, swap, arrangement size."""
import argparse, os, subprocess, sys, threading, time, psycopg
from pathlib import Path

SETUP_SQL_TEMPLATE = """
DROP CLUSTER IF EXISTS lg CASCADE;
CREATE CLUSTER lg SIZE '{size}';
SET cluster = 'lg';
DROP MATERIALIZED VIEW IF EXISTS auctions CASCADE;
DROP MATERIALIZED VIEW IF EXISTS bids CASCADE;
DROP VIEW IF EXISTS auctions_core CASCADE;
DROP VIEW IF EXISTS random CASCADE;
DROP VIEW IF EXISTS moments CASCADE;
DROP VIEW IF EXISTS seconds CASCADE;
DROP VIEW IF EXISTS minutes CASCADE;
DROP VIEW IF EXISTS hours CASCADE;
DROP VIEW IF EXISTS days CASCADE;
DROP VIEW IF EXISTS years CASCADE;
DROP VIEW IF EXISTS items CASCADE;
DROP TABLE IF EXISTS empty CASCADE;

CREATE TABLE empty (e TIMESTAMP);
CREATE VIEW items (id, item) AS VALUES
    (0, 'Signed Memorabilia'),(1,'City Bar Crawl'),(2,'Best Pizza in Town'),(3,'Gift Basket'),(4,'Custom Art');

CREATE VIEW years AS
SELECT * FROM generate_series('1970-01-01 00:00:00+00','2099-01-01 00:00:00+00','1 year') year
WHERE mz_now() BETWEEN year AND year + '1 year' + '{scale} day';

CREATE VIEW days AS
SELECT * FROM (
    SELECT generate_series(year, year + '1 year' - '1 day'::interval, '1 day') AS day FROM years
    UNION ALL SELECT * FROM empty)
WHERE mz_now() BETWEEN day AND day + '1 day' + '{scale} day';

CREATE VIEW hours AS
SELECT * FROM (
    SELECT generate_series(day, day + '1 day' - '1 hour'::interval, '1 hour') AS hour FROM days
    UNION ALL SELECT * FROM empty)
WHERE mz_now() BETWEEN hour AND hour + '1 hour' + '{scale} day';

CREATE VIEW minutes AS
SELECT * FROM (
    SELECT generate_series(hour, hour + '1 hour' - '1 minute'::interval, '1 minute') AS minute FROM hours
    UNION ALL SELECT * FROM empty)
WHERE mz_now() BETWEEN minute AND minute + '1 minute' + '{scale} day';

CREATE VIEW seconds AS
SELECT * FROM (
    SELECT generate_series(minute, minute + '1 minute' - '1 second'::interval, '1 second') AS second FROM minutes
    UNION ALL SELECT * FROM empty)
WHERE mz_now() BETWEEN second AND second + '1 second' + '{scale} day';

CREATE DEFAULT INDEX ON years;
CREATE DEFAULT INDEX ON days;
CREATE DEFAULT INDEX ON hours;
CREATE DEFAULT INDEX ON minutes;
CREATE DEFAULT INDEX ON seconds;

CREATE VIEW moments AS
SELECT second AS moment FROM seconds
WHERE mz_now() >= second AND mz_now() < second + '{scale} day';

CREATE VIEW random AS SELECT moment, digest(moment::text, 'md5') AS random FROM moments;

CREATE VIEW auctions_core AS
SELECT moment, random,
    get_byte(random,0)+get_byte(random,1)*256+get_byte(random,2)*65536 AS id,
    get_byte(random,3)+get_byte(random,4)*256 AS seller,
    get_byte(random,5) AS item,
    moment + get_byte(random,6)*'1 minutes'::interval AS end_time
FROM random;

CREATE MATERIALIZED VIEW auctions AS
SELECT auctions_core.id, seller, items.item, end_time FROM auctions_core, items
WHERE auctions_core.item % 5 = items.id;

CREATE MATERIALIZED VIEW bids AS
WITH prework AS (
    SELECT id AS auction_id, moment AS auction_start, end_time AS auction_end,
        digest(random::text || generate_series(1, get_byte(random,5))::text,'md5') AS random
    FROM auctions_core)
SELECT get_byte(random,0)+get_byte(random,1)*256+get_byte(random,2)*65536 AS id,
    get_byte(random,3)+get_byte(random,4)*256 AS buyer,
    auction_id, get_byte(random,5)::numeric AS amount,
    auction_start + get_byte(random,6)*'1 minutes'::interval AS bid_time
FROM prework;
"""


def get_clusterd_pid_for_cluster(cur, cluster_name: str) -> int | None:
    """Find clusterd PID for the given user cluster by mz_clusters.id."""
    cur.execute("SELECT id FROM mz_clusters WHERE name=%s", (cluster_name,))
    row = cur.fetchone()
    if not row:
        return None
    cid = str(row[0])
    try:
        out = subprocess.check_output(["pgrep", "-f", f"cluster-{cid}-replica-"]).decode().strip().splitlines()
    except subprocess.CalledProcessError:
        return None
    return int(out[0]) if out else None


def read_proc_status(pid: int) -> dict:
    d = {}
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                k, _, v = line.partition(":")
                d[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return d


def read_vmstat() -> dict:
    d = {}
    with open("/proc/vmstat") as f:
        for line in f:
            k, v = line.split()
            d[k] = int(v)
    return d


def monitor_loop(pid: int, out_path: Path, stop_at: float):
    """Sample /proc stats every 1s until stop_at (unix ts)."""
    with out_path.open("w") as f:
        f.write("ts,vmrss_kb,vmswap_kb,pswpin,pswpout\n")
        base_vm = read_vmstat()
        while time.time() < stop_at:
            st = read_proc_status(pid)
            vm = read_vmstat()
            rss = int(st.get("VmRSS", "0 kB").split()[0]) if "VmRSS" in st else 0
            swap = int(st.get("VmSwap", "0 kB").split()[0]) if "VmSwap" in st else 0
            f.write(f"{time.time():.3f},{rss},{swap},{vm['pswpin']-base_vm['pswpin']},{vm['pswpout']-base_vm['pswpout']}\n")
            f.flush()
            time.sleep(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", required=True)
    ap.add_argument("--scale", type=int, default=1)
    ap.add_argument("--hydration-wait", type=int, default=120, help="max seconds to wait for index hydration")
    ap.add_argument("--query-count", type=int, default=200, help="steady-state point queries to run")
    ap.add_argument("--out-dir", default="/home/ubuntu/swap_results")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)
    size = args.size
    scale = args.scale
    tag = f"{size}_s{scale}"
    print(f"=== size={size} scale={scale} ===")

    conn = psycopg.connect("postgres://materialize@localhost:6875/materialize", autocommit=True)
    cur = conn.cursor()
    cur.execute("SET transaction_isolation = 'serializable'")

    t0 = time.time()
    setup_sql = SETUP_SQL_TEMPLATE.format(size=size, scale=scale)
    for stmt in [s.strip() for s in setup_sql.split(";") if s.strip()]:
        cur.execute(stmt)
    setup_t = time.time() - t0
    print(f"setup: {setup_t:.1f}s")

    # Resolve clusterd PID now (cluster exists from setup)
    clusterd_pid = None
    for _ in range(30):
        clusterd_pid = get_clusterd_pid_for_cluster(cur, "lg")
        if clusterd_pid:
            break
        time.sleep(0.5)
    if not clusterd_pid:
        print("ERROR: could not resolve clusterd pid for lg", file=sys.stderr)
        sys.exit(1)
    print(f"clusterd pid: {clusterd_pid}")

    stop_at = time.time() + args.hydration_wait + 30
    mon_thread = threading.Thread(
        target=monitor_loop, args=(clusterd_pid, out_dir / f"{tag}.csv", stop_at), daemon=True
    )
    mon_thread.start()

    try:
        t_hyd = time.time()
        cur.execute("DROP INDEX IF EXISTS bids_id_idx CASCADE")
        cur.execute("CREATE INDEX bids_id_idx ON bids(id)")
        # Force completion via a probe query
        cur.execute("SELECT count(*) FROM bids WHERE id = 123123123")
        hyd_s = time.time() - t_hyd
        print(f"hydration+index: {hyd_s:.1f}s")

        # Arrangement size (per-replica introspection, requires SET cluster)
        cur.execute("SET cluster = 'lg'")
        try:
            cur.execute("""
                SELECT coalesce(sum(records),0), coalesce(sum(size),0)
                FROM mz_introspection.mz_arrangement_sizes
            """)
            row = cur.fetchone() or (0, 0)
            arr_records, arr_bytes = row[0], row[1]
        except Exception as e:
            print(f"arrangement query failed: {e}")
            arr_records, arr_bytes = None, None
        print(f"arrangement: records={arr_records}, bytes={arr_bytes}")

        # Steady-state point query phase: N hits on the index, random ids
        import random, statistics
        rng = random.Random(42)
        lats = []
        for _ in range(args.query_count):
            target = rng.randint(0, 2**24 - 1)
            t = time.time()
            cur.execute("SELECT count(*) FROM bids WHERE id = %s", (target,))
            cur.fetchone()
            lats.append((time.time() - t) * 1000.0)
        lats.sort()
        p50 = lats[len(lats) // 2]
        p95 = lats[int(len(lats) * 0.95)]
        p99 = lats[int(len(lats) * 0.99)]
        mean = statistics.mean(lats)
        print(f"point-query ms: mean={mean:.2f} p50={p50:.2f} p95={p95:.2f} p99={p99:.2f}")

        # Persist raw latencies per size
        (out_dir / f"{tag}_lat.txt").write_text("\n".join(f"{x:.3f}" for x in lats))
        time.sleep(2)
    finally:
        mon_thread.join(timeout=args.hydration_wait + 35)

    # Dump summary line
    summary = out_dir / "summary.csv"
    write_header = not summary.exists()
    with summary.open("a") as f:
        if write_header:
            f.write("size,scale,setup_s,hydration_s,arr_records,arr_bytes,q_mean_ms,q_p50_ms,q_p95_ms,q_p99_ms\n")
        f.write(f"{size},{scale},{setup_t:.1f},{hyd_s:.1f},{arr_records},{arr_bytes},{mean:.2f},{p50:.2f},{p95:.2f},{p99:.2f}\n")
    print(f"wrote {summary}")


if __name__ == "__main__":
    main()
