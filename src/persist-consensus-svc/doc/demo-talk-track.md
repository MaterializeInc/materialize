# Demo Talk Track: Group Commit Consensus

## Setup

Before starting, have these ready in separate terminals:

1. **Consensus service** (not started yet — we'll start it live)
2. **Monitoring stack** running: `cd misc/monitoring && ./mzcompose run default`
3. **environmentd** built but not started yet
4. **Console** running on port 6874 (optional)
5. **demo.sh** ready to run

Open Grafana at http://localhost:3001/d/consensus-svc-demo and the Console at http://localhost:6874.

---

## Act 1: environmentd Won't Start Without Consensus

Start environmentd with the RPC consensus URL:

```
./bin/environmentd +nightly --release -- \
  --persist-consensus-url='rpc://localhost:6890' \
  --system-parameter-default=default_timestamp_interval=100ms
```

> "We've added a new consensus backend to persist. Instead of pointing at Postgres or CRDB, we point environmentd at an RPC endpoint — `rpc://localhost:6890`. Right now nothing is listening there, so environmentd is stuck in a retry loop trying to connect. It can't start without consensus."

Let it sit for a few seconds so the audience can see the retry logs.

## Act 2: Start the Consensus Service

In another terminal, start the consensus service:

```
AWS_PROFILE=mz-scratch-admin cargo run -p mz-persist-consensus-svc -- \
  --s3-bucket phemberger-s3-directory-test--use1-az4--x-s3 \
  --s3-prefix consensus/ \
  --s3-region us-east-1
```

> "This is the group commit consensus service. It's a single-threaded actor that batches all shard CAS operations into one S3 write every 20 milliseconds. It's basically a log on object storage."

Watch environmentd connect and finish startup. Give a moment for the Console to load.

> "environmentd connected. You can see in Grafana that metrics are flowing — the service is alive, handling head and CAS operations. In the Console, Materialize is fully operational."

Point out the stats log line the service emits every 10 seconds.

## Act 3: Run the Demo — Scale Up Shards

Run the demo script:

```
src/persist-consensus-svc/demo/demo.sh
```

> "This script creates a Postgres source with a single row being updated 20 times a second. Then it starts adding materialized views in stages — 5, then 20, 50, 100, 200. Each MV creates its own persist shard that needs to CAS through consensus every tick."

**Watch Grafana as MVs are created:**

- **Active Shards** panel: step increases as MVs are added (5 → 20 → 50 → 100 → 200)
- **S3 WAL Writes/s** panel: rises initially, then **levels off and stays flat**
- **Ops per Flush Batch**: grows — more ops batched into each flush as shards increase
- **Operations/s by Type**: CAS committed rate climbs with shard count

> "This is the key insight. The S3 writes line stays flat while the shard count keeps climbing. With Postgres, every one of those shards would be an independent write — 200 shards at 1 tick/sec is 200 writes/sec to your database. Here, it's still just ~50 S3 PUTs regardless. And that number doesn't change whether you have 200 shards or 200,000."

**In the Console** (if running): show the materialized views appearing in the object list.

## Act 4: Freshness

> "We can also look at freshness. We're running with a 100ms timestamp interval, which is 10x faster than the default 1s tick. The consensus service flushes every 20ms, so it's not the bottleneck."

**Caveat:**

> "Fair warning — this is my 5-year-old laptop running everything locally against real S3 in us-east-1. S3 Express One Zone latencies within AWS are much faster — single-digit millisecond PUTs. You can see our actual S3 latency numbers in the WAL PUT Latency panel. Also, the Postgres source is probably falling behind — that's my laptop, not the consensus service."

## Act 5: Kill and Recover

Kill the consensus service (Ctrl+C in its terminal).

> "Watch what happens when the consensus service goes down."

- environmentd logs show retry loops — persist can't CAS, but it keeps trying
- The Console may show stale data but doesn't crash

After a few seconds, restart the consensus service.

> "On startup, the service does a quick recovery — it loads the latest snapshot from S3, then replays any WAL entries after it. No LIST operations needed, just sequential GETs."

Watch the recovery logs: `loaded snapshot`, `replaying WAL batch`, `recovery complete`.

> "And we're back. Materialize reconnects, the retry loops succeed, and everything resumes. The recovery was fast because snapshots bound how much WAL we need to replay."

## Act 6: Production Vision

> "A few things to note about where this could go:"

> "**One service per cluster.** Each cluster is a natural batching boundary. You partition by S3 prefix — `consensus/cluster-1/`, `consensus/cluster-2/` — so clusters don't interfere with each other."

> "**High availability.** Because the service uses S3 conditional writes (`If-None-Match: *`) as the real source of truth, you could run active-standby instances. The conditional PUT acts as a distributed fence — if two instances try to write the same batch, exactly one wins. The other sees a 412 and knows to back off."

> "**The cost story.** S3 Express One Zone at 50 PUTs/sec is about $11/month. Compare that to a dedicated Postgres or CRDB instance for consensus. And the write rate doesn't change as you add shards — it's O(1/batch_window), not O(shards)."

> "**It's a log on object storage.** The architecture is structurally the same thing persist already does — WAL entries, snapshots, compaction. A natural next step is 'persist-on-persist' — using an internal persist shard for the snapshot layer."
