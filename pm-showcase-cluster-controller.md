# Cluster controller: graceful reconfiguration, burst autoscaling, and full visibility

This document showcases the new cluster-management capabilities for product.
Every SQL statement and result below was captured verbatim from a locally
running Materialize built from this branch. Nothing is mocked or
hand-written. Two cosmetic notes:

- Local development builds use replica sizes spelled `scale=1,workers=2`; in
  cloud these read `100cc`, `200cc`, etc. Read `scale=1,workers=1` as "small"
  and `scale=1,workers=8` as "8x larger".
- To make transitions observable on a demo timescale, the demo clusters carry
  a deliberately expensive index that takes ~50 seconds to hydrate on a
  1-worker replica, a stand-in for real-world dataflows that take minutes to
  hours. Setup statements are in Appendix B.

## TL;DR: what's new for users

- **Resizing a cluster is now zero-downtime and runs in the background.**
  `ALTER CLUSTER ... SET (SIZE = ...)` returns in milliseconds. Materialize
  provisions the new replicas alongside the old ones, waits until they have
  fully hydrated, and only then cuts over and retires the old ones. The
  cluster answers queries throughout. The operation is durable: it survives
  session disconnects and a full `environmentd` restart.
- **Clusters can autoscale for hydration.** A new cluster option,
  `AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = ...))`, makes
  Materialize temporarily run one extra, larger replica whenever the cluster
  has objects that are not yet hydrated. The burst replica serves results
  long before the steady replica is ready, then tears itself down.
- **You can always see what's happening.** `SHOW CLUSTERS` now answers "what
  did I ask for, what is actually running, is something in progress" in one
  command; a new view `mz_internal.mz_cluster_reconfigurations` carries the
  detail; every transition lands in the audit log with a reason.

---

## Scenario 1: resizing a cluster with zero downtime

`prod_analytics` is a cluster at size `scale=1,workers=1` serving two
indexes: a cheap one (`order_totals_idx`) and the expensive
`event_fingerprint_idx` described above.

### The resize returns immediately

```
ALTER CLUSTER prod_analytics SET (SIZE = 'scale=1,workers=2');
Time: 104.202 ms
```

That's it. No session to babysit. The reconfiguration now runs in the
background.

### Watching it happen

`SHOW CLUSTERS` immediately shows the current and the target shape, the
in-flight flag, and the overlapping replica sets:

```
SHOW CLUSTERS WHERE name = 'prod_analytics';
      name      |                    replicas                    |   current_size    |    target_size    | reconfiguration_in_flight | burst_size | comment
----------------+------------------------------------------------+-------------------+-------------------+---------------------------+------------+---------
 prod_analytics | r1 (scale=1,workers=1), r2 (scale=1,workers=2) | scale=1,workers=1 | scale=1,workers=2 | t                         |            |
(1 row)
```

The new introspection view adds the enforcement deadline. This `ALTER` gave
no explicit timeout, so the system default of 1 day applies. A transition is
never unbounded:

```
SELECT c.name, r.current_size, r.target_size,
       r.reconfiguration_in_flight AS in_flight,
       to_timestamp(r.reconfiguration_deadline::text::numeric / 1000) AS deadline
FROM mz_internal.mz_cluster_reconfigurations r
JOIN mz_clusters c ON c.id = r.cluster_id
WHERE c.name = 'prod_analytics';
      name      |   current_size    |    target_size    | in_flight |          deadline
----------------+-------------------+-------------------+-----------+----------------------------
 prod_analytics | scale=1,workers=1 | scale=1,workers=2 | t         | 2026-06-11 23:58:22.012+00
(1 row)
```

### No downtime

Mid-reconfiguration (while the new replica is still hydrating) queries
against both indexes answer in milliseconds, served by the old replica:

```
SELECT total FROM order_totals WHERE id = 42;
 total
-------
   100
(1 row)
Time: 11.176 ms

SELECT fp FROM event_fingerprint;
                fp
----------------------------------
 ffffebfcda3c1606012f242a4104711e
(1 row)
Time: 19.350 ms
```

Note that `mz_clusters.size` still reports `scale=1,workers=1`: the catalog
reports the *realized* configuration (what is actually serving) and the
pending target is surfaced separately. Some 40 seconds later the new replica
has hydrated, the controller cuts over, and everything reads the new size:

```
SHOW CLUSTERS WHERE name = 'prod_analytics';
      name      |        replicas        |   current_size    |    target_size    | reconfiguration_in_flight | burst_size | comment
----------------+------------------------+-------------------+-------------------+---------------------------+------------+---------
 prod_analytics | r2 (scale=1,workers=2) | scale=1,workers=2 | scale=1,workers=2 | f                         |            |
(1 row)
```

### A full audit trail

Both the cluster-level transitions and every replica the controller touched
are recorded in `mz_audit_events`. Replica creates carry the strategy that
provisioned them as their reason; a replica the cluster's configuration no
longer calls for is dropped with reason `retired`:

```
SELECT details->>'transition' AS transition,
       details->>'target_size' AS target_size, occurred_at
FROM mz_catalog.mz_audit_events
WHERE event_type = 'alter' AND object_type = 'cluster'
  AND details->>'cluster_name' = 'prod_analytics'
  AND details->>'transition' IS NOT NULL
ORDER BY id;
 transition |    target_size    |        occurred_at
------------+-------------------+----------------------------
 started    | scale=1,workers=2 | 2026-06-10 23:58:22.013+00
 finalized  | scale=1,workers=2 | 2026-06-10 23:59:05.21+00
(2 rows)

SELECT event_type, details->>'replica_name' AS replica,
       details->>'logical_size' AS size, details->>'reason' AS reason
FROM mz_catalog.mz_audit_events
WHERE object_type = 'cluster-replica'
  AND details->>'cluster_name' = 'prod_analytics'
ORDER BY id;
 event_type | replica |       size        |     reason
------------+---------+-------------------+-----------------
 create     | r1      | scale=1,workers=1 | manual
 create     | r2      | scale=1,workers=2 | reconfiguration
 drop       | r1      |                   | retired
(3 rows)
```

### Changing your mind: cancellation

Re-issuing `ALTER CLUSTER` while a reconfiguration is in flight simply
re-targets it; issuing it with the *current* size cancels. Here we start a
move to `workers=4`, then cancel it. The in-flight replica is dropped and
the audit log records the change of heart:

```
ALTER CLUSTER prod_analytics SET (SIZE = 'scale=1,workers=4');
SHOW CLUSTERS WHERE name = 'prod_analytics';
      name      |                    replicas                    |   current_size    |    target_size    | reconfiguration_in_flight | burst_size | comment
----------------+------------------------------------------------+-------------------+-------------------+---------------------------+------------+---------
 prod_analytics | r2 (scale=1,workers=2), r3 (scale=1,workers=4) | scale=1,workers=2 | scale=1,workers=4 | t                         |            |
(1 row)

ALTER CLUSTER prod_analytics SET (SIZE = 'scale=1,workers=2');
-- a few seconds later:
SHOW CLUSTERS WHERE name = 'prod_analytics';
      name      |        replicas        |   current_size    |    target_size    | reconfiguration_in_flight | burst_size | comment
----------------+------------------------+-------------------+-------------------+---------------------------+------------+---------
 prod_analytics | r2 (scale=1,workers=2) | scale=1,workers=2 | scale=1,workers=2 | f                         |            |
(1 row)

 transition |    target_size
------------+-------------------
 started    | scale=1,workers=2
 finalized  | scale=1,workers=2
 started    | scale=1,workers=4
 cancelled  | scale=1,workers=2
 finalized  | scale=1,workers=2
(5 rows)
```

### Bounding a reconfiguration: timeouts that survive disconnects

`WITH (WAIT UNTIL READY (TIMEOUT ..., ON TIMEOUT ...))` bounds an individual
reconfiguration. The deadline is stored in the catalog and enforced by the
system, not by the user's session. Closing the laptop no longer aborts
anything. The default action, `ROLLBACK`, is the safe one: if the target
can't hydrate in time, the cluster stays on its current shape and keeps
serving.

Here we try to *downsize* to a shape that cannot hydrate within a 10-second
deadline. While the clock runs, both replica sets are up:

```
ALTER CLUSTER prod_analytics SET (SIZE = 'scale=1,workers=1')
  WITH (WAIT UNTIL READY (TIMEOUT '10s', ON TIMEOUT 'ROLLBACK'));

SHOW CLUSTER REPLICAS WHERE cluster = 'prod_analytics';
    cluster     | replica |       size        | ready | comment
----------------+---------+-------------------+-------+---------
 prod_analytics | r2      | scale=1,workers=2 | t     |
 prod_analytics | r3      | scale=1,workers=1 | f     |
(2 rows)
```

At the deadline the controller rolls back on its own: the not-yet-hydrated
target replica is dropped, the realized configuration is untouched, and the
cluster reads settled again, never having stopped serving:

```
-- 15 seconds later:
SHOW CLUSTERS WHERE name = 'prod_analytics';
      name      |        replicas        |   current_size    |    target_size    | reconfiguration_in_flight | burst_size | comment
----------------+------------------------+-------------------+-------------------+---------------------------+------------+---------
 prod_analytics | r2 (scale=1,workers=2) | scale=1,workers=2 | scale=1,workers=2 | f                         |            |
(1 row)

SELECT total FROM order_totals WHERE id = 42;
 total
-------
   100
(1 row)
```

The abandoned target isn't lost: the timeout is recorded as a durable
`timed-out` transition in the audit log, carrying the deadline and the target
that didn't make it. (`finalized` events carry the deadline too, so a late or
forced cut-over is distinguishable from an in-time one.)

```
SELECT details->>'transition' AS transition,
       details->>'target_size' AS target_size,
       to_timestamp((details->>'deadline')::numeric / 1000) AS deadline
FROM mz_catalog.mz_audit_events
WHERE event_type = 'alter' AND object_type = 'cluster'
  AND details->>'cluster_name' = 'prod_analytics'
  AND details->>'transition' IS NOT NULL
ORDER BY id DESC LIMIT 2;
 transition |    target_size    |         deadline
------------+-------------------+---------------------------
 timed-out  | scale=1,workers=1 | 2026-06-10 23:59:23.09+00
 started    | scale=1,workers=1 | 2026-06-10 23:59:23.09+00
(2 rows)
```

### Surviving a restart

The user's intent is durable, so a reconfiguration continues across a full
`environmentd` restart. We start one (back down to `workers=1`), then kill
`environmentd` mid-flight and restart it. Immediately after the restart:

```
SHOW CLUSTERS WHERE name = 'prod_analytics';
      name      |                    replicas                    |   current_size    |    target_size    | reconfiguration_in_flight | burst_size | comment
----------------+------------------------------------------------+-------------------+-------------------+---------------------------+------------+---------
 prod_analytics | r2 (scale=1,workers=2), r3 (scale=1,workers=1) | scale=1,workers=2 | scale=1,workers=1 | t                         |            |
(1 row)
```

Still in flight, same target, and it completes on its own. The audit trail
shows one continuous operation spanning the crash (`started` at 23:59:29,
kill at ~23:59:37, `finalized` at 00:00:43):

```
 transition |    target_size    |        occurred_at
------------+-------------------+----------------------------
 finalized  | scale=1,workers=1 | 2026-06-11 00:00:43.893+00
 started    | scale=1,workers=1 | 2026-06-10 23:59:29.301+00
(2 rows)
```

---

## Scenario 2: burst autoscaling for hydration

A new cluster option declares the policy once; the system manages the burst
lifecycle from then on, with no user action:

```
CREATE CLUSTER fast_start (
  SIZE = 'scale=1,workers=1',
  AUTO SCALING STRATEGY = (ON HYDRATION (
    HYDRATION SIZE = 'scale=1,workers=8',
    LINGER DURATION = '15s'))
);

SHOW CREATE CLUSTER fast_start;
 fast_start | CREATE CLUSTER "fast_start" (AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=8', LINGER DURATION = INTERVAL '00:00:15')), ..., REPLICATION FACTOR = 1, SIZE = 'scale=1,workers=1', SCHEDULE = MANUAL)
```

The policy is armed existentially: a burst runs only while some object on the
cluster is not yet hydrated. The freshly created cluster has no objects, so
nothing bursts, just the steady replica:

```
SHOW CLUSTERS WHERE name = 'fast_start';
    name    |        replicas        |   current_size    |    target_size    | reconfiguration_in_flight | burst_size | comment
------------+------------------------+-------------------+-------------------+---------------------------+------------+---------
 fast_start | r1 (scale=1,workers=1) | scale=1,workers=1 | scale=1,workers=1 | f                         |            |
(1 row)
```

### Burst fires when there is hydration work

We attach an expensive index (~50s to hydrate at the cluster's steady size).
Within a tick, the controller runs an extra replica at the configured
`HYDRATION SIZE` (8x the steady size), visible both in the replica list and
in the `burst_size` column:

```
CREATE INDEX pageview_fingerprint_idx IN CLUSTER fast_start ON pageview_fingerprint (fp);

-- 4 seconds after attaching the index:
SHOW CLUSTERS WHERE name = 'fast_start';
    name    |                    replicas                    |   current_size    |    target_size    | reconfiguration_in_flight |    burst_size     | comment
------------+------------------------------------------------+-------------------+-------------------+---------------------------+-------------------+---------
 fast_start | r1 (scale=1,workers=1), r2 (scale=1,workers=8) | scale=1,workers=1 | scale=1,workers=1 | f                         | scale=1,workers=8 |
(1 row)
```

### The payoff: results long before the steady replica is ready

Half a minute in, the burst replica has fully hydrated the index while the
steady replica is still computing (it hasn't even reported hydration state
for the index yet), and the cluster is already answering in milliseconds:

```
SELECT i.name AS index, cr.name AS replica, cr.size, h.hydrated
FROM mz_internal.mz_hydration_statuses h
JOIN mz_indexes i ON i.id = h.object_id
JOIN mz_cluster_replicas cr ON cr.id = h.replica_id
JOIN mz_clusters c ON c.id = cr.cluster_id
WHERE c.name = 'fast_start' AND i.name = 'pageview_fingerprint_idx'
ORDER BY cr.name;
          index           | replica |       size        | hydrated
--------------------------+---------+-------------------+----------
 pageview_fingerprint_idx | r2      | scale=1,workers=8 | t
(1 row)

SELECT fp FROM pageview_fingerprint;
                fp
----------------------------------
 ffffebfcda3c1606012f242a4104711e
(1 row)
Time: 15.738 ms
```

Without the burst, this query would have waited ~50 seconds for the steady
replica.

### Burst tears itself down

Once the steady replica hydrates, the burst replica lingers for the
configured `LINGER DURATION` (a guard against thrash while a user is actively
creating objects), then disappears on its own:

```
SHOW CLUSTER REPLICAS WHERE cluster = 'fast_start';
  cluster   | replica |       size        | ready | comment
------------+---------+-------------------+-------+---------
 fast_start | r1      | scale=1,workers=1 | t     |
(1 row)
```

The full lifecycle is in the audit log: burst transitions at the cluster
level, the replica creates with reason `hydration-burst`, and the teardown
drops with reason `retired`:

```
 transition |    burst_size     |        occurred_at
------------+-------------------+----------------------------
 started    | scale=1,workers=8 | 2026-06-11 00:01:00.728+00
 finished   | scale=1,workers=8 | 2026-06-11 00:02:17.058+00
(2 rows)

 event_type | replica |       size        |     reason
------------+---------+-------------------+-----------------
 create     | r1      | scale=1,workers=1 | manual
 create     | r2      | scale=1,workers=8 | hydration-burst
 drop       | r2      |                   | retired
(3 rows)
```

### Managing the policy, and guardrails

The policy round-trips through `SHOW CREATE CLUSTER`, is removed with
`ALTER ... RESET`, and re-added or changed with `ALTER ... SET` (shown live
in the run; output as in the creation snippet above). Nonsensical
configurations are rejected up front:

```
CREATE CLUSTER bad_burst (
  SIZE = 'scale=1,workers=2',
  AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=2'))
);
ERROR:  HYDRATION SIZE must differ from the cluster SIZE ('scale=1,workers=2'); a burst replica at the same size would not accelerate hydration

CREATE CLUSTER bad_burst (
  SIZE = 'scale=1,workers=1',
  SCHEDULE = ON REFRESH,
  AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=2'))
);
ERROR:  AUTO SCALING STRATEGY cannot be combined with a SCHEDULE other than MANUAL
```

---

## Observability reference

| Surface | What it answers |
|---|---|
| `SHOW CLUSTERS` | Per cluster: `current_size`, `target_size`, `reconfiguration_in_flight`, `burst_size`, plus the replica list. "What did I ask for / what's running / is something happening" in one command. |
| `mz_internal.mz_cluster_reconfigurations` | The detail: current vs. target size, replication factor, availability zones; in-flight flag; enforcement deadline; active burst size. |
| `mz_internal.mz_hydration_statuses` | Per replica, per object: hydrated or not, the signal the controller acts on. |
| `mz_catalog.mz_audit_events` | History. Reconfiguration transitions: `started` / `finalized` / `timed-out` / `cancelled`. Burst transitions: `started` / `finished`. Replica creates carry the responsible strategy as their reason (`reconfiguration`, `hydration-burst`, `schedule`, the latter with the full `scheduling_policies` decision detail); replicas the configuration no longer calls for are dropped with reason `retired`. |

---

## Side-effect changes users will notice

These fall out of the new model rather than being headline features; they
belong in release notes and docs.

1. **A bare `ALTER CLUSTER SET (SIZE = ...)` is now graceful by default.**
   Previously it dropped and recreated the replicas immediately, leaving the
   cluster unavailable until the new replicas rehydrated. Now the
   zero-downtime overlap path runs by default, at the cost of transiently
   paying for both replica sets. The old immediate behavior is an explicit
   opt-in, `WITH (WAIT FOR '0s')`. The deadline is already past, so the
   controller cuts over at once without provisioning overlap replicas:

   ```
   ALTER CLUSTER batch_jobs SET (SIZE = 'scale=1,workers=2') WITH (WAIT FOR '0s');

   -- 3 seconds later: replaced in place, no overlap window
   SHOW CLUSTER REPLICAS WHERE cluster = 'batch_jobs';
     cluster   | replica |       size        | ready | comment
   ------------+---------+-------------------+-------+---------
    batch_jobs | r2      | scale=1,workers=2 | t     |
   (1 row)
   ```

2. **The implicit `ON TIMEOUT` default flips from `COMMIT` to `ROLLBACK`.** A
   timed-out reconfiguration now reverts to the old shape instead of cutting
   over to a possibly-unhydrated target. Safe by default: a timeout never
   silently induces downtime.

3. **`WITH (WAIT ...)` is durable; disconnects no longer abort.** Previously
   the session carried the reconfiguration, so a network blip or a closed SQL
   tool aborted it. Now the target, deadline, and timeout action live in the
   catalog and the controller enforces them regardless of session lifetime
   (Scenario 1 demonstrates surviving a full restart).

4. **`SHOW CLUSTERS` changed shape.** The `size` column is now
   `current_size`, and `target_size`, `reconfiguration_in_flight`, and
   `burst_size` are new (see any `SHOW CLUSTERS` output above). Anyone
   parsing `SHOW CLUSTERS` output must adapt.

5. **Scheduled (`SCHEDULE = ON REFRESH`) clusters now report
   `replication_factor = 0`.** The controller owns their replica set; a
   replica exists only inside refresh windows, and `mz_cluster_replicas` is
   the authoritative view of what is running. The feature itself behaves as
   before. It is now one strategy inside the controller framework rather
   than a separate mechanism:

   ```
   CREATE CLUSTER nightly (
     SIZE = 'scale=1,workers=1',
     SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '60 seconds')
   );

   SELECT name, replication_factor FROM mz_clusters WHERE name = 'nightly';
     name   | replication_factor
   ---------+--------------------
    nightly | 0
   (1 row)

   -- after creating a REFRESH EVERY materialized view on it, a replica runs
   -- inside the refresh window, while replication_factor still reads 0:
   SHOW CLUSTER REPLICAS WHERE cluster = 'nightly';
    cluster | replica |       size        | ready | comment
   ---------+---------+-------------------+-------+---------
    nightly | r1      | scale=1,workers=1 | t     |
   (1 row)
   ```

   The on-window replica create is attributed `schedule` in the audit log and
   carries the full scheduling decision: which materialized views needed a
   refresh, which were still inside their post-refresh compaction window, and
   the hydration-time estimate the window was widened by:

   ```
   SELECT event_type, details->>'replica_name' AS replica,
          details->>'reason' AS reason,
          jsonb_pretty(details->'scheduling_policies') AS scheduling_policies
   FROM mz_catalog.mz_audit_events
   WHERE object_type = 'cluster-replica' AND details->>'cluster_name' = 'nightly'
   ORDER BY id;
    event_type | replica |  reason  |            scheduling_policies
   ------------+---------+----------+--------------------------------------------
    create     | r1      | schedule | {                                         +
               |         |          |   "on_refresh": {                         +
               |         |          |     "decision": "on",                     +
               |         |          |     "hydration_time_estimate": "00:01:00",+
               |         |          |     "objects_needing_compaction": [],     +
               |         |          |     "objects_needing_refresh": [          +
               |         |          |       "u516"                              +
               |         |          |     ]                                     +
               |         |          |   }                                       +
               |         |          | }
   (1 row)
   ```

6. **Replica names churn on resize.** A graceful resize creates fresh
   replicas (`r1` → `r2` in Scenario 1); names are not preserved, and the
   `<name>-pending` replicas of the old mechanism are gone. Tooling should
   key on the cluster, not on replica names.

7. **`mz_clusters.size` / `SHOW CREATE CLUSTER` lag the request.** During a
   reconfiguration they keep reporting the pre-change (realized)
   configuration until cut-over; the pending target is in `SHOW CLUSTERS` /
   the introspection view. Reading back the size right after `ALTER` no
   longer shows the requested value.

8. **Audit log shape changes.** Replica creates gain new reasons
   (`reconfiguration`, `hydration-burst`); on-refresh creates keep reason
   `schedule` and the `scheduling_policies` decision detail (shown in 5.
   above). Every replica the controller retires (cut-over, cancellation,
   burst teardown, refresh-window close) is dropped with the new reason
   `retired`, where the legacy paths wrote `manual`. One deliberate nuance: a
   replication-factor *decrease* also drops the surplus replica as `retired`,
   even though the config change itself was user-initiated. New cluster-level
   transition events exist (see the observability table). The legacy
   scheduled-cluster "off" event with its `decision: off` detail blob is gone;
   the window closing is the `retired` drop itself.

9. **Billing.** Overlap replicas during a resize and burst replicas are
   ordinary replicas and are billed as such: a resize transiently bills both
   sets, a burst bills the `HYDRATION SIZE` replica while it runs. Not a new
   rule, but these replicas now appear without a user creating them
   explicitly.

---

## Appendix A: rollout gating

The work ships dark behind independent gates, so each piece can be rolled out
(and disabled) separately:

| Setting | Default | Gates |
|---|---|---|
| `enable_cluster_controller` | `false` | Master switch: the controller owns managed-cluster replica sets; legacy paths bypassed. |
| `enable_background_alter_cluster` | `false` | Off: `ALTER` blocks the session until the reconfiguration resolves (same durable mechanism underneath, disconnecting still doesn't abort). On: `ALTER` returns immediately. |
| `enable_auto_scaling_strategy` | `false` | SQL acceptance of `AUTO SCALING STRATEGY` (catalog-safe feature flag). |
| `enable_hydration_burst` | `true` | Break-glass: disables only the burst strategy, environment-wide; graceful reconfiguration and `ON REFRESH` untouched. |
| `enable_zero_downtime_cluster_reconfiguration` | `false` | Pre-existing flag for the `WITH (WAIT ...)` SQL surface. |
| `default_cluster_reconfiguration_timeout` | 1 day | Deadline written when an `ALTER` gives no explicit timeout, so no transition is ever unbounded. |
| `default_hydration_burst_linger` | `0s` | Linger used when a strategy omits `LINGER DURATION`. |
| `cluster_controller_tick_interval` | `5s` | Controller reconcile cadence. |

## Appendix B: demo environment setup

Local environment: `bin/environmentd --reset -- --unsafe-mode`, a debug
development build (`--unsafe-mode` only for `mz_unsafe.mz_sleep` used as a
wait between captures). Flags, as `mz_system` (the replay script additionally
enables `unsafe_enable_unsafe_functions` and the `REFRESH EVERY` /
`SCHEDULE = ON REFRESH` feature flags):

```sql
ALTER SYSTEM SET enable_cluster_controller = true;
ALTER SYSTEM SET enable_zero_downtime_cluster_reconfiguration = true;
ALTER SYSTEM SET enable_background_alter_cluster = true;
ALTER SYSTEM SET enable_auto_scaling_strategy = true;
ALTER SYSTEM SET cluster_controller_tick_interval = '1s';  -- default 5s; snappier demo
```

Scenario 1 objects. `event_fingerprint_idx` is the deliberately expensive
index: an md5 over ~10 kB per row, 320k rows, ~50s to hydrate on one worker
of the debug build (an optimized build chews through roughly 6x as many rows
in the same time, so scale the row count accordingly). The data is loaded in
eight separate inserts so the work spreads across the workers of a larger
replica (which is what makes the 8-worker burst replica in Scenario 2 hydrate
~8x faster):

```sql
CREATE CLUSTER prod_analytics (SIZE = 'scale=1,workers=1');
CREATE TABLE orders (id int, amount numeric);
INSERT INTO orders SELECT generate_series(1, 1000), 100;
CREATE TABLE events (x int);
INSERT INTO events SELECT generate_series(1, 40000);
-- ... seven more 40k-row inserts ...
SET cluster = prod_analytics;
CREATE VIEW order_totals AS SELECT id, sum(amount) AS total FROM orders GROUP BY id;
CREATE INDEX order_totals_idx ON order_totals (id);
CREATE VIEW event_fingerprint AS
  SELECT max(md5(repeat(x::text, 2000))) AS fp FROM events;
CREATE INDEX event_fingerprint_idx ON event_fingerprint (fp);
```

Scenario 2 uses an identical fingerprint view (`pageview_fingerprint` over a
freshly loaded `pageviews` table) indexed `IN CLUSTER fast_start`.

The complete capture script (every statement, wait, and kill in order) is
`pm-showcase-replay.sh` next to this document; running it against a fresh
environment regenerates every transcript quoted above.
