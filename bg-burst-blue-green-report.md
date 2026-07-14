# Does hydration-burst autoscaling speed up a blue/green cutover?

**Short answer: yes.** On a local debug build, adding an `AUTO SCALING STRATEGY`
to the green cluster cut the blue/green readiness wait (the "deploy_await"
phase that dominates a cutover) from **68.4s to 12.4s, a 5.5x speedup**, with no
change to the cutover procedure and no permanent upsize of the production
cluster. Every number and capture below is verbatim from a locally running
Materialize built from this branch. Harness: `bg-burst-blue-green-replay.sh`
(this repo), which writes a verbatim transcript to
`/tmp/bg-burst-test/transcript.log` when run.

As in the PM showcase, local builds spell replica sizes `scale=1,workers=N`.
Read `workers=1` as "small" and `workers=8` as "8x larger".

## Why they compose (the mechanism)

- **Blue/green cutover** in Materialize is `ALTER CLUSTER/SCHEMA <a> SWAP WITH
  <b>`: an atomic catalog rename, sub-second, no data movement. The expensive
  part of a deployment is not the swap, it is *waiting for the green cluster's
  dataflows to hydrate* before you dare swap. That wait is a client-side poll
  over introspection views. The in-repo test
  (`test/cluster/blue-green-deployment/deploy.td`) and the real tooling
  (dbt-materialize, `mz-deploy`) all define "ready" as **hydrated on some
  replica** (join on `object_id` / `EXCEPT`; the tooling takes `MAX(hydrated)`
  over replicas, the "best replica"). No query requires *all* replicas to be
  hydrated.

- **Hydration burst** runs one extra, larger replica on a cluster while it has
  unhydrated objects, then tears it down once the steady replica catches up. It
  is armed on *any* managed cluster carrying the strategy. It is not coupled to
  blue/green in any way.

These two facts predict the win: create the new dataflows on a green cluster
that carries a burst strategy, the oversized burst replica hydrates them first,
the readiness poll (best-replica) goes green earlier, and you swap sooner. Then
the burst evaporates, so production is not left permanently larger.

## Method

Two back-to-back blue/green deployments against the same base data (an `events`
table, 320k rows, and a deliberately expensive fingerprint materialized view,
`max(md5(repeat(...)))` over `SELECT DISTINCT x`, chosen so the heavy map
distributes across a replica's workers). Both deployments poll the **exact
readiness query** from `deploy.td`, then run the real `SWAP`.

- **baseline**: green cluster at the steady size (`workers=1`).
- **burst**: green cluster at the steady size **plus**
  `AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=8',
  LINGER DURATION = '5s'))`.

Flags: `enable_cluster_controller`, `enable_auto_scaling_strategy`,
`enable_background_alter_cluster`, `cluster_controller_tick_interval='1s'`
(`enable_hydration_burst` defaults on).

## Result

| deployment | green cluster | deploy_await (create -> ready) |
|---|---|---|
| baseline | `workers=1` | **68.4s** |
| burst | `workers=1` + burst `workers=8` | **12.4s** |

Reference point: the blue cluster's own initial hydration at `workers=1` took
70.0s, so 68.4s baseline is the honest "wait for a `workers=1` replica" cost.

### Baseline: readiness waits on the single steady replica

```
>>> [baseline] deploy_await (create green dataflow -> readiness): 68.4s

SHOW CLUSTERS WHERE name = 'prod_deploy';
    name     |        replicas        | activity | comment
-------------+------------------------+----------+---------
 prod_deploy | r1 (scale=1,workers=1) |          |
```

### Burst: readiness satisfied by the burst replica, not the steady one

At the readiness moment the cluster is running both replicas, and the hydration
signal the readiness poll reads shows the object hydrated on the `workers=8`
burst replica (`r2`) while the steady `workers=1` replica (`r1`) has not yet
reported hydration at all:

```
>>> [burst] deploy_await (create green dataflow -> readiness): 12.4s

SHOW CLUSTERS WHERE name = 'prod_deploy';
    name     |                    replicas                    |               activity               | comment
-------------+------------------------------------------------+--------------------------------------+---------
 prod_deploy | r1 (scale=1,workers=1), r2 (scale=1,workers=8) | hydration burst at scale=1,workers=8 |

-- per-replica hydration of the deployed MV at the readiness moment:
 replica |       size        |   object    | hydrated
---------+-------------------+-------------+----------
 r2      | scale=1,workers=8 | fingerprint | t          <- burst, hydrated
                                                        (no r1 rows yet: steady still hydrating)
```

### The swap is unchanged, and it works

Same atomic transaction in both rounds; the production name ends up serving the
newly deployed definition:

```
BEGIN;
ALTER SCHEMA prod SWAP WITH prod_deploy;
ALTER CLUSTER prod SWAP WITH prod_deploy;
COMMIT;
-- swap succeeded (attempt 1)

SELECT version, fp FROM prod.fingerprint;   -- baseline round -> v1, burst round -> v2
```

### The burst spans the cutover, then tears itself down

The burst was keyed on the cluster's identity, not its name, so it kept
accelerating across the `SWAP` (which renamed the green cluster to `prod` at
17:53:52) and cleaned itself up once the steady replica finally hydrated. From
the durable audit log:

```
-- cluster-level burst transitions
   cluster   | transition |    burst_size     |        occurred_at
-------------+------------+-------------------+----------------------------
 prod_deploy | started    | scale=1,workers=8 | 2026-07-14 17:53:39
 prod        | finished   | scale=1,workers=8 | 2026-07-14 17:54:55

-- the burst replica's whole life: created for the burst, retired on teardown
 event_type |   cluster   | replica |     reason      |        occurred_at
------------+-------------+---------+-----------------+----------------------------
 create     | prod_deploy | r2      | hydration-burst | 2026-07-14 17:53:39
 drop       | prod        | r2      | retired         | 2026-07-14 17:54:55
```

Production ends up back at the steady size, no permanent upsize:

```
SHOW CLUSTERS WHERE name='prod';
 name |        replicas        | activity | comment
------+------------------------+----------+---------
 prod | r1 (scale=1,workers=1) |          |
```

## Notes and honest caveats

- **Magnitude is workload- and hardware-dependent.** The 5.5x here reflects
  this VM (16 cores, debug build) and a workload whose heavy map genuinely
  distributes across workers. A naive `max(md5(...))` over a table read only
  parallelized ~2x on this box, because the debug-build persist snapshot
  read is a large serial floor and the map got pushed onto few workers. Forcing
  an exchange (`SELECT DISTINCT x`) spread the map and restored ~6x scaling.
  The point being validated is the *interaction* (burst satisfies the
  blue/green readiness gate earlier); the exact multiplier tracks how
  parallelizable the green dataflows are, and an optimized build has a smaller
  read floor.

- **No coupling, no special casing.** Nothing in the burst strategy or the swap
  path knows about the other. Burst fires because the green cluster has
  unhydrated objects; readiness passes because the green cluster's objects are
  hydrated on some replica; the swap is a rename. They compose for free.

- **Billing.** During the burst window you transiently pay for the extra
  `HYDRATION SIZE` replica (here `workers=8`) on top of the steady one, exactly
  as if you had briefly created it by hand. It is gone after `LINGER DURATION`
  past steady hydration.

- **Fit with the tooling.** Real blue/green tooling (dbt `deploy_await`,
  `mz-deploy wait`) uses best-replica readiness, so this benefit applies without
  any tooling change: point the deploy at a cluster template that carries the
  strategy and the await phase shortens on its own.
