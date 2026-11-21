# Antithesis harness

Runs self-managed Materialize, deployed by the real orchestratord on
Kubernetes, under [Antithesis](https://antithesis.com). The target is the class
of bugs that only appear at a specific restart point, under a concurrent-writer
race, or in a timing window: 0dt rollouts, persist leases and finalization,
controller and catalog state rebuilt at boot, and source ingestion across
restarts.

The workload is one process in the fault-excluded `driver` pod: a
[hegel](https://hegel.dev) state machine whose rules send SQL and Kubernetes
requests and check every outcome against in-memory models. Its one test
command, `first_start_workload`, starts the workload. An eventually rule stops
faults, runs the post-fault checks and ends the timeline. The design, and a
comparison with the Test Composer workload it replaced, are in
[the design doc](../../doc/developer/design/20261120_antithesis_driver.md).

## What the driver checks

| Component | Guarantees |
|---|---|
| Client history (`history.py`) | Strict serializable, serializable and strong session serializable appends and reads of several keys, through tables, an index, materialized views and a join, checked for G0 to G2 anomalies with real-time edges by `serializability.py`, and for stale, lost, phantom and aborted reads |
| Contended counters (`counters.py`) | Read-then-write `UPDATE` and `INSERT ... SELECT` stay within the bounds of increments acknowledged and attempted |
| Atomic commits (`atomic.py`) | No read sees part of a multi-table write transaction, or a rolled-back one |
| IVM (`views.py`) | Twelve view kinds, each as an indexed view and a materialized view, equal a Python reference (`ivm_model.py`) on single reads and at every closed timestamp of a `SUBSCRIBE` |
| Postgres, MySQL and SQL Server sources (`cdc.py`, with `pg.py`, `mysql.py`, `sqlserver.py`) | Serial upstream transactions over three tables and a ledger. A lost COMMIT is resolved by ending its session and reading the ledger. One read of progress, ledger and exports: the ledger is a whole-transaction prefix of the commit order, consistent with the progress position (LSN or GTID), and every export equals its table replayed to that prefix, also after upstream `ADD COLUMN`. A further export of one table is created and dropped while the source ingests, under a burst of upstream transactions, and is read at once after its creation returns. It must equal its table at the same prefix as the others (a new export that is only ahead of them has a property of its own). Real-time recency reads see every transaction committed (for SQL Server, captured) before them. A fourth table, `types` (`cdc_types.py`), has one column per upstream type, 51 for Postgres, 41 for MySQL and 32 for SQL Server. A row's seed determines every other column, so each visible row is judged on its own |
| Webhooks (`webhook.py`) | Concurrent requests to a webhook source and a webhook table. No phantom, duplicate or altered rows, a read that shows a row shows every row acknowledged before it was sent, and after faults stop every acknowledged row is visible |
| Kafka source (`kafka.py`) | Produces to a topic that a background consumer tails as the ground truth. The none and upsert envelope exports equal the topic below the mapped offsets, the offsets are monotone, acknowledged produces are in the topic |
| Kafka formats (`kafka_formats.py`) | The same reasoning for a JSON source, an Avro source through the schema registry (none and upsert envelopes, a record of many field types whose values follow from its seed, one schema evolution), and a JSON upsert source that receives undecodable values and tombstones: a read fails exactly while a key's latest value is undecodable (`upsert_recovery_model.py`), a later document or tombstone heals it, and a second export reports the errors inline |
| Kafka sinks (`kafka.py`) | A Debezium sink of a view and an upsert sink of an aggregate, each in JSON and in Avro, all tailed read-committed. Stream invariants of `sink_oracle.py`, consolidated output equals the view at a committed frontier, and after faults stop equals the model of acknowledged writes |
| Window functions (`windows.py`) | Row numbers, ranks, `lag` and `lead` with per-row offsets, and framed aggregates, each as an indexed view and a materialized view, equal `window_retractions_model.py` evaluated on the base rows the same read returned, under updates that move rows between partitions |
| Interval aggregates (`aggregates.py`) | Grouped interval sums equal `aggregate_retractions_model.py`. A group whose sum leaves a component's range makes the view fail to read, and deleting the cause makes it readable again |
| Data types (`datatypes.py`) | One table with 49 typed columns (scalars, arrays, lists, maps, ranges), each value determined by the row's seed. The table, an indexed view, a materialized view, a view grouped by each column and one of each column's least and greatest value, read at one timestamp |
| COPY (`copies.py`) | A `COPY FROM STDIN` batch in text or csv is visible whole and once or not at all, also when the client fails it or drops the connection, and carries every type. `COPY TO STDOUT` in text, csv and binary returns the rows of the same query in the same transaction |
| Schema evolution (`schema_evolution.py`) | `ALTER TABLE ADD COLUMN` under concurrent inserts. Rows hold their written values and NULL or the default elsewhere (`schema_model.py`), `SELECT *` shows every acknowledged column, and dependents keep the columns they were defined over |
| Session rebinding (`rebinding.py`) | A durable view replaced between two definitions, per-session temporary views that shadow it, prepared statements and cursors on sessions of their own. A read never returns a definition replaced before it began, and a cursor keeps its snapshot |
| Cancellation (`cancel.py`) | A cancel request, `pg_cancel_backend` or a `statement_timeout` lands a drawn delay after a statement was sent: blind and read-then-write inserts, updates, deletes, `COMMIT`, `COPY FROM STDIN`, reads, subscriptions, and `CREATE` and `DROP` of indexes and materialized views. A statement answered as cancelled or timed out took no effect, batches stay whole, a session sees no more cancelled statements than cancel requests, and a cancel ends a subscription |
| Fixed timestamps (`asof.py`) | What a read returned at a timestamp is what every later read `AS OF` that timestamp returns: from the table, an indexed view and a materialized view, on each replica, by `SELECT` and by `SUBSCRIBE ... AS OF ... UP TO`, until the retained history ends. A timestamp within half the retained history stays readable |
| Blue/green deployments (`bluegreen.py`) | A staging schema and cluster are built, swapped with the live ones in one transaction and dropped, under reads and subscriptions through the live names. A read is answered by one generation, correct for it, shows every acknowledged row, never fails because a live name is missing, and never returns a generation older than one live before it began |
| System parameters (`flags.py`) | In half the timelines, parameters that CI also varies (persist, compute and storage write, compaction, fetch and hand-over paths) change while the workload runs. It has no check of its own: every other check holds under each value |
| Disruptions (`disrupt.py`) | Replica changes, deletions of environmentd, clusterd and orchestratord pods, also a drawn fraction of a second after a DDL operation is sent, and 0dt rollouts, some manually promoted, with kills in a chosen phase and a spec change (image, extra args, force, request, strategy, resources, or a revert) in a chosen phase. `activeGeneration` never decreases |
| Status claims (`claims.py`) | An `Applied` status means the active StatefulSet runs the spec, and `WaitingForApproval` means it does not (`spec_model.py`). Violations that a spec change during `Promoting` explains (CLO-339) have messages of their own |
| Read-only isolation (`read_only.py`) | A candidate generation acknowledges no write sent to its pod, holds no replication connection to the upstream Postgres (`isolation.py`), and the timestamp oracle never goes backwards |
| After faults stop (`health.py`, each component's `eventually`) | The CR settles, SQL serves, replicas come online, every pending statement finishes, every component's final read succeeds and passes its checks, sources catch up with every acknowledged upstream write, run, and advance, sinks run and reach every acknowledged write, no pod crash-loops. The components' checks run side by side, because a timeline seldom lives for minutes once faults stop |

A guarantee is an `always`, which fails when a run never evaluates it, and a
situation worth reaching is a `sometimes` or `reachable`, which fails when a
run never reaches it. Expect the latter on situations that need faults in
short runs. The assertion messages are those of the legacy workload's checks,
so their properties keep their history. hegel reports the run's own verdict
as `::materialize_driver passes properties`, which fails if the state machine
raised.

Not ported yet, in the order the design doc gives: the remaining rollout
observer oracles (protected StatefulSets, leader fencing, the serving version),
exit evidence, upstream TRUNCATE and DROP with their known-bug buckets, the
persistcli shard audits and access control. `COPY` to and from object storage
is not covered either.

The Kubernetes-only components (`claims.py`, `read_only.py`, the spec changes
and the memory log) have unit tests only: the `standalone` local loop does not
run them.

Each source runs when the driver has its upstream:
`MZ_ANTITHESIS_UPSTREAM_POSTGRES_URL`, `MZ_ANTITHESIS_UPSTREAM_MYSQL_URL`,
`MZ_ANTITHESIS_UPSTREAM_SQL_SERVER_URL` and `MZ_ANTITHESIS_KAFKA_BROKER`, as
the manifests give them. Webhooks always run.

## Running locally

```
bin/pyactivate -m pip install -r misc/python/materialize/antithesis/requirements.txt pymssql
bin/pyactivate -m pytest -q misc/python/materialize/antithesis misc/python/materialize/antithesis_driver
bin/pyactivate -m materialize.antithesis_driver.local --output /tmp/driver --duration 300 standalone
bin/pyactivate -m materialize.antithesis_driver.local --output /tmp/driver --duration 300 kind
```

`standalone` runs the driver on this machine against fresh
`materialize/materialized`, Postgres, MySQL, SQL Server and Redpanda
containers on one Docker network. SQL Server has only an amd64 image, which
runs under emulation on arm64. `kind` deploys these manifests into a kind cluster with a release's
published environmentd, clusterd and orchestratord images and a locally built
driver image, and needs `kind`,
`kubectl`, `helm` and the license key (`MZ_CI_LICENSE_KEY` or
`~/.config/materialize/antithesis-license-key`). Either runs the workload for
`--duration` seconds, then the eventually, and lists the assertions a run
would fail, including those never reached. Nothing injects faults locally.
A kind run keeps a second copy of every image in the node, so check free disk
space first and `kind delete cluster --name mz-antithesis-driver` afterwards.

## Layout

| Path | What |
|---|---|
| `manifests/` | Kubernetes manifest templates and the orchestratord Helm values |
| `driver/` | mzbuild image `antithesis-driver`: the driver and its `first_start_workload` command |
| `config/manifests/` | Rendered manifests (gitignored: they embed the license key) |
| `misc/python/materialize/antithesis_driver/` | The driver's state machine, components and local loop |
| `misc/python/materialize/antithesis/` | Rendering, images, SQL error classification, and the pure models the checks use |
| `misc/python/materialize/orchestratord/` | Kubernetes client for an orchestratord-managed environment |

The Materialize CR is not in the manifests. orchestratord registers its CRD at
startup, so the driver creates the CR, waits for the first generation to serve
SQL, and then reports `setup_complete`.

## Instrumentation

The Antithesis build flavor (`bin/mzimage ... --antithesis`, or
`CI_ANTITHESIS=1`) builds with sancov coverage flags and the
`mz-ore/antithesis` feature, which links the Antithesis SDK and reports:

- every process panic as the failed property `Materialize process panicked`,
  except panics on the reviewed `DESIGNED_PANICS` list
  (`mz_ore::antithesis::classify_panic`), which are a structured observation;
- every `halt!` as a per-site `process halted at <file>:<line>` event, plus a
  verdict from `mz_ore::antithesis::classify_halt`: a structured observation
  for messages on the reviewed `DESIGNED_HALTS` list, a dedicated failed
  property for halts of filed bugs, and `Materialize process
  halted for an unexpected reason` for everything else;
- a designed halt as exit code 143 in place of 166
  (`mz_ore::process::halt_exit_code`), so the platform's `No unexpected
  container exits`, which skips 0, 137 and 143, fails only for the other
  halts. A designed panic exits 143 too in place of the abort
  (`mz_ore::process::exit_designed_panic`), which the platform's `No
  unexpected crashes` would report. Both leave a record in the termination
  message, by which the driver's crash-loop check tells them apart from a
  SIGTERM. The upgrade base gets the same exit codes from its build patch
  (`images.upgrade_base_exit_code`);
- every soft assertion that logs instead of panicking as
  `Materialize soft assertion failed`;
- optional-hit safety assertions placed with `mz_ore::antithesis_always!` and
  `mz_ore::antithesis_unreachable!`, and coverage guidance from
  `mz_ore::antithesis_{sometimes,reachable}!`, which marks rare branches for
  exploration without becoming a property.

Without the feature all of these compile to nothing. The driver's own Python
code is cataloged and coverage-instrumented from `/opt/antithesis/catalog/` in
its image.

## Memory

The Antithesis VM has about 10 GiB, and every pod shares it, including two
environmentd generations and two sets of clusterd replicas during a rollout.
environmentd is limited to 2 GiB (`environmentdResourceRequirements` in the
CR), clusterd to 1 GiB per container (the `clusterd-memory` LimitRange, since
replica sizes carry no memory limit), orchestratord to 512 MiB, and SQL Server
caps itself at 2 GiB (`MSSQL_MEMORY_LIMIT_MB`). The other dependencies and the
driver have no limit.

The platform's `Peak memory usage` fails above 95% and names no process. While
the node is above 85%, the driver logs the working set of each pod every 5 s
(`antithesis_driver/memory.py`), so the logs before a failing sample show who
held the memory.

A kill for memory exits like a crash. A panic reports `Materialize process
panicked` from inside the process. An OOM kill or an eviction is a SIGKILL
(137) with no panic message, which the platform's `No unexpected crashes` and
`No unexpected container exits` skip. The platform's `Peak memory usage` fails
above 95% of the VM's memory, and its examples list the largest processes.

## Cross-version upgrades

Rendered with `--upgrade-from-environmentd-image` and
`--upgrade-from-clusterd-image`, the environment starts on an older release,
and half the driver's rollouts move it to the image under test. Once the spec
names the new image, later rollouts keep it.

The base release is the newest release of the previous minor version
(`images.upgrade_base_version`). The published release images seed AWS-LC from
CPU jitter, which aborts on Antithesis's deterministic CPU, so CI rebuilds
environmentd and clusterd from the release tag without jitter entropy and pushes
them as `<release>--antithesis.upgrade-base`. They have no coverage
instrumentation. Every image tag starts with its version
(`images.image_tag`), because orchestratord reads it to gate environmentd
arguments and to refuse rollbacks.

## Submitting to Antithesis

CI builds the images on PRs labeled `ci-antithesis` (see `ci/README.md`) and
pushes them to the Antithesis registry. The step requires `MZ_CI_LICENSE_KEY`
to render and publish the config image against those exact references.
The step's `antithesis-images.json` artifact lists the images, upgrade base
images, and a `config` reference pinned to its pushed digest. Download the
artifact and launch without a local registry login, excluding the driver from
faults:

```
snouty launch --launcher basic_k8s_test \
    --config-image "$(jq -r '.config' antithesis-images.json)" \
    --param "antithesis.images=$(jq -r 'del(.config, ."upgrade-base-version") | [.[]] | join(";")' antithesis-images.json)" \
    --fault-tolerance high_availability --fault-exclusions '^driver$' \
    --max-compute-hours <hours> ...
```

To render and validate the manifests locally:

```
bin/pyactivate -m materialize.antithesis.render --output test/antithesis/config/manifests \
    --environmentd-image <environmentd> --clusterd-image <clusterd> \
    --orchestratord-image <orchestratord> --workload-image <antithesis-driver> \
    [--upgrade-from-environmentd-image <upgrade-base-environmentd> \
     --upgrade-from-clusterd-image <upgrade-base-clusterd>]
snouty validate test/antithesis/config
snouty launch --launcher basic_k8s_test --config test/antithesis/config \
    --fault-tolerance high_availability --fault-exclusions '^driver$' ...
```

snouty builds the config image (`FROM scratch` with the rendered manifests under
`/manifests`) and pushes it to `ANTITHESIS_REPOSITORY`. environmentd will not
start under Kubernetes without a license key, and Antithesis runs air-gapped,
so the key is baked into the rendered `materialize-backend` Secret. The config
image must therefore only ever go to the tenant's private Antithesis registry,
never to GHCR.
