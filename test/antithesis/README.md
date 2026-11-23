# Antithesis harness

Runs self-managed Materialize, deployed by the real orchestratord on
Kubernetes, under [Antithesis](https://antithesis.com), which injects faults
(process kills and pauses, network partitions, clock changes) into a
deterministic simulation and can replay any moment of it. The target is the
class of bugs that needs a restart or a partition at one specific point:
a process that dies between two durable writes, a read after a failover, a
dataflow that rehydrates while its inputs change.

## The workload

`driver/driver.py` is one process in a pod that is excluded from faults. It
creates the environment and its objects, reports that setup is complete, and
then runs randomly drawn rules on six threads, while one more follows the
rollouts. At a drawn moment it stops the faults, asserts that everything
recovers, and exits, which ends the timeline.

Every timeline branches from the end of setup, and most timelines of a short
run last a minute or two. A rollout needs about a minute until its new
generation is ready to promote, so setup already requests one and waits for
that point.

Nearly every check compares two relations in one `SELECT`, so at one
timestamp. Such a check holds whatever was written and whichever statements
failed, so the driver keeps no model of the data beyond the set of
acknowledged writes.

| Rules | What is asserted |
|---|---|
| `write`, `read` | A transaction inserts the same rows into two tables. A strict serializable read shows every write acknowledged before it began, and no read shows a row twice or a row nobody wrote |
| `views` | The two tables hold the same rows. A materialized view equals its query, and an indexed join of the two tables and the counters equals the materialized view. Also read on one replica alone and at serializable isolation |
| `increment`, `read_counter` | A counter updated with `UPDATE ... SET v = v + 1` stays within the increments acknowledged and sent |
| `pg_write`, `pg_read` | Upstream transactions move an amount between two tables of a Postgres source, and their sum in Materialize never changes |
| `pg_export` | A second table for the same upstream table, added to the running source, equals the first |
| `kafka_read` | A Kafka sink of the materialized view, read back through a Kafka source, never shows a state ahead of the view |
| `ddl`, `ddl_read` | Views, indexes and tables are created and dropped and the cluster is resized while everything else runs. A view created this way equals its query |
| `kill`, `kill_after_ddl` | Deletes environmentd, clusterd and orchestratord pods, also a fraction of a second after a DDL statement was sent |
| `roll` | Requests 0dt rollouts to a new generation, promotes the manual ones, deletes pods in their phases, and changes the spec while one is promoting. The active generation never decreases, and an up-to-date resource runs its spec |
| `write_to_candidate` | A generation that orchestratord has not told to promote acknowledges no write sent straight to its pod |
| Persist shard audits | Read-only `persistcli inspect` checks sample live and unmapped shards for missing blobs. A live collection must retain its shard. Each source export with data must hold its remap since below its upper |
| after faults stop | Every check above passes again, the Postgres source and the Kafka round trip catch up, every replica is online, and the Materialize resource is up to date |

A timeline draws how much DDL and how many pod deletions it gets, including
none, because a rollout completes only after about two minutes without either.
A third of the timelines also set three of the system parameters that CI
varies (`get_variable_system_parameters`) to other values.

Two properties fail by known bugs. `pg_export`: for a moment after `CREATE
TABLE ... FROM SOURCE`, the new table is ahead of the source's other tables.
"an up-to-date resource runs a spec changed while promoting": orchestratord
reports such a change as applied without deploying it (CLO-339).
`ALTER TABLE ... ADD COLUMN` is left out, because environmentd cannot boot
after it dies during that statement, which would end every timeline that hits
it.

## The build flavor

With `CI_ANTITHESIS=1` in the environment, mzbuild builds environmentd,
clusterd and orchestratord with coverage instrumentation and the
`mz-ore/antithesis` feature. With this feature, `halt!` exits with code 143.
The panic handler uses the same exit code for catalog fencing and expired
reader leases.
Every other uncaught panic still aborts and fails the run.
environmentd runs with soft assertions enabled.

## Running it

Locally, without Antithesis and without faults (also a step of the test
pipeline):

```
bin/mzcompose --find antithesis run default --duration 60
```

On Antithesis: label the PR `ci-antithesis`. The step `Build x86_64
(Antithesis)` builds the images, renders the manifests (the shared ones in
`misc/helm-charts/testing`, the operator chart with `operator-values.yaml`,
and `driver.yaml`), pushes everything to the Antithesis registry and uploads
`antithesis-images.json`. The config image contains the license key, so it
must not go to any other registry. Then:

```
snouty launch --launcher basic_k8s_test \
    --config-image "$(jq -r .config antithesis-images.json)" \
    --param "antithesis.images=$(jq -r 'del(.config) | [.[]] | join(";")' antithesis-images.json)" \
    --fault-tolerance high_availability --fault-exclusions '^driver$' \
    --duration 60m --test-name "materialize k8s harness"
```

To check the manifests without pushing anything:

```
MZ_CI_LICENSE_KEY=dummy bin/ci-builder run stable ci/test/build-antithesis.sh --render target-xcompile/antithesis-config
snouty validate target-xcompile/antithesis-config
```

A failing `always` property names its check. One that no timeline evaluated
is also reported as failing, without a counterexample. The properties that end
in "after faults stop" are evaluated only in timelines that reach the final
check.
