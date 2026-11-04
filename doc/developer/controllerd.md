# Local controllerd

`bin/controllerd` builds controllerd, adapterd, clusterd and pubsubd through
the developer launcher. Local demos use the default development build. The
launcher does not create SQL objects or adapter declarations. Controllerd
initializes the default adapter only for a fresh catalog.

M3 targets this local-process workflow, including version upgrades. Kubernetes
backend and operator integration are deferred beyond M3.

Controllerd provisions an environment-scoped pubsubd from static configuration and
includes it in `/api/inventory`. Ordinary startup needs no separate pubsub command.
The relay is shared by adapters and replicas and survives controller termination.

## Prerequisites and startup

Use a local CockroachDB configured as in [the developer guide](guide.md#cockroachdb),
with a user allowed to create databases and schemas. The default metadata URL is
`postgres://root@localhost:26257/materialize_controllerd`, **not** environmentd's
`materialize` database. Alternatively, create a dedicated PostgreSQL database
first and pass `--postgres postgres://USER@localhost/materialize_controllerd`.
PostgreSQL's default 100-connection limit can be too small for the per-process
pools. Size `max_connections` and server memory for all participating controllers,
adapters and replicas.
`MZDEV_POSTGRES` and `MZDEV_BLOB` are honored, so check these variables before
starting. A fresh state directory rejects existing metadata schemas rather than
converting an environmentd environment.

State defaults to `mzdata-controllerd/` in the checkout, including the environment
ID, blobs, secrets and scratch directory. This default directory is Git-ignored.
Keep custom state directories private and outside the checkout. Override it with `--controller-data-directory /absolute/path`; use a
separate metadata database and blob location for each environment. Do not reuse
another environment's state or run two controllers for the same local deployment.

Deleting this directory does not reset the metadata database. Do not recreate only
its environment ID: the default blob data and secrets were also stored there.
For a fresh default environment, use the destructive `bin/controllerd --reset`
command described below. For custom targets, use an unused database and state
directory. If the original directory still exists, select it with
`--controller-data-directory` to resume that environment without resetting.

Start the environment:

```sh
bin/controllerd
```

Use `--optimized` for an optimized build, or `--build-only` to build without
starting services. The process backend tries the configured relay ports and falls
back to available ports. The controller uses the assigned endpoint for its own
Persist client and every managed adapter and replica. Inspect `/api/inventory`
for the relay's assigned endpoints and observed process status.

For version coexistence, keep each version's `controllerd`, `adapterd`, `clusterd`
and `pubsubd` executables in a separate, fixed directory. Select it with
`--controller-binaries-directory /absolute/path/to/version`. This skips building
and leaves the shared `mzbuild/bin` symlinks unchanged. It cannot be combined with
`--build-only`. Keep relay selection independent of these deployment directories,
using one provisioner and an explicit fixed `--pubsubd-image` or external relay.

For an existing, environment-scoped external relay, pass
`--pubsub-url http://HOST:PORT`. The launcher does not probe, adopt, start or stop
that service. Relay unavailability does not gate SQL startup: Persist clients use
the durable fallback. Keep a single provisioner during deployment overlap. Other
controllers connect using the provisioner's discovered relay URL. Transfer
provisioning ownership only after stopping the previous provisioner, using the
same environment identity and backend namespace/prefix for retained adoption.
The relay executable/image and resource configuration must stay environment-scoped,
independent of the deployment release.

The managed relay defaults to one CPU and 512 MiB memory. Raw controller options
`--pubsub-cpu-limit`, `--pubsub-memory-limit`, `--pubsub-http-port` and
`--pubsubd-image` configure it. Forward these through the launcher after `--`.
For the local managed relay, changing the configured binary or rebuilding it at
the same path replaces only pubsubd and retains its assigned endpoints. Unchanged
controller restarts adopt it. Argument/resource changes alone do not restart local
processes. Adapters and replicas retain their existing adoption behavior.
Processes without a matching executable-identity record are adopted conservatively
until they exit, rather than replaced from an unverified configuration.

The default adapter size `1` declares one CPU and 4 GiB memory. Override the map
with `--adapter-sizes '{"1":{"cpu_limit":2,"memory_limit":"8 GiB"}}'`.

On hosts booted with systemd, the process backend uses user scopes with
`MemoryMax` and `CPUQuota`, requiring a usable user manager. Otherwise it launches
processes directly without enforcing these allocations. Inventory reports declared
resources, not measured limits or usage.

## SQL and HTTP

```sh
curl -fsS http://127.0.0.1:6882/api/adapters
curl -fsS http://127.0.0.1:6882/api/inventory
psql 'postgres://materialize@127.0.0.1:6875/materialize'
```

The default adapter tries configured SQL port 6875. If it is occupied, use the
backend-assigned external SQL endpoint in `/api/adapters`, also for additional
adapters. Wait for readiness before connecting. Assigned endpoints alone do not
mean an adapter is ready. Do not assume all adapters share a fixed SQL/HTTP port.
Both `/api/adapters` and `/api/inventory` label adapter endpoints by protocol and
configured listener name, such as `sql-external`, `sql-internal` and `http-external`.
Custom listener names are preserved rather than replaced by backend port numbers.
Controller HTTP is unauthenticated and binds to loopback on port 6882. Override
it with `-- --controller-http-listen-addr=127.0.0.1:PORT`. Adapter listeners use
the existing no-auth developer configuration, which permits non-loopback access.
Use only on a trusted development host or supply `--listeners-config-path`.

```sh
curl -fsS -X POST http://127.0.0.1:6882/api/adapters \
  -H 'Content-Type: application/json' -d '{"name":"extra","size":"1"}'
# Replace aN with the ID returned by the inventory. This drops that adapter.
curl -fsS -X DELETE http://127.0.0.1:6882/api/adapters/aN
```

## Stop and restart without reset

Press Ctrl-C in the **controller launcher terminal**. Managed pubsub keeps running.
The launcher signals only controllerd, not its children. Run the same
`bin/controllerd` command again with the same state directory,
metadata/blob URLs, pubsub URL and listener/size configuration. Controllerd
adopts healthy children through the process backend. This is a controller stop,
not a shutdown of the whole environment. Do not use process-group kills.

To stop the whole local environment without resetting it:

```sh
bin/controllerd --stop
```

Use the same `--controller-data-directory` as startup when overriding the default.
This stops that environment's controllers first, then its managed adapters,
replicas and pubsubd. It works after Ctrl-C or controller failure, does not build
anything, and is safe to repeat. Other environments and external services stay
running. Data, metadata and catalog declarations are preserved. Start again with
`bin/controllerd` and the same configuration to resume, without resetting state.

The state directory records the backend's process-metadata location. Later starts
reuse that location even if the shell's `TMPDIR` changes. Stop validates PID start
times before signaling retained processes and preserves their metadata files. If
some identity records are unusable, it stops verified processes first, then reports
that it could not verify a complete shutdown. Do not launch another controller for
this environment while `--stop` is running.

To discard the default local environment and start fresh:

```sh
bin/controllerd --reset
```

This is destructive. It stops identifiable processes associated with the default
state directory, drops the `consensus`, `tsoracle` and `storage` schemas in
`materialize_controllerd`, and deletes `mzdata-controllerd/` before starting again.
It also works after the directory was deleted, recovering process identities from
surviving adapters or replicas. It refuses custom database/blob/state settings and
refuses to reset while another worktree uses the same metadata database. External
relays are not adopted or stopped. Do not launch another controller during reset.

An empty adapter inventory is valid after a normal restart. Use HTTP POST to create
an adapter and resume SQL without reinitializing the catalog.

HTTP names are exact, like quoted SQL identifiers. Creating `{"name":"Foo",
"size":"1"}` requires `DROP ADAPTER "Foo"` in SQL. Invalid input returns 400,
unknown IDs return 404, and duplicate names or exhausted catalog-conflict retries
return 409. Retry a catalog-conflict response after refreshing the inventory.
SQL CREATE, ALTER and DROP require a superuser even with RBAC disabled.
Catalog listing follows the normal public metadata privileges.

`mz_catalog.mz_adapters` lists declarations. `mz_internal.mz_adapter_endpoints`
and `mz_internal.mz_adapter_statuses` expose observations for each deployment
generation. HTTP inventory includes the same readiness observations. Backend
`online` is process status, not SQL readiness. Readiness uses an existing
unauthenticated `/api/readyz` route, without changing listener policy or bypassing
TLS checks. It is unknown when no such endpoint can be observed. Change timestamps
are not heartbeats, and neither assigned endpoints nor past readiness guarantee a
future connection.

A restarted controller re-probes readiness rather than attaching the prior
controller's sample to an unverified process. Readiness can briefly become unknown
while existing SQL sessions and committed processes continue running.


## Promote a compatible deployment

Native prototype deployments can serve SQL, DDL and table writes before promotion.
Both builds must understand the same complete committed-plan encoding. This does
not establish compatibility between arbitrary releases.

Promotion uses the candidate adapter's internal HTTP port, not controllerd's port.
For the default adapter `a1`:

```sh
candidate_controller=http://127.0.0.1:6883
candidate_http=$(curl -fsS "$candidate_controller/api/adapters" |
  jq -r '.[] | select(.id == "a1") | .endpoints["http-internal"][0]')
curl -fsS "http://$candidate_http/api/leader/status"
```

Wait for `ReadyToPromote`. SQL readiness alone does not establish upgrade
readiness. The candidate must also satisfy the configured catch-up and replica
stability requirements. Do not skip catch-up when verifying an upgrade.

```sh
curl -fsS -X POST "http://$candidate_http/api/leader/promote"
curl -fsS "$candidate_controller/api/inventory" |
  jq '{generation, active_generation}'
curl -fsS "$candidate_controller/api/adapters"
```

The POST acknowledges the request, not completed promotion. Wait until inventory
reports the candidate as active and its adapter is SQL-ready again. Promotion
restarts the candidate adapter, so reconnect its SQL sessions. It must preserve
warmed replicas and leave the older admitted deployment writable. Retirement is
a separate operation.

## Retire one deployment

Inventory distinguishes this controller's `generation` from the catalog's
`active_generation`. Promotion changes active execution authority, not SQL write
permission for other admitted deployments.

Promotion does not retire the previous deployment. To retire generation 1 through
an active controller listening on port 6883, then stop that generation's local
processes:

```sh
bin/controllerd --retire-generation 1 --controller-url http://127.0.0.1:6883
```

Use the same `--controller-data-directory` as startup. The launcher sends the
selected environment ID to `POST /api/deployments/1/retire`. Controllerd rejects a
mismatched environment or an attempt to retire the active generation. A successful
response acknowledges durable retirement, not completed process cleanup.

After verifying that receipt, the launcher stops the retired generation's
controller, adapters and replicas. It does not build, reset data, stop surviving
generations, or stop the shared relay. Cleanup also works when the retired
controller has already exited. Retry the same command after a partial cleanup.
An unconfirmed retirement stops no local processes.

Transfer relay provisioning to a surviving controller before retiring its current
provisioner, following the shared-relay rules above. Retained pubsub keeps running,
but an external-mode controller does not automatically become its provisioner.
Do not use `--stop` for a single deployment: it stops the whole environment.

## Inspect Persist collections

`GET /api/collections` returns pretty-printed JSON for catalog-known Persist-backed
collections, including system collections. Each entry has an `id`, a qualified
`name` (null when unnamed), and the committed combined collection `since` array
(null when unavailable). This catalog compaction frontier is constrained by read,
recovery and retention requirements. It is not physical Persist since, which can
lag. An empty array is an empty frontier, not an unavailable observation.

```sh
curl -fsS http://127.0.0.1:6882/api/collections
curl -fsSG http://127.0.0.1:6882/api/collections \
  --data-urlencode 'id=s%' \
  --data-urlencode 'name=%mz\_catalog.%'
```

The equivalent browser URL is:

```text
http://127.0.0.1:6882/api/collections?id=s%25&name=%25mz%5C_catalog.%25
```

Both optional filters use case-sensitive SQL LIKE patterns: `%` matches any
sequence, `_` matches one character, and backslash escapes a literal character.
When both are supplied, both must match. A name filter excludes unnamed entries.
Filters apply to the committed catalog inventory.

Inspection works without a serving adapter and reads only the catalog. It does
not acquire read holds, inspect Persist shards, or change compaction. Missing
catalog frontiers remain null rather than falling back to physical shard state.
