# Local controllerd

`bin/controllerd` builds controllerd, adapterd, clusterd and pubsubd through
the developer launcher. Local demos use the default development build. The
launcher does not create SQL objects or adapter declarations. Controllerd
initializes the default adapter only for a fresh catalog.

Controllerd provisions an environment-scoped pubsubd from static configuration and
includes it in `/api/inventory`. Ordinary startup needs no separate pubsub command.
The relay is shared by adapters and replicas and survives controller termination.

## Prerequisites and startup

Use a local CockroachDB configured as in [the developer guide](guide.md#cockroachdb),
with a user allowed to create databases and schemas. The default metadata URL is
`postgres://root@localhost:26257/materialize_controllerd`, **not** environmentd's
`materialize` database. Alternatively, create a dedicated PostgreSQL database
first and pass `--postgres postgres://USER@localhost/materialize_controllerd`.
`MZDEV_POSTGRES` and `MZDEV_BLOB` are honored, so check these variables before
starting. A fresh state directory rejects existing metadata schemas rather than
converting an environmentd environment.

State defaults to `mzdata-controllerd/` in the checkout, including the environment
ID, blobs, secrets and scratch directory. This default directory is Git-ignored.
Keep custom state directories private and outside the checkout. Override it with `--controller-data-directory /absolute/path`; use a
separate metadata database and blob location for each environment. Do not reuse
another environment's state or run two controllers for the same local deployment.

Start the environment:

```sh
bin/controllerd
```

Use `--optimized` for an optimized build, or `--build-only` to build without
starting services. The process backend tries the configured relay ports and falls
back to available ports. The controller uses the assigned endpoint for its own
Persist client and every managed adapter and replica. Inspect `/api/inventory`
for the relay's assigned endpoints and observed process status.

For an existing, environment-scoped external relay, pass
`--pubsub-url http://HOST:PORT`. The launcher does not probe, adopt, start or stop
that service. Relay unavailability does not gate SQL startup: Persist clients use
the durable fallback. Keep a single provisioner during deployment overlap. Other
controllers connect using the provisioner's discovered relay URL. Transfer
provisioning ownership only after stopping the previous provisioner, using the
same environment identity and backend namespace/prefix for retained adoption.
The relay executable/image and resource configuration must stay environment-scoped,
independent of the deployment release. Kubernetes deployments use an image with
pubsubd as its entry point, such as the repository's `pubsubd` image definition.

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

`--reset` is rejected. Do not delete the state directory or reset metadata while
children are running. An empty adapter inventory is valid after restart. Use
HTTP POST to create an adapter and resume SQL without reinitializing the catalog.

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
