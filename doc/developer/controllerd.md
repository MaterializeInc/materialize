# Local controllerd

`bin/controllerd --optimized` builds controllerd, adapterd, clusterd and pubsubd
through the developer launcher. It connects to an independently running pubsubd.
The launcher does not create SQL objects or adapter declarations. Controllerd
initializes the default adapter only for a fresh catalog.

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

Build once, then start pubsub in a separate terminal:

```sh
bin/controllerd --optimized --build-only
target/optimized/pubsubd --listen-addr=127.0.0.1:6879 \
  --internal-http-listen-addr=127.0.0.1:6881
```

The HTTP override avoids pubsubd's default port colliding with controllerd's
HTTP port. Leave this terminal running across controller restarts. If these
ports are occupied, choose free ports rather than killing other services.

In another terminal:

```sh
bin/controllerd --optimized
```

For an existing, environment-scoped external relay, pass
`--pubsub-url http://HOST:PORT` instead. The launcher checks TCP reachability,
not gRPC readiness or service identity. It never starts, stops or replaces this
service. Pubsub must remain available to the retained adapters and replicas.
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
Controller HTTP is unauthenticated and binds to loopback on port 6880. Override
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

Press Ctrl-C in the **controller launcher terminal**, leaving pubsub running.
The launcher signals only controllerd, not its children. Run the same
`bin/controllerd --optimized` command again with the same state directory,
metadata/blob URLs, pubsub URL and listener/size configuration. Controllerd
adopts healthy children through the process backend. This is a controller stop,
not a shutdown of the whole environment. Do not use process-group kills.

`--reset` is rejected. Do not delete the state directory or reset metadata while
children are running. An empty adapter inventory is valid after restart. Use
HTTP POST to create an adapter and resume SQL without reinitializing the catalog.

HTTP names are exact, like quoted SQL identifiers. Creating `{"name":"Foo",
"size":"1"}` requires `DROP ADAPTER "Foo"` in SQL. Invalid input returns 400,
unknown IDs return 404, and duplicate names or exhausted catalog-conflict retries
return 409. Retry a catalog-conflict response after refreshing the inventory.
SQL CREATE, ALTER and DROP require a superuser even with RBAC disabled.
Catalog listing follows the normal public metadata privileges.
