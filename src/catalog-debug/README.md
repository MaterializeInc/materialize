# `catalog-debug`

This tool allows us to debug an individual customer's catalog. It connects directly to the backing
durable store, so you will need a Cloud Admin to help you.

> **Note**: If you plan on running `catalog-debug` more than once or on a very large catalog, we'd
recommend compiling it in release mode, e.g. `cargo run --release -- <args>`.

### `edit` and `delete`

These commands use cooperative compare-and-append (CAS), without an exclusive open or
automatic promotion. By default, they refuse to modify a catalog when client
heartbeats or recent publication indicate a live environment, and explain the
reason. Registered client incarnations are conservatively treated as live because
heartbeats are counters, not wall-clock leases. The check treats catalog
publications within five minutes as recent and runs
inside persist at the CAS snapshot boundary, including after contention retries.
Compaction can advance recorded timestamps, so the check can conservatively refuse
an edit after writers have stopped.

Pass `--force` to `edit` or `delete` to override this advisory liveness safety check.
It does not fence live writers or promote the debug client. Live writers apply
foreign changes or halt and rebuild if they cannot apply them. The check is
advisory, not a guarantee that the environment is stopped.

### `upgrade-check`

To use the `upgrade-check` command you'll need to provide a mapping from cluster replica sizes to
resource specification. To get the latest mapping check the [Pulumi config in the Cloud Repo](https://github.com/MaterializeInc/cloud/blob/main/Pulumi.production.yaml), search for "cluster_replica_sizes".

A nice oneliner for converting from YAML to JSON is:
```
python -c 'import sys,json,yaml; print(json.dumps(yaml.safe_load(sys.stdin.read())))'
```
