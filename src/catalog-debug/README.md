# `catalog-debug`

This tool allows us to debug an individual customer's catalog. It connects directly to the backing
durable store, so you will need a Cloud Admin to help you.

> **Note**: If you plan on running `catalog-debug` more than once or on a very large catalog, we'd
recommend compiling it in release mode, e.g. `cargo run --release -- <args>`.

### `upgrade-check`

To use the `upgrade-check` command you'll need to provide a mapping from cluster replica sizes to
resource specification. To get the latest mapping check the [Pulumi config in the Cloud Repo](https://github.com/MaterializeInc/cloud/blob/main/Pulumi.production.yaml), search for "cluster_replica_sizes".

A nice oneliner for converting from YAML to JSON is:
```
python -c 'import sys,json,yaml; print(json.dumps(yaml.safe_load(sys.stdin.read())))'
```
