### System catalog files

`mz-debug` outputs system catalog files if
[`--dump-system-catalog`](#dump-system-catalog) is `true` (the default).

The generated files are in `system-catalog` sub-directory as `*.csv` files and
contains:

- Core catalog object definitions
- Cluster and compute-related information
- Data freshness and frontier metric
- Source and sink metrics
- Per-replica introspection metrics (under `{cluster_name}/{replica_name}/*.csv`)

For more information about each relation, view the [system
catalog](/sql/system-catalog/).
