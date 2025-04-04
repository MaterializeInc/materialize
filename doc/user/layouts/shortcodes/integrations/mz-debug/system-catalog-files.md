#### System Catalog Files
Stored under the `system-catalog` sub-directory as `*.csv` files. Contains:
- Core catalog object definitions
- Cluster and compute-related information
- Data freshness and frontier metric
- Source and sink metrics
- Per-replica introspection metrics (under `{cluster_name}/{replica_name}/*.csv`) 

For more information about each relation, view the [system catalog](/sql/system-catalog/)