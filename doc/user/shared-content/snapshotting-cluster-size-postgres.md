Snapshotting can take anywhere from a few minutes to several hours, depending on the size of your dataset,
the upstream database, the number of tables (more tables can be parallelized in Postgres), and the [size of your ingestion cluster](/sql/create-cluster/#size).

We've observed the following approximate snapshot rates from PostgreSQL:
| Cluster Size | Snapshot Rate |
|--------------|---------------|
| 25 cc | ~20 MB/s |
| 100 cc | ~50 MB/s |
| 800 cc | ~200 MB/s |
