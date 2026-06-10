---
headless: true
---
During the snapshotting, the data ingestion for the existing tables for the same
source is temporarily blocked. As such, if possible, you can resize the cluster
to speed up the snapshotting process and once the process finishes, resize the
cluster for steady-state. You can monitor the snapshot progress on the overview
page for the source in the Materialize console.
