Creating the tables from sources starts the
[snapshotting](/ingest-data/#snapshotting) process. Snapshotting syncs the
currently available data into Materialize. Because the initial snapshot is
persisted in the storage layer atomically (i.e., at the same ingestion
timestamp), you are not able to query the table until snapshotting is complete.
