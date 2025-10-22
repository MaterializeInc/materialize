For SQL Server, if none of the replicating tables are receiving write queries,
snapshotting may take up to an additional 5 minutes to complete. The 5 minute
interval is due to a hardcoded interval in the SQL Server Change Data Capture
(CDC) implementation which only notifies CDC consumers every 5 minutes when no
changes are made to replicating tables.
