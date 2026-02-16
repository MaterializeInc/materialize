# Source Progress

- https://github.com/MaterializeInc/console/issues/1197

## The Problem

Today, when a user creates a source in the console, they have no way answer the
following questions without writing sql:

1. Is my source ready to query?
2. Is my source caught up to the upsteam datasource?

## Success Criteria

A user should be able to see details for a given source, including:

- Initial snapshot complete
- Messages received
- Bytes received
- Updates received
- Updates committed
- CPU usage
- Memory usage

Basically we want to expose the values in `mz_source_statistics` to users.

## Out of Scope

Currently, we don't have a way to get the total size of a source in bytes or
rows, so we are ignoring that until the data are available in Materialize.

## Solution Proposal

Parker H has already designed a page that will expose this information. The
focus of this document is the details of how we get the data on that page.

### useSources

We will update the existing query to retrieve the source statistics in
conjunction with the status. If the source is running and has no snapshot
committed, we will display a "Snapshotting" `StatusPill`.

```sql
SELECT
    s.id,
    s.name,
    s.type,
    s.size,
    sc.name AS "schemaName",
    d.name AS "databaseName",
    status.status,
    status.error,
    stats.snapshot_committed,
    owners."isOwner"
FROM
    mz_catalog.mz_sources AS s
        JOIN mz_catalog.mz_schemas AS sc ON sc.id = s.schema_id
        JOIN mz_catalog.mz_databases AS d ON d.id = sc.database_id
        JOIN
            (
                    SELECT
                        r.id,
                        (mz_is_superuser() OR has_role(current_user, r.oid, 'USAGE'))
                            AS "isOwner"
                    FROM mz_catalog.mz_roles AS r
                )
                AS owners
            ON owners.id = s.owner_id
        LEFT JOIN mz_internal.mz_source_statuses AS status ON status.id = s.id
        LEFT JOIN mz_internal.mz_source_statistics AS stats ON stats.id = s.id
WHERE s.id LIKE 'u%' AND s.type <> 'progress' AND s.type <> 'subsource'
ORDER BY d.name, sc.name, name;
```

### useSourceStatistics

This will be a new hook that subscribes to the source statistics and bins the
data for display in graphs, very similar to `useClusterUtilization`. Gus has
committed to enabling 30 day retention on this table, so we will be able to set
an appropriate AS OF to fetch historical data. Because these metrics are per
worker, we have to sum the values across all workers.

```sql
SUBSCRIBE (
    WITH
        subsources AS
        (
            SELECT od.referenced_object_id AS id, s.id AS "sourceId"
            FROM
                mz_catalog.mz_sources AS s
                    JOIN mz_internal.mz_object_dependencies AS od ON object_id = s.id
            WHERE s.id = $1 AND s.type <> $2
        ),
        sources AS
        (
            SELECT s.id, s.id AS "sourceId"
            FROM mz_catalog.mz_sources AS s
            WHERE s.id = $3
        ),
        combined_sources AS
        (
            SELECT id, "sourceId" FROM subsources
            UNION ALL SELECT id, "sourceId" FROM sources
        )
    SELECT
        s."sourceId" AS id,
        sum(messages_received) AS "messagesReceived",
        sum(bytes_received) AS "bytesReceived",
        sum(updates_staged) AS "updatesStaged",
        sum(updates_committed) AS "updatesCommitted",
        max(offset_known) AS "offsetKnown",
        max(offset_committed) AS "offsetCommitted",
        CASE
                WHEN bool_or(rehydration_latency IS NULL) THEN NULL
                    ELSE max(rehydration_latency)
            END
            AS "rehydrationLatency"
    FROM
        combined_sources AS s
            JOIN mz_internal.mz_source_statistics AS ss ON ss.id = s.id
    GROUP BY s."sourceId"
)
WITH (PROGRESS)
AS OF AT LEAST '2024-03-12T22:10:51.432Z'::timestamp
ENVELOPE UPSERT (KEY (id));
```

The timestamp will be calculated on the client: `now - selectedTimePeriod`.

The results we get back are cumulative values since the last restart, so we
will want to calculate a rate.

```typescript
function rateFromCumulativeValues(data: CumulativeValues[]): IngestionRate[] {
  const rates: IngestionRate[] = [];
  for (let i = 1; i < data.length; i++) {
    const prev = data[i - 1];
    const curr = data[i];
    const timeDiff = curr.timestamp.getTime() - prev.timestamp.getTime();
    const bytesPerSecond =
      (curr.bytesReceived - prev.bytesReceived) / (timeDiff / 1000);
    rates.push({
      id: curr.id,
      timestamp: curr.timestamp,
      bytesPerSecond,
    });
  }

  return rates;
}
```

### useClusterReplicaMetrics

A new hook to fetch cluster replica metrics. There is an open question around
which replica to pick when there are multiple, but this proposal is for showing
the metrics from the largest replica. Since the design calls for displaying
absolute values, we have to look up the replica capacity, then we can work
backwards to figure out the approximate e.g. memory usage based on the
total and the percentage.

```sql
SELECT
    cr.id,
    cr.name,
    cr.size,
    crs.cpu_nano_cores * crs.processes AS "cpuNanoCores",
    crs.memory_bytes * crs.processes AS "memoryBytes",
    crs.disk_bytes * crs.processes AS "diskBytes",
    cru.cpu_percent AS "cpuPercent",
    cru.memory_percent AS "memoryPercent",
    cru.disk_percent AS "diskPercent"
FROM
    mz_catalog.mz_cluster_replicas AS cr
        JOIN mz_internal.mz_cluster_replica_sizes AS crs ON crs.size = cr.size
        JOIN
            (
                    SELECT
                        replica_id,
                        max(cru.cpu_percent) AS cpu_percent,
                        max(cru.memory_percent) AS memory_percent,
                        max(cru.disk_percent) AS disk_percent
                    FROM mz_internal.mz_cluster_replica_utilization AS cru
                    GROUP BY replica_id
                )
                AS cru
            ON cr.id = cru.replica_id
WHERE cr.cluster_id = $1
ORDER BY cr.id;
```

### useCurrentSourceStatistics

A new hook that returns the most recent statistics. We will also get this data
from the subscribe for the graphs, but in the interest of component boundaries,
We will fetch this data in the component that needs it. It will use the same
select as `useSourceStatistics`, except without subscribe.

### useSourceSize

A new hook that returns the total size of a source. This data is only updated
once an hour, so we should indicate this to the user somehow.

```sql
SELECT object_id, sum(size_bytes), max(collection_timestamp)
FROM mz_storage_usage
WHERE object_id = 'u2080'
GROUP BY object_id;
```

### New components

The root route in `SourceDetail` currently redirects to the errors route, this
will instead render a new `SourceOverview` component, similar to how the
clusters components are structured. This component will be responsible for
fetching the data using the hooks described above as well as rendering loading
and error states. The exact design of the graph components is TBD, but ideally
we can use a single `SourceStatisticsGraph` component that accepts a dataKey and
title, as well as the data returned from the `useSourceStatistics` hook.

### Test plan

An RTL test suite, similar to the `ClusterOverview.test.tsx` will verify that
given mock data, the components render the appropriate loading, error and
success states. We won't try to validate the graphs beyond verifying that we
rendered something that doesn't appear to be an error state.

## Minimal Viable Prototype

[Figma design](https://www.figma.com/file/kZfWkyc9cxlVz7RYFxHhjt/Console-Q423?type=design&node-id=550-1618&mode=design&t=hRjygFRofhlMEJ2W-4)

## Alternatives

I didn't seriously entertain any alternative designs, happy to hear suggestions
if I've missed a better approach.

## Open questions

### Cluster metrics

If a source cluster has multiple replicas, which metrics should we display? For
now, since sources can only have a single replica, we chose to just use the
first replica returned.

### Data preview

We've decided this is out of scope, since there is no safe way to query this
data.
