---
title: "CREATE SINK: Iceberg"
description: "Connecting Materialize to an Apache Iceberg table sink"
menu:
  main:
    parent: 'create-sink'
    identifier: csink_iceberg
    name: Iceberg
    weight: 30
---

{{< private-preview />}}

`CREATE SINK` connects Materialize to an external system you want to write data to, and provides details about how to encode that data.

To use an Apache Iceberg table as a sink, you need to create two connections:
1. An [Iceberg catalog connection](#creating-an-iceberg-catalog-connection) that specifies how to connect to your Iceberg catalog
2. An [AWS connection](#creating-an-aws-connection) that specifies how to access S3 storage where Iceberg data files will be written

Once created, these connections are **reusable** across multiple `CREATE SINK` statements.

Sink source type      | Description
----------------------|------------
**Source**            | Pass all data received from the source to the sink.
**Table**             | Stream all changes to the specified table out to the sink.
**Materialized view** | Stream all changes to the view to the sink. This lets you use Materialize to process a stream, and then stream the processed values. Note that this feature only works with [materialized views](/sql/create-materialized-view), and _does not_ work with [non-materialized views](/sql/create-view).

## Syntax

```mzsql
CREATE SINK [IF NOT EXISTS] <sink_name>
    [IN CLUSTER <cluster_name>]
    FROM <source_or_view>
    INTO ICEBERG CATALOG CONNECTION <iceberg_catalog_conn> (
        NAMESPACE '<namespace>',
        TABLE '<table_name>'
    )
    USING AWS CONNECTION <aws_conn>
    KEY (<key_columns>...) [NOT] ENFORCED
    [FORMAT PARQUET]
    ENVELOPE UPSERT | DEBEZIUM
    WITH (
        COMMIT INTERVAL = '<interval>'
    );
```

## Limitations

{{< warning >}}
Iceberg sinks in Materialize have the following important limitations:
{{< /warning >}}

- **Schema evolution is not supported**: The Iceberg table schema must remain fixed after sink creation. Adding, removing, or modifying columns will cause the sink to fail.

- **Partition spec evolution is not supported**: Currently, all Iceberg tables created by Materialize are unpartitioned. Custom partitioning schemes are not supported.

- **Nested data types have limited support**: While simple scalar types work well, complex nested types (arrays, maps, records) may not have proper Iceberg field IDs assigned.

- **Write-only**: Materialize can only write to Iceberg tables via sinks. Reading from Iceberg tables as a source is not supported. However, you can query Iceberg tables written by Materialize using external tools like DuckDB, Spark, or Trino.

- **Feature flag required**: You must enable the `enable_iceberg_sink` feature flag to use Iceberg sinks.

## Creating an Iceberg catalog connection

Before creating an Iceberg sink, you must first create a connection to your Iceberg catalog. Materialize supports two types of Iceberg catalogs:

### REST Catalog

Use this for generic Iceberg REST catalogs like Polaris or Nessie.

```mzsql
CREATE SECRET iceberg_oauth_secret AS '<CLIENT_SECRET>';

CREATE CONNECTION iceberg_catalog TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'https://your-catalog-endpoint.com/api/catalog',
    CREDENTIAL = 'client-id:' || SECRET iceberg_oauth_secret,
    WAREHOUSE = 'my_warehouse',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
);
```

#### Options

Field | Required | Description
------|----------|------------
`CATALOG TYPE` | Yes | Must be `'REST'` for generic REST catalogs
`URL` | Yes | The HTTP(S) endpoint URL for the Iceberg REST catalog
`CREDENTIAL` | Yes | OAuth2 credentials in `CLIENT_ID:CLIENT_SECRET` format. The secret can be provided inline or via a secret reference.
`WAREHOUSE` | No | The warehouse name within the catalog
`SCOPE` | No | OAuth2 scope for authentication. Default: `'PRINCIPAL_ROLE:ALL'`

### S3 Tables REST Catalog

Use this for AWS S3 Tables, which provides a managed Iceberg catalog service.

```mzsql
CREATE SECRET aws_secret AS '<AWS_SECRET_ACCESS_KEY>';

CREATE CONNECTION aws_conn TO AWS (
    ACCESS KEY ID = '<AWS_ACCESS_KEY_ID>',
    SECRET ACCESS KEY = SECRET aws_secret,
    REGION = 'us-east-1'
);

CREATE CONNECTION s3_tables_catalog TO ICEBERG CATALOG (
    CATALOG TYPE = 'S3TABLESREST',
    URL = 'https://s3tables.us-east-1.amazonaws.com',
    AWS CONNECTION = aws_conn,
    WAREHOUSE = 'my-s3-table-bucket'
);
```

#### Options

Field | Required | Description
------|----------|------------
`CATALOG TYPE` | Yes | Must be `'S3TABLESREST'` for AWS S3 Tables
`URL` | Yes | The AWS S3 Tables REST catalog endpoint URL
`AWS CONNECTION` | Yes | Reference to an AWS connection for authentication
`WAREHOUSE` | Yes | The S3 Tables warehouse (bucket) name

## Creating an AWS connection

Iceberg sinks require an AWS connection to write data files to S3 storage. The connection can use either role assumption or static credentials.

{{< tabs >}}
{{< tab "Role assumption (Recommended)">}}

```mzsql
CREATE CONNECTION aws_role_assumption TO AWS (
    ASSUME ROLE ARN = 'arn:aws:iam::123456789012:role/MaterializeIcebergRole',
    REGION = 'us-east-1'
);
```

See [AWS connection permissions](/sql/create-connection/#aws-permissions) for details on configuring the IAM role trust policy.

{{< /tab >}}

{{< tab "Static credentials">}}

```mzsql
CREATE SECRET aws_secret AS '<AWS_SECRET_ACCESS_KEY>';

CREATE CONNECTION aws_credentials TO AWS (
    ACCESS KEY ID = '<AWS_ACCESS_KEY_ID>',
    SECRET ACCESS KEY = SECRET aws_secret,
    REGION = 'us-east-1'
);
```

{{< /tab >}}
{{< /tabs >}}

## Creating an Iceberg sink

Once you have created the necessary connections, you can create an Iceberg sink.

### Syntax details

#### `INTO ICEBERG CATALOG CONNECTION`

Specifies the Iceberg catalog connection and table location.

Option | Required | Description
-------|----------|------------
`NAMESPACE` | Yes | The Iceberg namespace (database) where the table will be created or accessed. In Polaris, this might be `'database1'`. In S3 Tables, this is the namespace within your bucket.
`TABLE` | Yes | The name of the Iceberg table to write to. If the table does not exist, Materialize will create it.

#### `USING AWS CONNECTION`

Specifies the AWS connection to use for writing Parquet data files to S3 storage.

#### `KEY`

Specifies the columns that uniquely identify each row. This is required for Iceberg sinks to properly handle updates and deletes using equality deletes.

- `KEY (...) ENFORCED`: Materialize will validate that the specified key is a unique key of the upstream relation. If validation fails, the sink creation will fail.
- `KEY (...) NOT ENFORCED`: Disables uniqueness validation. Use this only if you have outside knowledge that the key is unique. If the key is not actually unique, the Iceberg table may contain inconsistent data.

See [Upsert key selection](/sql/create-sink/kafka/#upsert-key-selection) for details on key validation.

#### `FORMAT`

The format for encoding data. Currently, only `PARQUET` is supported (and is the default).

#### `ENVELOPE`

Determines how changes are represented in the Iceberg table.

- **`UPSERT`**: For insertion events, writes the row. For update events, uses Iceberg equality deletes to remove the old row and inserts the new row. For deletion events, uses equality deletes to remove the row.

- **`DEBEZIUM`**: Similar to upsert but preserves Debezium-style change event semantics. Uses both equality deletes and position deletes to track row changes.

#### `WITH`

Option | Required | Description
-------|----------|------------
`COMMIT INTERVAL` | Yes | The time interval at which Materialize commits batches of changes to the Iceberg table. Must be a valid [interval](/sql/types/interval/) value (e.g., `'30s'`, `'1min'`, `'5min'`). Smaller intervals provide lower latency but create more Iceberg snapshots. Larger intervals reduce snapshot overhead but increase latency.

## Features

### Upsert semantics

Iceberg sinks use Iceberg's native delete capabilities to provide full upsert semantics:

- **Insertions**: Written as new data files in Parquet format
- **Updates**: Implemented using equality deletes (removes old row based on key) combined with a new data file insert
- **Deletions**: Implemented using position deletes (removes specific row by file position and offset)

### Atomic commits

All changes within a commit interval are written atomically as a single Iceberg snapshot. If Materialize fails during a batch, the entire batch is rolled back and retried.

### Progress tracking

Materialize stores its progress in Iceberg snapshot metadata:

- `mz-frontier`: Tracks the Materialize progress frontier
- `mz-sink-id`: Unique sink identifier
- `mz-sink-version`: Sink version for fencing and conflict resolution

This allows Materialize to resume from the last successful snapshot if the sink is restarted.

### Conflict resolution

If multiple writers attempt to commit to the same Iceberg table simultaneously, Materialize automatically detects conflicts and retries the commit with the updated table state.

## Required privileges

To execute the `CREATE SINK` command, you need:

{{< include-md file="shared-content/sql-command-privileges/create-sink.md" >}}

## Required AWS IAM permissions

The AWS IAM role or user must have the following S3 permissions for the bucket where Iceberg data is stored:

- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject`
- `s3:ListBucket`

## Examples

### Basic Iceberg sink with REST catalog

```mzsql
-- Create secrets
CREATE SECRET polaris_oauth_secret AS 'your-oauth-client-secret';
CREATE SECRET aws_secret AS 'your-aws-secret-key';

-- Create connections
CREATE CONNECTION polaris_catalog TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'https://polaris.example.com/api/catalog',
    CREDENTIAL = 'client-id:' || SECRET polaris_oauth_secret,
    WAREHOUSE = 'analytics'
);

CREATE CONNECTION aws_conn TO AWS (
    ACCESS KEY ID = 'your-access-key-id',
    SECRET ACCESS KEY = SECRET aws_secret,
    REGION = 'us-east-1'
);

-- Create a materialized view
CREATE MATERIALIZED VIEW user_activity_summary AS
SELECT
    user_id,
    COUNT(*) as activity_count,
    MAX(last_seen) as last_activity
FROM user_events
GROUP BY user_id;

-- Create the Iceberg sink
CREATE SINK user_activity_iceberg
    IN CLUSTER sink_cluster
    FROM user_activity_summary
    INTO ICEBERG CATALOG CONNECTION polaris_catalog (
        NAMESPACE 'analytics_db',
        TABLE 'user_activity'
    )
    USING AWS CONNECTION aws_conn
    KEY (user_id) ENFORCED
    ENVELOPE UPSERT
    WITH (
        COMMIT INTERVAL = '1min'
    );
```

### Iceberg sink with AWS S3 Tables

```mzsql
-- Create AWS connection for S3 Tables catalog
CREATE SECRET aws_secret AS 'your-aws-secret-key';

CREATE CONNECTION aws_conn TO AWS (
    ACCESS KEY ID = 'your-access-key-id',
    SECRET ACCESS KEY = SECRET aws_secret,
    REGION = 'us-east-1'
);

-- Create S3 Tables catalog connection
CREATE CONNECTION s3_tables_catalog TO ICEBERG CATALOG (
    CATALOG TYPE = 'S3TABLESREST',
    URL = 'https://s3tables.us-east-1.amazonaws.com',
    AWS CONNECTION = aws_conn,
    WAREHOUSE = 'my-analytics-bucket'
);

-- Create sink
CREATE SINK orders_iceberg
    FROM orders_view
    INTO ICEBERG CATALOG CONNECTION s3_tables_catalog (
        NAMESPACE 'production',
        TABLE 'orders'
    )
    USING AWS CONNECTION aws_conn
    KEY (order_id) ENFORCED
    ENVELOPE UPSERT
    WITH (
        COMMIT INTERVAL = '30s'
    );
```

### Querying Iceberg tables with DuckDB

After creating an Iceberg sink, you can query the resulting Iceberg table using DuckDB or other Iceberg-compatible tools:

```sql
-- Using DuckDB CLI
INSTALL iceberg;
LOAD iceberg;

SELECT * FROM iceberg_scan('s3://your-bucket/path/to/iceberg/table');
```

## Monitoring

### Check sink status

```mzsql
SELECT
    s.name,
    s.type,
    s.cluster_id,
    ss.status,
    ss.error
FROM mz_sinks s
JOIN mz_internal.mz_sink_statuses ss ON s.id = ss.id
WHERE s.name = 'user_activity_iceberg';
```

### View Iceberg sink details

```mzsql
SELECT
    id,
    namespace,
    table
FROM mz_catalog.mz_iceberg_sinks
WHERE id = (SELECT id FROM mz_sinks WHERE name = 'user_activity_iceberg');
```

### Monitor sink progress

```mzsql
SELECT
    s.name,
    u.offset
FROM mz_sinks s
JOIN mz_internal.mz_sink_statistics ss ON s.id = ss.id;
```

## Troubleshooting

### Sink fails with "schema evolution isn't supported"

The Iceberg table schema must remain fixed after the sink is created. If you need to change the schema:

1. Drop the existing sink
2. Modify your upstream view or source to match the new schema
3. Recreate the sink (which will create a new Iceberg table or use a table with the new schema)

### Performance considerations

- **Commit interval tuning**: Smaller commit intervals (e.g., `30s`) provide lower latency but create more Iceberg snapshots and increase catalog load. Larger intervals (e.g., `5min`) reduce overhead but increase end-to-end latency.

- **Cluster sizing**: Iceberg sinks perform parallel Parquet file writes. Ensure your cluster has adequate resources for the write throughput you need.

- **Compaction**: Iceberg tables created by Materialize may accumulate many small Parquet files and delete files over time. Use Iceberg's compaction operations (via external tools like Spark) to optimize file layout.

## Related pages

- [`CREATE CONNECTION`](/sql/create-connection) - Create Iceberg catalog and AWS connections
- [`SHOW SINKS`](/sql/show-sinks) - List all sinks
- [`DROP SINK`](/sql/drop-sink) - Remove a sink
- [Iceberg integration guide](/serve-results/sink/iceberg) - Step-by-step tutorial

[`text`]: ../../types/text
[`interval`]: ../../types/interval
