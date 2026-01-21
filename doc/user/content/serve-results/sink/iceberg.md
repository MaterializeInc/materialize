---
title: "Apache Iceberg"
description: "How to stream data from Materialize to Apache Iceberg tables"
menu:
  main:
    parent: 'sink'
    name: Apache Iceberg
    weight: 30
---

{{< private-preview />}}

This guide walks you through streaming data from Materialize to [Apache Iceberg](https://iceberg.apache.org/) tables using Iceberg sinks.

## Overview

[Apache Iceberg](https://iceberg.apache.org/) is an open table format for huge analytic datasets. It provides features like ACID transactions, schema evolution, time travel, and efficient incremental processing. By using an Iceberg sink, you can stream real-time results from Materialize into Iceberg tables for use with data lakes and analytics platforms.

### Why use Iceberg sinks?

Iceberg sinks enable you to:

- **Build real-time data lakes**: Stream continuously updated data from Materialize into your data lake infrastructure
- **Bridge streaming and batch analytics**: Make real-time Materialize views available to batch processing tools like Spark, Trino, or Athena
- **Enable time travel**: Leverage Iceberg's snapshot history to query data as it appeared at any point in time
- **Interoperate with modern data stacks**: Use Iceberg's broad ecosystem support to integrate Materialize with tools like DuckDB, Snowflake, Databricks, and more

### How it works

Materialize continuously evaluates changes to your sources and materialized views, then writes those changes to an Iceberg table:

1. **Change tracking**: Materialize tracks all inserts, updates, and deletes from your source data or materialized views
2. **Batching**: Changes are accumulated over a configurable commit interval (e.g., 30 seconds)
3. **Parquet writing**: At each interval, Materialize writes data files in Parquet format along with Iceberg delete files for updates/deletes
4. **Atomic commits**: All changes within an interval are committed atomically as a single Iceberg snapshot
5. **Progress tracking**: Materialize stores its progress in snapshot metadata, enabling resumption after failures

### Architecture

```
┌─────────────────┐
│  Materialize    │
│   (Sources &    │
│   Views)        │
└────────┬────────┘
         │
         │ Iceberg Sink
         ▼
┌─────────────────┐      ┌──────────────┐
│ Iceberg Catalog │◄─────┤ S3 Storage   │
│ (Metadata)      │      │ (Data Files) │
│                 │      │              │
│ - Polaris       │      │ Parquet +    │
│ - Nessie        │      │ Delete Files │
│ - AWS S3 Tables │      │              │
└─────────────────┘      └──────────────┘
         │
         │ Read Access
         ▼
┌─────────────────┐
│  Query Engines  │
│                 │
│ - DuckDB        │
│ - Spark         │
│ - Trino         │
│ - Athena        │
└─────────────────┘
```

## Before you begin

Ensure you have:

- An Iceberg catalog (REST-compatible like Polaris or Nessie, or AWS S3 Tables)
- S3-compatible object storage for Iceberg data files
- AWS credentials with S3 read/write permissions
- Materialize feature flag `enable_iceberg_sink` enabled
- A [cluster](/concepts/clusters/) to host the sink

## Step 1: Enable the Iceberg sink feature

Iceberg sinks require a feature flag to be enabled:

```mzsql
ALTER SYSTEM SET enable_iceberg_sink = true;
```

## Step 2: Create necessary secrets

Store sensitive credentials as secrets:

```mzsql
-- For Iceberg catalog authentication (if using REST catalog)
CREATE SECRET iceberg_oauth_secret AS '<your-oauth-client-secret>';

-- For AWS S3 access
CREATE SECRET aws_secret AS '<your-aws-secret-access-key>';
```

## Step 3: Create an AWS connection

Create a connection for S3 storage access:

{{< tabs >}}
{{< tab "Role assumption (Recommended)">}}

```mzsql
CREATE CONNECTION aws_conn TO AWS (
    ASSUME ROLE ARN = 'arn:aws:iam::123456789012:role/MaterializeIcebergRole',
    REGION = 'us-east-1'
);
```

Make sure your IAM role has the following S3 permissions:
- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject`
- `s3:ListBucket`

See [AWS connection permissions](/sql/create-connection/#aws-permissions) for trust policy configuration.

{{< /tab >}}

{{< tab "Static credentials">}}

```mzsql
CREATE CONNECTION aws_conn TO AWS (
    ACCESS KEY ID = '<your-access-key-id>',
    SECRET ACCESS KEY = SECRET aws_secret,
    REGION = 'us-east-1'
);
```

{{< /tab >}}
{{< /tabs >}}

## Step 4: Create an Iceberg catalog connection

Choose the catalog type that matches your setup:

{{< tabs >}}
{{< tab "Polaris/Nessie (REST Catalog)">}}

For generic REST-compatible Iceberg catalogs like Polaris or Nessie:

```mzsql
CREATE CONNECTION iceberg_catalog TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'https://polaris.example.com/api/catalog',
    CREDENTIAL = 'your-client-id:' || SECRET iceberg_oauth_secret,
    WAREHOUSE = 'my_warehouse',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
);
```

{{< /tab >}}

{{< tab "AWS S3 Tables">}}

For AWS S3 Tables:

```mzsql
CREATE CONNECTION s3_tables_catalog TO ICEBERG CATALOG (
    CATALOG TYPE = 'S3TABLESREST',
    URL = 'https://s3tables.us-east-1.amazonaws.com',
    AWS CONNECTION = aws_conn,
    WAREHOUSE = 'my-s3-table-bucket'
);
```

{{< /tab >}}
{{< /tabs >}}

## Step 5: Create a source or materialized view

Prepare the data you want to stream to Iceberg. You can sink from a source, table, or materialized view.

For this example, let's create a materialized view that aggregates user activity:

```mzsql
-- Assume you have a source named user_events
CREATE MATERIALIZED VIEW user_activity_summary AS
SELECT
    user_id,
    DATE_TRUNC('hour', event_timestamp) as hour,
    COUNT(*) as event_count,
    COUNT(DISTINCT session_id) as session_count,
    MAX(event_timestamp) as last_event_time
FROM user_events
GROUP BY user_id, DATE_TRUNC('hour', event_timestamp);
```

## Step 6: Create the Iceberg sink

Now create the sink to stream data to Iceberg:

```mzsql
CREATE SINK user_activity_iceberg
    IN CLUSTER sink_cluster
    FROM user_activity_summary
    INTO ICEBERG CATALOG CONNECTION iceberg_catalog (
        NAMESPACE 'analytics',
        TABLE 'user_activity'
    )
    USING AWS CONNECTION aws_conn
    KEY (user_id, hour) ENFORCED
    ENVELOPE UPSERT
    WITH (
        COMMIT INTERVAL = '1min'
    );
```

### Key parameters explained

- **`NAMESPACE`**: The Iceberg namespace (database) where the table will be created
- **`TABLE`**: The Iceberg table name
- **`KEY`**: Columns that uniquely identify each row (required for updates/deletes)
- **`ENVELOPE UPSERT`**: Use upsert semantics (insert, update, delete)
- **`COMMIT INTERVAL`**: How often to commit batches (smaller = lower latency, more snapshots)

## Step 7: Monitor the sink

Check the sink status:

```mzsql
SELECT
    s.name,
    s.type,
    ss.status,
    ss.error
FROM mz_sinks s
JOIN mz_internal.mz_sink_statuses ss ON s.id = ss.id
WHERE s.name = 'user_activity_iceberg';
```

View sink details:

```mzsql
SELECT
    id,
    namespace,
    table
FROM mz_catalog.mz_iceberg_sinks
WHERE id = (SELECT id FROM mz_sinks WHERE name = 'user_activity_iceberg');
```

## Step 8: Query the Iceberg table

Once the sink is running, you can query the Iceberg table using various tools:

### Using DuckDB

```sql
-- Install and load the Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- Query the table
SELECT
    user_id,
    hour,
    event_count,
    session_count
FROM iceberg_scan('s3://your-bucket/warehouse/analytics/user_activity')
WHERE hour >= CURRENT_DATE - INTERVAL 7 days
ORDER BY hour DESC, event_count DESC
LIMIT 100;
```

### Using Apache Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "rest") \
    .config("spark.sql.catalog.my_catalog.uri", "https://polaris.example.com/api/catalog") \
    .getOrCreate()

df = spark.read.format("iceberg") \
    .load("my_catalog.analytics.user_activity")

df.show()
```

### Using Trino

```sql
SELECT
    user_id,
    hour,
    event_count
FROM iceberg.analytics.user_activity
WHERE hour >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY hour DESC
LIMIT 100;
```

## Common patterns

### Real-time aggregation to data lake

Stream aggregated metrics from Materialize to Iceberg for historical analysis:

```mzsql
-- Create an aggregation view
CREATE MATERIALIZED VIEW daily_sales_summary AS
SELECT
    product_id,
    DATE_TRUNC('day', order_timestamp) as sale_date,
    COUNT(*) as order_count,
    SUM(quantity) as total_quantity,
    SUM(revenue) as total_revenue
FROM orders
GROUP BY product_id, DATE_TRUNC('day', order_timestamp);

-- Sink to Iceberg
CREATE SINK daily_sales_iceberg
    FROM daily_sales_summary
    INTO ICEBERG CATALOG CONNECTION iceberg_catalog (
        NAMESPACE 'sales',
        TABLE 'daily_summary'
    )
    USING AWS CONNECTION aws_conn
    KEY (product_id, sale_date) ENFORCED
    ENVELOPE UPSERT
    WITH (COMMIT INTERVAL = '5min');
```

### Change data capture (CDC) to Iceberg

Capture changes from a PostgreSQL source and stream to Iceberg:

```mzsql
-- Create PostgreSQL source
CREATE SOURCE pg_users
FROM POSTGRES CONNECTION pg_conn (
    PUBLICATION 'mz_source'
)
FOR TABLES (public.users);

-- Sink raw CDC data to Iceberg
CREATE SINK users_cdc_iceberg
    FROM pg_users
    INTO ICEBERG CATALOG CONNECTION iceberg_catalog (
        NAMESPACE 'raw',
        TABLE 'users'
    )
    USING AWS CONNECTION aws_conn
    KEY (id) ENFORCED
    ENVELOPE UPSERT
    WITH (COMMIT INTERVAL = '30s');
```

## Limitations and considerations

### Current limitations

{{< warning >}}
Be aware of these limitations when using Iceberg sinks:
{{< /warning >}}

- **No schema evolution**: The table schema is fixed after creation. To change the schema, you must drop and recreate the sink.
- **No custom partitioning**: Tables are created without partitioning. Custom partition specs are not supported.
- **Limited nested type support**: Complex nested types may not work correctly.
- **Write-only**: Materialize cannot read from Iceberg tables; use external query engines instead.

### Performance tuning

**Commit interval selection**:
- **Smaller intervals** (30s - 1min): Lower latency, more Iceberg snapshots, higher catalog load
- **Larger intervals** (5min - 15min): Higher latency, fewer snapshots, reduced catalog load

Start with `1min` and adjust based on your latency requirements and catalog performance.

**Cluster sizing**:
- Iceberg sinks perform parallel Parquet writes
- Monitor cluster CPU and memory utilization
- Scale up if you see resource exhaustion during high-throughput periods

**Compaction**:
- Over time, Iceberg tables accumulate many small Parquet files and delete files
- Use Iceberg maintenance operations (via Spark or other tools) to compact files
- Schedule regular compaction jobs for optimal query performance

### Operational guidelines

- **Separate clusters**: Dedicate a cluster for sinks separate from sources and query processing to enable [blue/green deployments](/manage/dbt/blue-green-deployments)
- **Monitor catalog health**: Large numbers of snapshots can impact catalog performance
- **Expire old snapshots**: Use Iceberg's snapshot expiration to clean up old snapshots
- **Test schema changes**: Before modifying schemas, test the change in a development environment

## Troubleshooting

For common issues and solutions, see:

- [CREATE SINK: Iceberg troubleshooting](/sql/create-sink/iceberg/#troubleshooting)
- [General sink troubleshooting](/serve-results/sink/sink-troubleshooting)

## Related resources

- [CREATE SINK: Iceberg](/sql/create-sink/iceberg) - Full SQL reference
- [CREATE CONNECTION](/sql/create-connection) - Connection management
- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/) - Official Iceberg docs
- [Polaris Catalog](https://www.polaris.io/) - Open-source REST catalog
- [Project Nessie](https://projectnessie.org/) - Git-like catalog for data lakes
