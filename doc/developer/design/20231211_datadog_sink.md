# Datadog Sink

- Associated issues:
    - https://github.com/MaterializeInc/materialize/issues/17601
    - https://github.com/MaterializeInc/materialize/issues/4779

## The Problem

Users want to export **metrics** to Datadog. But they need to learn how to configure, test, deploy, and maintain two external services: the Datadog Agent and a SQL Exporter — making the experience long and sometimes even frustrating.

## Success Criteria

In a few clicks, a user can integrate Materialize with Datadog. Also, the user can export custom metrics to Datadog by running the following SQL commands:

```sql
CREATE CONNECTION conn_datadog TO DATADOG API KEY = ...;
CREATE SINK FROM <view/table> TO DATADOG CONNECTION conn_datadog;
```

The source content of the sink, which can be either a table, materialized view, or source, will be defined by the user but needs to match a predefined structure.

## Out of Scope

It is out of the scope:

- Alternative approaches, such as https://github.com/MaterializeInc/materialize/issues/4779, to select labels and values, or send any additional information to Datadog outside the connection validation or metrics.
- Compression algorithms before exporting metrics.

## Solution Proposal

The solution proposal involves a new connection and sink for Datadog, and an OAuth process to create a seamless experience.

### Datadog Connection

The new connection needs a Datadog API key, which is required to send metrics to Datadog. Before creating the connection, the user must create a secret that contains the API key and then create the connection as follows:

```sql
CREATE CONNECTION conn_datadog TO DATADOG (API KEY = ...);
```

### Datadog Sink

The Datadog sink will build an internal state containing the metrics to export from the source. It will then submit the entire state to Datadog at a 15-second interval.

The 15-second interval comes from [the Datadog agent](https://docs.datadoghq.com/agent/basic_agent_usage/?tab=agentv6v7#collector) collection interval. While this value could be customizable, for the initial approach and MVP, a default value is ok. The sink will utilize Materialize logical time to calculate when the progress reaches the 15-second interval and will then export the metrics to Datadog. In the event of a sink crash or pause, it will have a similar behavior to the Kafka sink, recovering from the last logical time processed.

I discarded sending updates when a metric's value changes (streaming without state.) Collections like storage usage are updated once an hour, and this would make charts empty in Datadog for an hour or any smaller time range.

### Datadog Metrics

Submitting metrics to Datadog API requires the following fields:
<details>
<summary>Fields</summary>

| Field             | Type      | Definition|
|-------------------|-----------|-----------|
| metric [required] | String    | The name of the timeseries.                                                                                              |
| points [required] | [Object]  | Points relating to a metric. All points must be objects with timestamp and a scalar value (cannot be a string). Timestamps should be in POSIX time in seconds, and cannot be more than ten minutes in the future or more than one hour in the past. |
| resources         | [Object]  | A list of resources to associate with this metric.                                                                       |
| source_type_name  | String    | The source type name.                                                                                                    |
| tags              | [String]  | A list of tags associated with the metric.                                                                              |
| type              | enum      | The type of metric. The available types are 0 (unspecified), 1 (count), 2 (rate), and 3 (gauge). Allowed enum values: 0, 1, 2, 3                                                        |
| unit              | String    | The unit of point value.                                                                                                 |
| interval          | Int64     | If the type of the metric is rate or count, define the corresponding interval.                                          |
| metadata          | [Object]  | Metadata for the metric.                                                                                                 |

</details>

All fields, except the `timestamp`, are set by the user. Materialize will set the timestamp using its logical time upon reaching progress in the 15-second interval. Optional fields such as `metadata` and `tags` can be customized by the user within Datadog's app. The type field in Materialize will be represented as either `NULL`, `'count'`, `'rate'`, or `'gauge'`, and later converted into its corresponding integer value.

#### Timestamp

In Materialize, we have two possible values to use as a timestamp: wall-clock time and logical time. The sink will use logical time as it is better suited for implementation in a sink than I initially thought. Using logical time brings code coherence and features like recovery without data loss in the event of a failure. As a con, this behavior can hold compaction indefinitely if the sink enters into a crash loop.

While we could allow the user to set the timestamp, it would make the experience harder. Collections like `mz_cluster_replica_utilization` do not include a timestamp, forcing users to create one themselves. *I do not discard the chance to make it optional.*

### Materialize source structure

A Datadog sink needs a _source_: a table, materialized view, or source — and must have a structure that matches [Datadog's API submit metrics endpoint](https://docs.datadoghq.com/api/latest/metrics/#submit-metrics) fields.

A table in Materialize acting as a source for the sink must have the following structure:

```sql
CREATE TABLE metrics (
    -- Required:
    labels JSONB,
    metric TEXT,
    value FLOAT,
    -- Optional:
    type TEXT,
    unit TEXT,
    interval BIGINT
);
```

And a materialized view needs the same structure:
```sql
CREATE MATERIALIZED VIEW metrics AS
    SELECT
    -- Required:
    DISTINCT(
        jsonb_build_object(
            'cluster_id', R.cluster_id,
            'cluster_name', C.name,
        )
    ) as labels,
    'cluster_replica_cpu_usage' as metric,
    cpu_percent::float as value,
    -- Optional:
    'gauge' as type,
    120 as interval,
    'percent' as unit
    FROM mz_internal.mz_cluster_replica_utilization U
    JOIN mz_catalog.mz_cluster_replicas R ON (U.replica_id = R.id)
    JOIN mz_catalog.mz_clusters C ON (R.cluster_id = C.id);
```

**NOTE**: Metrics and their values do not need to be unique for each timestamp, but the latest value will suppress the other in Datadog. It would be great if the sink could detect this during creation/execution and emit a warning if there are different values at the same point in time for a set of metrics and labels.

Another distinction is that Datadog uses *resources* to what equals to *labels* in the OpenMetrics definition. We ended up liking more and choosing *labels* over *resources*.

### OAuth Process

Datadog recommends an OAuth process for their integrations and creating an API key on behalf of the user if the integration needs to write data into Datadog. The token does not have the scope to write data into Datadog, so creating the API key is necessary.

The OAuth process requires additional logic in the console. The whole process is defined in the following sequence diagram:

<details open>
<summary>Sequence diagram</summary>
<img width="2129" alt="Sequence Diagram" src="https://github.com/joacoc/materialize/assets/11491779/28dc14cb-a488-4e8b-8c89-ab69accfd858">
</details>

At the end of the flow, the console creates a secret containing the Datadog API key, connection, and sink. Although we lack an integrations page, a screen similar to what the mz displays at the end of the flow is more than enough for an MVP.

### Datadog Sink Code

The Datadog sink will be added as a new module at `storage/src/sink` and have its own implementation of `SinkRender`. The Datadog sink doesn't need to send retractions or manage a complex state. Shared code paths between the Kafka sink and the Datadog sink will surge and should be handled with consideration. E.g. adding a new sink could introduce changes in places like (`alter_compatible`)[https://github.com/MaterializeInc/materialize/blob/cc947745215d96e4d86ff507b6ac92311690d7d3/src/storage-types/src/sinks.rs#L294C1-L294C1].

## Minimal Viable Prototype

1. The user should be able to create the integration from Datadog in a couple of clicks.
2. The user should be able to create a new Datadog connection and sink to export custom metrics. The following commands should work:
    ```sql
    CREATE SECRET API_KEY_SECRET AS 'Datadog_API_KEY';
    CREATE CONNECTION datadog TO DATADOG API KEY = API_KEY_SECRET;
    CREATE SINK metrics_sink FROM mz_metrics TO DATADOG CONNECTION datadog;
    ```

## Alternatives

An alternative is to implement a Datadog agent client. This would require the user to set up an external service to configure, test, deploy, and maintain.

## Open questions

Is it necessary to implement proto `into_proto/from_proto`?
