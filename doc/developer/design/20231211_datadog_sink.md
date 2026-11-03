# Datadog Sink

- Associated issues:
    - https://github.com/MaterializeInc/materialize/issues/17601
    - https://github.com/MaterializeInc/materialize/issues/4779

## The Problem

Users want to export **metrics** to Datadog. But they need to learn how to configure, test, deploy, and maintain two external services: the Datadog Agent and a SQL Exporter — making the experience long and sometimes even frustrating.

## Success Criteria

A Materialize user can integrate and export custom metrics to Datadog by running the following SQL commands:

```sql
CREATE CONNECTION conn_datadog TO DATADOG (API KEY = API_KEY_SECRET);
CREATE SINK FROM <source> TO DATADOG CONNECTION conn_datadog;
```

The source content of the sink, which can be either a table, materialized view, or source, will be defined by the user but needs to match a predefined structure.

## Out of Scope

It is out of the scope:

- Alternative approaches, such as https://github.com/MaterializeInc/materialize/issues/4779, to select labels and values, or send additional data to Datadog.
- Compression algorithms before exporting metrics.
- OAuth process to create an API Key.
- Anything else related to a Datadog integration, such as dashboards or tiles.

## Solution Proposal

The solution proposal involves a new connection and sink for Datadog.

### Datadog Connection

The new connection needs a Datadog API key, which is required to send metrics to Datadog. Before creating the connection, the user must create a secret that contains the API key and then create the connection as follows:

```sql
CREATE CONNECTION conn_datadog TO DATADOG (API KEY = ...);
```

The connection will automatically validate the API Key, just like Postgres or Kafka connections.

### Datadog Sink

The Datadog sink will build an internal state containing the metrics to export from the source. It will then submit the entire state to Datadog at a 15-second interval.

The 15-second interval comes from [the Datadog agent](https://docs.datadoghq.com/agent/basic_agent_usage/?tab=agentv6v7#collector) collection interval. While this value could be customizable, a default value is ok for the initial approach and MVP. The sink will use Materialize logical time to calculate when the progress reaches the 15-second interval and will then export the metrics to Datadog. In the event of a sink crash or pause, it will behave similarly to the Kafka sink, recovering from the last logical time processed.

A discarded approach is exporting metrics only when their values change. This would be streaming from the source to Datadog without any state retention. But, certain metrics, like storage usage, are updated once an hour. This means exporting such metrics once every hour, resulting in no updates in Datadog for an hour or any shorter timeframe. This makes it an unsuitable approach for metrics visualizations.

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

In this design document, two of the optional fields, `metadata` and `tags`, are not going to be considered for the implementation to make the structure simpler. The user will have to set the values for all other fields, except for the `timestamp`. Materialize will set the timestamp using its logical time upon reaching progress in the 15-second interval.
Optional fields can be omitted and customized by the user within Datadog's app.
The `type` field will be represented in Materialize as either `NULL`, `'count'`, `'rate'`, or `'gauge'`, and later converted into its corresponding integer value.

**NOTE**: Datadog uses *resources* to what equals *labels* in the OpenMetrics definition. We ended up liking more and choosing *labels* over *resources*.

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


The idea behind the following considerations, or let's call them controls, is to verify that the source metrics are correct. The severity of the control may not be critical or necessary to trigger an error and stop exporting metrics; instead, we might simply skip exporting the metric and log a warning.

Controls:
1. If the source structure is incorrect (e.g. a missing required column), the sink will throw an error about it during its creation.
2. If a required field is incomplete or contains `NULL` values, the sink will not export the metric until it is fixed. If it is possible to detect during sink creation, a `NOTICE` message containing the warning should be sent. Otherwise, a log should be created and stored in a dedicated collection, or the status should be changed to error/stale or assigned a new status name more suitable to the situation. I'm still looking and thinking for what is the best suitable approach here to monitor and log/report errors.
3. If an optional field it is empty or contains a `NULL` value no warning should be generated. But we should raise one if there is a value mismatch, such as using `'gage'` instead of `'gauge'`. Compared to the case in (2.), the sink should continue to export the metric.

Not mandatory but a plus:
* If the sink detects a duplicated set of metrics and labels with different values for the same timestamp during creation, it should log or emit a `NOTICE` warning message. Datadog selects the latest value sent for these cases.

### Datadog Sink Code

The Datadog sink will be added as a new module at `storage/src/sink` and have its own implementation of `SinkRender`. The Datadog sink doesn't need to send retractions or manage a complex state. Shared code paths between the Kafka sink and the Datadog sink will surge and should be handled with consideration. E.g. adding a new sink could introduce changes in places like (`alter_compatible`)[https://github.com/MaterializeInc/materialize/blob/cc947745215d96e4d86ff507b6ac92311690d7d3/src/storage-types/src/sinks.rs#L294C1-L294C1].

## Minimal Viable Prototype

The user should be able to create a new Datadog connection and sink to export custom metrics. The following commands should work:
```sql
CREATE SECRET API_KEY_SECRET AS '<VALID_DATADOG_API_KEY>';
CREATE CONNECTION datadog TO DATADOG (API KEY = API_KEY_SECRET);
CREATE SINK metrics_sink FROM <METRICS> TO DATADOG CONNECTION datadog;
```

## Alternatives

An alternative is to implement a Datadog agent client. This would require the user to set up an external service to configure, test, deploy, and maintain.

## Open questions

Is it necessary to implement proto `into_proto/from_proto`?
