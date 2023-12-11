# Datadog Sink

- Associated issues:
    - https://github.com/MaterializeInc/materialize/issues/17601
    - https://github.com/MaterializeInc/materialize/issues/4779

## The Problem

Users want to export Materialize monitoring data to Datadog. To do it, they need to learn how to configure, test, deploy, and maintain two external services, Datadog Agent and SQL Exporter, making the experience long and sometimes even frustrating.

To solve this problem, Materialize could have a sink exporting monitoring data to Datadog.

## Success Criteria

The following code exports monitoring data from a view to Datadog:
```sql
CREATE CONNECTION conn_datadog TO DATADOG API KEY = ...;
CREATE SINK FROM <metrics view> TO DATADOG CONNECTION conn_datadog;
```

Sink health metrics and statistics should be collected in the process.

## Out of Scope

It is out of the scope:
- Alternative approaches, such as https://github.com/MaterializeInc/materialize/issues/4779, to select labels and values, or send any additional information to Datadog outside the connection validation or metrics.
- Compression algorithms before exporting data.
- A definition for a view joining the essential monitoring data.
- Any dashboard or integration work outside the Materialize codebase.

## Solution Proposal

The solution is separated into three different deliverables in the following order:
1. Datadog API client.
2. Datadog connection.
3. Datadog sink.

### Datadog API Client

It will require a new crate, called `datadog-client`, similar to `cloud-client` and `frontegg-api`. This client will interact with Datadog API to upload metrics and validate connections.

### Datadog Connection

A new connection will be added to the parser. The connection validation step needs to use the `datadog-client`.

### Datadog sink

The Datadog sink will be added as a new module at `storage/src/sink` and have its own implementation of `SinkRender`. The Datadog sink doesn't need to send retractions or manage a complex state. Shared code paths between the Kafka sink and the Datadog sink will surge and should be handled with care and consideration. E.g. adding a new sink could introduce changes in places like (`alter_compatible`)[https://github.com/MaterializeInc/materialize/blob/cc947745215d96e4d86ff507b6ac92311690d7d3/src/storage-types/src/sinks.rs#L294C1-L294C1].

## Minimal Viable Prototype

A minimal viable prototype enables a new Datadog connection, and a Datadog sink to export data using the connection.

The following command should export data to Datadog:

```
CREATE SECRET API_KEY_SECRET AS 'Datadog_API_KEY';
CREATE CONNECTION datadog TO DATADOG API KEY = API_KEY_SECRET;
CREATE SINK metrics_sink FROM mz_metrics TO DATADOG CONNECTION datadog;
```

## Alternatives

An alternative is to implement a Datadog agent client. This would require the user to set up an external service to configure, test, deploy, and maintain.

## Open questions

Is it necessary to implement proto `into_proto/from_proto`?

