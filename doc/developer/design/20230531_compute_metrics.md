- Feature name: Operational metrics for Compute
- Associated:
  * [#18745: Improve Compute metrics dashboard](https://github.com/MaterializeInc/materialize/issues/18745)
  * [#16026: Prometheus metrics for the Compute Controller](https://github.com/MaterializeInc/materialize/issues/16026)
  * [#16951: Add replica connection metrics](https://github.com/MaterializeInc/materialize/issues/16951)
  * [#17064: Add replica fully hydrated metric](https://github.com/MaterializeInc/materialize/issues/17064)

# Summary
[summary]: #summary

Observability into internals of the Compute layer is currently limited in production.
While Compute replicas do expose runtime information via introspection sources, that information is not easily available to cloud operators.
Other parts of Compute do not expose any runtime information in the first place.

To improve cloud operators' visibility into the operation of the Compute layer, this design document proposes the introduction of additional Prometheus metrics.
These metrics are either exported directly by the respective Compute components, or collected from introspection sources using the prometheus-exporter.
The goal is to provide cloud operators with usable means to recognize and diagnose production incidents involving Compute components before and as they arise.

# Motivation
[motivation]: #motivation

Diagnosing production incidents involving Compute components is often cumbersome today.
Usually when a replica is OOMing, exhausting its CPU allocation, or refusing to respond to queries, the only means to diagnose further is to connect to the environment in question and run SQL queries against the replica's introspection sources.
This quickly becomes unwieldy when multiple replicas from different environments are failing.
This method of debugging also requires SQL-level access to production environments, including the ability to run queries against specific replicas, which is limited for security and data privacy reasons.

Other Compute components, like the controller and the optimizer, do not expose much runtime information in the first place.
For example, in the past memory leaks in the controller have been hard to diagnose because we lack observability into runtime controller state.

Apart from hindering diagnosis of active incidents, the lack of observability also makes it difficult to judge the health of Compute components in production.
Being able to do so is useful for detecting anomalies before they become incidents, but also for release qualification.
Today we use system resource usage metrics (mostly CPU and memory) as proxies, but these are not always sufficient to detect issues.
For example, when there is a leak in controller state it might take a long time before that visibly reflects in the process' memory usage.

The Prometheus/Grafana infrastructure in place at Materialize today is suitable for solving the issues outlined above.
It enables interested parties to judge the health of our production deployments at a glance and enables high-level diagnosis of issues arising in individual environments and processes.
To make it useful for Compute we need to do the work of identifying and implementing relevant Prometheus metrics.

# Goals
[goals]: #goals

Enable individuals from Engineering, Support, and other interested groups to:

* Assess the health of Compute components over all environments.
* Diagnose misbehaving Compute components, like OOMing or non-responsive clusters.
* Assess performance trends over time, and especially between releases.

The Compute components in scope are:

* Compute controller
* Compute replicas

# Non-goals
[non-goals]: #non-goals

We consider the following goals useful but outside the scope of this design:

* Providing analytics data to inform product decisions.
* Providing observability for the optimizer. This work is tracked in <https://github.com/MaterializeInc/materialize/issues/17592>.

# Explanation
[explanation]: #explanation

## Prometheus Metrics

This design proposes to introduce a set of new Prometheus metrics, henceforth referred to as "metrics".

Each metric has a [type][metric-types].
In Materialize, the metric types in use are:

* **Counter**: A numerical value that can only increase or be reset to zero on process restart.
* **Gauge**: A numerical value that can increase and decrease arbitrarily.
* **Histogram**: A collection of bucket counts that represents a cumulative histogram.

A metric consists of a name and a set of labels with values (also referred to as "dimensions").
Metric and label names can mostly be chosen arbitrarily, but [conventions][metric-naming] for them exist.
One of these conventions is that metric names should have an application prefix, for Materialize that is `mz_`.

For a given metric, each combination of label values defines a time series.
For example, a metric `request_count` with a single `request_type` label defines one time series per distinct `request_type` value.
If you have multiple labels, and/or labels with high cardinalities, the number of time series can quickly grow very large.
This is especially true for histogram metrics, which expose several per-bucket time series.
Therefore, it is important to limit the number of labels and, if possible, the number of label values.

[metric-types]: https://prometheus.io/docs/concepts/metric_types/
[metric-naming]: https://prometheus.io/docs/practices/naming/

## Metrics Collection in the Database

The database has two ways of exposing metrics: directly and through the prometheus-exporter.

### Direct Export

All database processes (i.e. `environmentd` and `clusterd`) expose metric endpoints through which they export metric values collected by the components they run.
Database components can register and update metrics to be exposed through these endpoints using the [`MetricsRegistry`] type.

[`MetricsRegistry`]: https://github.com/MaterializeInc/materialize/blob/244bdc6cc6c6180cc1b92e1b84915dcaa52685e6/src/ore/src/metrics.rs#L105

### prometheus-exporter

The prometheus-exporter is a service running in Materialize cloud environments that generates Prometheus metrics from SQL query results.
It is configured with a set of queries to run at regular intervals.
The results of these queries are converted into metric labels and values and then exposed through a metric endpoint.

The prometheus-exporter is capable of performing per-replica queries, making it suitable for collecting metrics derived from introspection sources.

Note that for prometheus-exporter metrics we normally use the gauge type even for metrics with strictly increasing values.
That is because we replace the metric value every time with the query result, which the counter type doesn't allow (it can only be increased by a relative amount).
It would be possible to adapt the prometheus-exporter to also support the counter type, by remembering previous values, but so far that has not been necessary.

## Compute Metrics

In this document, we discuss two groups of metrics:

* Controller metrics, i.e. metrics exported by the compute controller
* Replica metrics, i.e. metrics exported by compute replicas

Controller metrics are always exposed through direct export.

Replicas are able to write to introspection sources, so replica metrics can be exposed either through direct export or through the prometheus-exporter querying introspection sources.
In general, we prefer exporting replica information through introspection sources, as this way the information also becomes useful to database users.
Replica metrics are exported directly when they are unlikely to be useful to database users, for example when they track implementation details of Compute.

### Controller Metrics

The following list describes the metrics we want to collect in the controller.
All metrics in this list are exposed through direct export using the controller's existing `MetricsRegistry`.

All metrics in this list have an `instance_id` label identifying the compute instance and, if applicable, a `replica_id` label.

* Compute protocol
  * [x] `mz_compute_commands_total`
    * **Type**: counter
    * **Labels**: `instance_id`, `replica_id`, `command_type`
    * **Description**: The total number of compute commands sent, by replica and command type.
  * [x] `mz_compute_responses_total`
    * **Type**: counter
    * **Labels**: `instance_id`, `replica_id`, `response_type`
    * **Description**: The total number of compute responses sent, by replica and response type.
  * [x] `mz_compute_command_message_bytes_total`
    * **Type**: counter
    * **Labels**: `instance_id`, `replica_id`, `command_type`
    * **Description**: The total number of bytes sent in compute command messages, by replica and command type.
    * **Notes**: A similar metric exists already as a histogram: `mz_compute_messages_sent_bytes`.
                 Proposing to rename it because "messages sent" doesn't imply who the sender is.
                 Also proposing to make it a counter to reduce its cardinality.
                 Also proposing to add a `command_type` label.
  * [x] `mz_compute_response_message_bytes_total`
    * **Type**: counter
    * **Labels**: `instance_id`, `replica_id`, `response_type`
    * **Description**: The total number of bytes sent in compute response messages, by replica and response type.
    * **Notes**: A similar metric exists already as a histogram: `mz_compute_messages_received_bytes`.
                 Proposing to rename it because "messages received" doesn't imply who the receiver is.
                 Also proposing to make it a counter to reduce its cardinality.
                 Also proposing to add a `response_type` label.
* Controller state
  * [x] `mz_compute_controller_replica_count`
    * **Type**: gauge
    * **Labels**: `instance_id`
    * **Description**: The number of replicas.
  * [x] `mz_compute_controller_collection_count`
    * **Type**: gauge
    * **Labels**: `instance_id`
    * **Description**: The number of installed compute collections.
  * [x] `mz_compute_controller_peek_count`
    * **Type**: gauge
    * **Labels**: `instance_id`
    * **Description**: The number of pending peeks.
  * [x] `mz_compute_controller_subscribe_count`
    * **Type**: gauge
    * **Labels**: `instance_id`
    * **Description**: The number of active subscribes.
  * [x] `mz_compute_controller_command_queue_size`
    * **Type**: gauge
    * **Labels**: `instance_id`, `replica_id`
    * **Description**: The size of the compute command queue, by replica.
  * [x] `mz_compute_controller_response_queue_size`
    * **Type**: gauge
    * **Labels**: `instance_id`, `replica_id`
    * **Description**: The size of the compute response queue, by replica.
* Command history
  * [x] `mz_compute_controller_history_command_count`
    * **Type**: gauge
    * **Labels**: `instance_id`, `type`
    * **Description**: The number of commands in the command history, by command type.
  * [x] `mz_compute_controller_history_dataflow_count`
    * **Type**: gauge
    * **Labels**: `instance_id`
    * **Description**: The number of dataflows in the command history.
* Replica connection state
  * [ ] `mz_compute_controller_connected_replica_count`
    * **Type**: gauge
    * **Labels**: `instance_id`
    * **Description**: The number of successfully connected replicas.
  * [ ] `mz_compute_controller_replica_connects_total`
    * **Type**: counter
    * **Labels**: `instance_id`, `replica_id`
    * **Description**: The total number of replica (re-)connections, by replica.
  * [ ] `mz_compute_controller_replica_connect_wait_time_seconds_total`
    * **Type**: counter
    * **Labels**: `instance_id`, `replica_id`
    * **Description**: The total time spent waiting for replica (re-)connection, by replica.
* Peeks
  * [x] `mz_compute_peeks_total`
    * **Type**: counter
    * **Labels**: `instance_id`, `result`
    * **Description**: The total number of peeks served, by result type (rows, error, canceled).
  * [x] `mz_compute_peek_duration_seconds`
    * **Type**: histogram
    * **Labels**: `instance_id`, `result`
    * **Description**: A histogram of peek durations since restart, by result type (rows, error, canceled).

### Replica Metrics

The following list describes the metrics we want to collect in the replicas.
These metrics are either exported directly or from introspection sources using the prometheus-exporter.
All metrics in this list have a `worker_id` label identifying the Timely worker.

* Command history
  * [x] `mz_compute_replica_history_command_count`
    * **Type**: gauge
    * **Labels**: `worker_id`, `type`
    * **Description**: The number of commands in the command history, by command type.
    * **Export Type**: direct
    * **Notes**: This metric exists already as `mz_compute_command_history_size`.
                 Proposing to rename for consistency, and adding worker and command type labels.
  * [x] `mz_compute_replica_history_dataflow_count`
    * **Type**: gauge
    * **Labels**: `worker_id`
    * **Description**: The number of dataflows in the command history.
    * **Export Type**: direct
    * **Notes**: This metric exists already as `mz_compute_dataflow_count_in_history`.
                 Proposing to rename for consistency, and adding a worker label.
* Dataflows
  * [x] `mz_dataflow_elapsed_seconds_total`
    * **Type**: gauge
    * **Labels**: `worker_id`, `collection_id`
    * **Description**: The total time spent computing dataflows, by dataflow.
    * **Export Type**: prometheus-exporter, through the `mz_scheduling_elapsed` introspection source
    * **Notes**: To reduce the cardinality of this metric, we limit it to dataflows that have an elapsed time of more than 1 second.
  * [x] `mz_dataflow_join_elapsed_seconds_total`
    * **Type**: gauge
    * **Labels**: `worker_id`, `collection_id`
    * **Description**: The total time spent computing joins in dataflows, by dataflow.
    * **Export Type**: prometheus-exporter, through the `mz_scheduling_elapsed` introspection source
    * **Notes**: To reduce the cardinality of this metric, we limit it to joins that have an elapsed time of more than 1 second.
  * [x] `mz_dataflow_shutdown_duration_seconds`
    * **Type**: histogram
    * **Labels**: `worker_id`
    * **Description**: A histogram of dataflow shutdown durations since restart.
    * **Export Type**: prometheus-exporter, through the `mz_dataflow_shutdown_durations_histogram` introspection source
  * [ ] `mz_dataflow_frontiers`
    * **Type**: gauge
    * **Labels**: `worker_id`, `collection_id`
    * **Description**: The frontiers of dataflows.
    * **Export Type**: prometheus-exporter, through the `mz_compute_frontiers` introspection source
    * **Notes**: To reduce the cardinality of this metric, we limit it to non-transient dataflows.
  * [ ] `mz_dataflow_import_frontiers`
    * **Type**: gauge
    * **Labels**: `worker_id`, `collection_id`
    * **Description**: The import frontiers of dataflows.
    * **Export Type**: prometheus-exporter, through the `mz_compute_import_frontiers` introspection source
    * **Notes**: To reduce the cardinality of this metric, we limit it to non-transient dataflows.
  * [ ] `mz_dataflow_initial_output_duration_seconds`
    * **Type**: counter
    * **Labels**: `worker_id`, `collection_id`
    * **Description**: The time from dataflow installation up to when the first output was produced.
    * **Export Type**: direct
    * **Notes**: To reduce the cardinality of this metric, we limit it to non-transient dataflows.
                 See also [Dataflow Hydration Time](#dataflow-hydration-time).
* Arrangements
  * [ ] `mz_arrangement_count`
    * **Type**: gauge
    * **Labels**: `worker_id`
    * **Description**: The number of arrangements in all dataflows.
    * **Export Type**: prometheus-exporter, through the `mz_arrangement_sizes` introspection source
  * [ ] `mz_arrangement_record_count`
    * **Type**: gauge
    * **Labels**: `worker_id`
    * **Description**: The number of records in all arrangements in all dataflows.
    * **Export Type**: prometheus-exporter, through the `mz_arrangement_sizes` introspection source
  * [ ] `mz_arrangement_batch_count`
    * **Type**: gauge
    * **Labels**: `worker_id`
    * **Description**: The number of batches in all arrangements in all dataflows.
    * **Export Type**: prometheus-exporter, through the `mz_arrangement_sizes` introspection source
  * [ ] `mz_arrangement_size_bytes`
    * **Type**: gauge
    * **Labels**: `worker_id`
    * **Description**: The size of all arrangements in all dataflows.
    * **Export Type**: prometheus-exporter, through the `mz_arrangement_sizes` introspection source
  * [ ] `mz_arrangement_maintenance_seconds_total`
    * **Type**: counter
    * **Labels**: `worker_id`
    * **Description**: The total time maintaining arrangements.
    * **Export Type**: direct
    * **Notes**: This metric exists already, with an extra label `arrangement_id`.
                 Proposing to remove the `arrangement_id` label, because it blows up the cardinality of this metric.
* Reconciliation
  * [ ] `mz_compute_reconciliation_reused_dataflows_count_total`
    * **Type**: counter
    * **Labels**: `worker_id`
    * **Description**: The total number of dataflows that were reused during compute reconciliation.
    * **Export Type**: direct
    * **Notes**: This metric exists already as `mz_compute_reconciliation_reused_dataflows`.
                 Proposing to rename to follow the Prometheus naming conventions, and adding a worker label.
  * [ ] `mz_compute_reconciliation_replaced_dataflows_count_total`
    * **Type**: counter
    * **Labels**: `worker_id`
    * **Description**: The total number of dataflows that were replaced during compute reconciliation.
    * **Export Type**: direct
    * **Notes**: This metric exists already as `mz_compute_reconciliation_replaced_dataflows`.
                 Proposing to rename to follow the Prometheus naming conventions, and adding a worker label.
* Peeks
  * [ ] `mz_compute_replica_peek_count`
    * **Type**: gauge
    * **Labels**: `worker_id`
    * **Description**: The number of pending peeks.
    * **Export Type**: prometheus-exporter, through the `mz_active_peeks` introspection source
  * [ ] `mz_compute_replica_peek_duration_seconds`
    * **Type**: histogram
    * **Labels**: `worker_id`
    * **Description**: A histogram of peek durations since restart.
    * **Export Type**: prometheus-exporter, through the `mz_peek_durations_histogram` introspection source
    * **Notes**: This metric exists already as `mz_peek_durations`.
                 Proposing to make it report seconds instead of nanoseconds, in line with the Prometheus conventions.
                 Also proposing to add the `compute_replica` prefix, to make it clear where the metric is collected.
* Scheduling
  * [ ] `mz_compute_replica_park_duration_seconds`
    * **Type**: histogram
    * **Labels**: `worker_id`
    * **Description**: A histogram of worker park durations since restart.
    * **Export Type**: prometheus-exporter, through the `mz_scheduling_parks_histogram` introspection source

#### Per-dataflow Metrics

Some dataflow metrics have a `collection_id` label, specifying IDs of compute collections (indexes, MVs, subscribes).
A dataflow can export multiple collections, so there is not generally a 1-to-1 mapping between the two concepts.
Dataflows have replica-local IDs that we could use to label per-dataflow metrics instead.
However, for debugging a collection ID is more useful than a dataflow ID, since the latter cannot be mapped to catalog objects without introspection information, which is only accessible on healthy replicas.
Hence, labeling with collection IDs is preferable even for metrics that are strictly speaking per-dataflow rather than per-collection.
If a dataflow has multiple collection exports, we can duplicate per-dataflow metrics for each of those exports.

Metrics with a `collection_id` label are prone to high cardinalities, so we need to apply cardinality-reducing measures.
Those generally work by filtering out short-running or transient dataflows.

* For all per-dataflow metrics, we can filter on the collection ID to ignore transient dataflows created by `SELECT`s and `SUBSCRIBE`s.
* For dataflow scheduling metrics, we can filter on the dataflows' elapsed time to ignore dataflows that have not been running for long.
  This approach has the advantage that it doesn't exclude long-running `SUBSCRIBE`s, which are potentially interesting for debugging.

#### Dataflow Hydration Time

We recognize that measuring dataflow hydration time is valuable for monitoring, issue diagnosis, and product analytics.
At the same time, answering the question "When is a replica fully hydrated?" is not trivial.
We consider answering this question to be outside the scope of this design.

Instead, this design proposes adding a minimum viable metric for estimating dataflow hydration time: `mz_dataflow_initial_output_duration_seconds`.
As the name suggests, this metric measures the time between the installation of a dataflow and the point where it first produced output.
At this point, the dataflow has processed the snapshots of its sources but it has not necessarily processed the entire source data available.
If the dataflow sources are sufficiently compacted, the time of initial output should be close to the time of hydration.
So we can expect this metric to be a reasonable stand-in for an actual hydration time measurement while being considerably easier to implement.

The metric is proposed as a direct-export metric rather than one collected by the prometheus-exporter through an introspection source.
The rationale is mainly that hopefully this metric is only a temporary solution until we have a better way to detect hydration.
Apart from that, it is also not clear what use our users would find in such an introspection source.

# Rollout
[rollout]: #rollout

This design does not propose any user-visible changes.
Therefore, we are unconstrained in the order in which we make the metric changes described above.

Most new metrics can immediately be enabled in production after they are implemented.
An exception are metrics that have a `collection_id` dimensions.
These are prone to high cardinalities, so we should test them for a couple days on staging only, to ensure our measures to reduce cardinality function as expected.

Once new metrics have been deployed to production, we will update the Compute dashboard to show them.
Some of the metrics proposed here are renamed ones that already exist and that are already in use in the dashboard.
For these metrics, we will adapt the dashboard queries temporarily so that they include both metrics (e.g., by simply adding their values) until the old ones have been phased out everywhere.

## Testing and observability
[testing-and-observability]: #testing-and-observability

We will test the new metrics manually in staging and production by inspecting their outputs and verifying that they are plausible.
Potentially high-cardinality metrics, such as the ones having a `collection_id` label, will be tested on staging before they are enabled in production.

# Drawbacks
[drawbacks]: #drawbacks

Every new metric increases the load on our Prometheus infrastructure.
We should therefore only add metrics that provide value enough to justify this added load.
Especially high-cardinality metrics must be well justified.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

This design integrates well with our existing metrics infrastructure and mirrors what other Materialize teams are already doing.

For each of the proposed metrics, we have the option of alternatively not implementing it.
Not implementing a metric means reduced observability but also reduced load on our Prometheus infrastructure, to varying degrees, depending on the metric.

For each of the proposed prometheus-exporter metrics, we have the option of instead directly exporting them from the replica code.
We default to using the prometheus-exporter because this minimizes the amount of code complexity these metrics introduce.
If it turns out that the additional queries made by the prometheus-exporter place a significant load on the replicas, we might need to reconsider and migrate some of the metrics to directly exported ones.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

- What other metrics would be valuable to implement?

# Future work
[future-work]: #future-work

Once this design is implemented, Compute observability will be solid but likely not perfect.
Future work will consist of identifying a) new valuable metrics we should also implement and b) implemented metrics that should be removed because they don't provide enough value.
A large part of a) will consist of adding metrics for optimizer operation, as tracked by <https://github.com/MaterializeInc/materialize/issues/17592>.

Implementing a reliable and hydration signal for replicas and dataflows is left as future work as well.
