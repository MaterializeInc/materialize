# Summary

This design doc introduces a way to import data from prometheus metrics endpoints into materialized views that can be inspected with SQL commands.

# Goals

* Allow our users to read & make decisions based on our metrics data without setting up a whole metrics ingestion/presentation pipeline.

* Let administrators inspect that metrics data (as much as metrics' attached data allows) together with the other available data we present in system tables.

* Retain a long-enough history of metrics values to allow making decisions based on how the metrics values change (but short enough to not consume unbounded memory).

# Non-Goals

* Export our system table logs as prometheus (yet) -- this design covers only the "metrics import" direction.

* Come up with a data retention scheme other than "older than n minutes gets dropped". If this was a full-featured time series database design, we'd want to thin metrics into minute/hour/day buckets. This isn't that.

* (Maybe) don't even expose the `CREATE SOURCE` mechanics externally?

# Description

<!--
    Describe the approach in detail. If there is no clear frontrunner, feel free to list all approaches in alternatives.
    If applicable, be sure to call out any new testing/validation that will be required
-->

To import prometheus metrics, a process must regularly scrape an endpoint. Something like the following would be our external interface:

```sql
CREATE SOURCE metrics_table_name
PROMETHEUS ENDPOINT 'http://localhost:9090/metrics'
WITH (
    metadata_table = "metrics_table_meta",
    poll_interval = '1 minute'::interval,
    retain_data = '5 minutes'::interval
);
```

This creates two relations that have schemas similar to the following:

```sql
CREATE TABLE metrics_table_name (
    metric TEXT,
    ts TIMESTAMP,
    labels JSONB,
    value float8
);

CREATE TABLE metrics_table_meta (
    metric TEXT,
    metric_type TEXT,
    help TEXT
);
```

Once created, the source polls the endpoint once a minute (via the configured the `poll_interval` duration), and retains metric readings for 5 minutes.


# Alternatives

<!-- // Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen -->

There are two major things that we could do differently to fulfill the brief, and a series of things that we could do to polish this design. I'll summarize the former under "Alternative approaches" and the latter under "Alternative details".

## Alternative approaches

### Create an internal metrics reporting channel instead (feeding metrics directly)

We could add a mechanism for providing log events to the dataflow crate (or a crate that df and others depend on), where if a piece of code now increments a prometheus counter, it would instead send an event that gets captured by a listener on a channel, and increments the prometheus cuonter, as well as queue the event for updating of internal metrics views.

The major benefit is that we encapsulate our metrics tooling usage more (making it easier to plug in more ways of recording metric values).

The downsides, however, are somewhat big:

* Things need to be structured such that you can get at the logger where you need to count the metric. Some parts where we currently do this make this somewhat difficult (e.g. the pgwire code, where the currently best way to count bytes sent is in encoding).

* Adding metrics gets more difficult. You not only have to figure out how go get your number counted the right way, you also have to figure out how to aggregate it properly in the dataflow world. We worry that this might mean that useful operational metrics don't get added as soon/as quickly as they'd be necessary, making it harder to run the system.

### Do not scrape arbitrary prometheus endpoints, instead scrape our internal metrics registry

This would get around the need to introduce an externally-definable source, and automatically feed a dataflow that we can keep up to date automatically.

There's one major reason why I think this would be harder to implement right now: Being able to pull in all our metrics depends on [#5825], which would be a pretty heavy re-work of our internal structures (have to thread a registry through to various bits that don't have a place to hold that registry yet, like the pgwire implementation).

Besides that, I think it's somewhat nice to represent this data as a source that we poll, and internally, our metrics could still be realized like this once a fix for [#5825] lands.

[#5825]: https://github.com/MaterializeInc/materialize/issues/5825

## Alternative details

### Table schema - denormalization

Several ways to represent the metrics imported. The most important "meta" information that users will need is the [metric type](https://prometheus.io/docs/concepts/metric_types/) - gauges, counters and others all behave differently, after all.

It would make sense (but be somewhat denormalized) to list the metric type alongside each reading, like so:

```sql
CREATE TABLE metrics_table_name (
    metric TEXT,
    metric_type TEXT,
    ts TIMESTAMP,
    labels JSONB,
    value float8
);
```

With this schema in place we could leave out the "meta" schema for the first iteration, as it provides only the "help" text.

Another way to avoid the `metric_type` entirely is to use a value column per metric type (and fill only the column whose metric type corresponds to the column):

```sql
CREATE TABLE metrics_table_name (
    metric TEXT,
    ts TIMESTAMP,
    labels JSONB,

    -- These are nice and easy:
    gauge_value float8,
    counter_value float8,

    -- And then histograms is where it gets gross:
    histogram_sum float8,
    histogram_p50 float8,
    histogram_p90 float8,
    -- (...and others, configured in a WITH parameter)
    histogram_count float8
);
```

I think if we want to go down this road, we'll want something like an "is a" table relationship instead of all those NULLable columns. It does smell much more complex and harder to use correctly, though.

# Open questions

* Should we expose the CREATE SOURCE mechanics for this externally? It probably
  has very limited utility for our users outside "read our metrics".

* Is it better to be fully normalized and have two tables, or better to have the kind of measurement in the measurement table and not have the "meta" table?

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->
