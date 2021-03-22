# Summary

This design doc introduces a way to import data from our internal prometheus registry into materialized views that can be inspected with SQL commands.

# Goals

* Allow our users to read & make decisions based on our metrics data without setting up a whole metrics ingestion/presentation pipeline.

* Let administrators inspect that metrics data (as much as metrics' attached data allows) together with the other available data we present in system tables.

* Retain a long-enough history of metrics values to allow making decisions based on how the metrics values change (but short enough to not consume unbounded memory).

# Non-Goals

None of the following are intended to be done with this design:

* Export our system table logs as prometheus (yet) -- this design covers only the "metrics import" direction.

* Expose any `CREATE SOURCE` mechanics for importing from arbitrary/external prometheus endpoints.

* Come up with a data retention scheme other than "older than n minutes gets dropped". If this was a full-featured time series database design, we'd want to thin metrics into minute/hour/day buckets. This isn't that.

# Description

<!--
    Describe the approach in detail. If there is no clear frontrunner, feel free to list all approaches in alternatives.
    If applicable, be sure to call out any new testing/validation that will be required
-->

By implementing this design, we'll make the following changes to materialized:

1. Introduce a background task that regularly scrapes the internal metrics registry using the registry's [`gather`](https://docs.rs/prometheus/0.12.0/prometheus/struct.Registry.html#method.gather) method (currently we can use the global registry, but later we'll need a handle to the ["materialized" metrics registry][#5825]; the mechanism will remain the same).
2. Ingest the generated metrics into a dataflow that populates metrics into tables like the following (names subject to change when we implement this):

```sql
CREATE TABLE mz_catalog.metrics (
    metric TEXT,
    ts TIMESTAMP,
    labels JSONB,
    value float8
);

CREATE TABLE mz_calalog.metrics_meta (
    metric TEXT,
    metric_type TEXT,
    help TEXT
);
```

3. This not-quite-source polls the endpoint once in the `--introspection-frequency` period (a duration that defaults to 1 second).

4. It retains metric readings for configurable period of time (probably on the order of 5 minutes, 300 full scrapes' worth of data) to make it possible for users to visualize changes in the metrics as they would with a "full" metrics pipeline. Note: If this gets too complex, the first implementation iteration of the scraper may just retain only the last reading.

## Possible work after this proposal is implemented

* As noted above, after [#5825] is implemented, we'll have to adjust the import task outlined above to receive & use the new registry.

* Once we have a way to import our process's own prometheus metrics, one could introduce a way (a full-fledged `SOURCE`) to read from arbitrary prometheus endpoints. This is explicitly a non-goal right now, and doesn't really align with our product direction right now... but it would be a fun skunkworks project.

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

### Scrape a prometheus HTTP endpoint instead of our internal registry

Prometheus offers a second kind of public interface besides the metrics registry: The HTTP endpoint that a prometheus database scraper itself uses to read the metrics. We could make a source definition that polls the HTTP endpoint and constructs metrics data from that, and even offer a CREATE SOURCE mechanic to productize this.

It'd be more complicated to do this (only [one crate](https://crates.io/crates/prometheus-parse) of unknown quality exists out there that even parses textual representations of prometheus metrics), and would dilute our product direction somewhat. Using the internal-only pathway through the registry also doesn't expose us to errors from the network.

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

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->

[#5825]: https://github.com/MaterializeInc/materialize/issues/5825
