# Sink connection management refactor

-   Associated:
    -   [sentry: panic: Reference to absent
        collection](https://github.com/MaterializeInc/database-issues/issues/4982)
    -   [sinks: bootstrapping catalog does not wait for sink
        creation](https://github.com/MaterializeInc/database-issues/issues/5981)

## Context

The current implementation of sink connections relies on establishing the
connection in adapter code. In addition to lacking parity with source connection
management, this also introduces an issue which is detailed in [sinks:
bootstrapping catalog does not wait for sink
creation](https://github.com/MaterializeInc/database-issues/issues/5981).

## Goals

-   Detail and defend the approach for moving sink connection management into
    storage, while still detecting some errors during sink creation.

## Overview

In general, the approach will be to mirror the structure of source connection
management for sinks. This means:

-   Check for potential errors during purification.
    -   This introduces the possibility for TOCTOU errors, so it will be
        possible to create a sink that we believe will work only to have the
        sink immediately fail.
-   Create the sink in the storage controller after purification or during
    bootstrapping, i.e. remove the notion of
    `prepare_export`/`SinkConnectionReady`.
    -   This guarantees that we understand when connections to sinks are
        established and does not potentially leave them in a dangling state
        during bootstrapping.

## Detailed description

The following sections detail the major work items needed to implement this
change.

### Establish connection in rendering

`storage::sink::kafka::KafkaSinkState::new` will become the point at which we
actually establish the connection to Kafka to sink out data.

This code path is executed every time a sink is instantiated and parallels the
structure of how sources work. Sink connections should be idempotent so
re-running this every time we restart should be fine.

Additionally, we need to ensure that we gracefully handle sink connection error
reporting via sink statuses.

### Remove `prepare_export`

On `main` currently, creating a sink requires generating an export token, which
is used to ensure that you hold the since of the object you are sinking to some
fixed point

With this refactor, preparation becomes totally unnecessary because we will no
longer prepare the export, we will only create it under the assumption that it
will either connect or error.

### Check errors in purification

Just as we perform asynchronous work for sources (e.g. checking that the PG
publication exists), we should do the same thing for sinks. This gives us a
chance to error if we believe that the connection will not succeed. Crucially,
purification should not be side-effecting, so this operation should not leave
any state behind.

### Remove `SinkConnectionReady`

We will describe connections to Kafka brokers where we plan to write the sink,
but will no longer "build" the connections in the controller. This means that
`SinkConnectionReady` will be obviated as `prepare_export` is.

### Managing broker topics

Kafka broker admins can delete topics––either the data or the progress topics.
We might encounter this in rendering the sink.

If admins delete the progress topic, we will re-create it, as well as ensure the
data topic exists. We do this because we cannot know if this is a net-new sink
or if the progress topic got deleted.

If admins delete the data topic, we will error iff we find data in the progress
topic that indicates that we had previously produced data for the data topic.
Otherwise, we will re-create the data topic.

## Alternatives

We could, theoretically, treat `CreateExportToken` as a proxy for the sink in
reconciliation, which would let us understand that a sink will be created with
some given ID, even if it is not yet present. However, I am leery of this
approach because of its complexity (how far down do you propagate
`CreateExportToken`s? does it differ during boostrapping vs. sink creation?) and
its lack of parallelism with sources + the idioms of SQL planning.
