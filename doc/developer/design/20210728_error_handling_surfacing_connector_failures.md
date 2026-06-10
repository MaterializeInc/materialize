# Consistent Error Handling and Surfacing Connector Failures

*I will use connector to refer to either source or sink below. I will use
`mz_connector` to mean either `mz_source` or `mz_sink` or both.*

## Summary

Currently, Materialize reacts to errors happening at different stages of a
connectors lifecycle in different ways. For example, an error happening during
purification/creation of a connector will be reported back to the user
immediately while errors that happen during runtime are only logged. We should
agree on what the behavior should be and fix it where needed.

Additionally, I propose to formally add the concept of a *lifecycle* for
connectors, which will allow the system and users to determine the status of a
connector.  Right now, this would only be possible by looking through log files
for error messages and manually associating them with connectors.

## Goals

- unify error handling policies across connectors
- make it explicit how Materialize should react to errors at different stages
  of execution
- introduce connector lifecycle and surface it to users through system tables

## Non-Goals

- introduce more structured error messages, see
  https://github.com/MaterializeInc/materialize/discussions/5340

## Description

The actual proposal is [way down](#proposed-changes) but it helps to first
understand the current state.

### Current State

#### Where/when do we interact with external systems?

- When creating a source/sink:
  - (coord) purification
- When creating a source/sink or when materialized is restarted:
  - (coord) initial connector setup, for example:
  - (coord) reading Kafka consistency topic
  - (coord) publishing schemas
  - (coord) creating topics
  - (coord) creating output file, checking input file
  - (coord) validate AWS credentials
- Continuously/During dataflow execution:
  - (coord) metadata loops, for example fetching new Kafka partitions,
    listening on BYO topics
  - (dataflow) listening on SQS for S3 updates, scanning S3 bucket (though it
    only happens once, it's still in the dataflow, happening after succesful
    source creation)
  - (dataflow) actual data ingest and writing, for example, from Kafka, S3,
    Postgres...

#### Failures during purification

In general, failures during purification lead to the statement being aborted,
nothing gets added to the catalog. This seems fair, because we cannot store
un-purified statements in the catalog and purification happens infrequently,
only when a statement is issued.

Most purification logic doesn't specify timeouts, only S3 and Kinesis have a 1
second timeout for the validation call. I believe none of the purification
checks do retries

#### Failures During Source/Sink Creation

The behaviour is very similar (the same?) to failures during purification:
nothing gets added to the catalog when there are failures.

Some calls have custom timeouts, some don't. I believe none of the calls have
retries.

#### Failures During "Normal Operations"

Failures during execution don't bring down materialized. If failures happen,
they are only surfaced in the log. Materialized doesn't "know" about the state
of a connector.

There are no global config options for timeouts/retries/back-off interval.

#### Failures During Restart (restarting a materialized with things in the catalog)

Failures during sink (re-)creation kill materialized when restarting with a
filled catalog.  Failures during source creation are fine.

The reason is that (re-)creating a source runs no source-specific code in the
coordinator. Everything happens in dataflow, where we have retries. This is not
true for sinks.

### Proposed Changes

The reaction to failures at various stages should uniform across different
connectors and informed by these rules:

 - Failures during connector creation (which includes purification) immediately
   report an error back to the user. No catalog entry is created.
 - Failures during runtime need to be surfaced to the user in system tables.
 - Failures that occur during a restart with an already filled catalog must not
   bring down Materialize. Instead, errors that occur must be reported to the
   user for each individual connector.

The concrete steps to achieve this, in the order we should address them:

 - "Teach" Materialize about the lifecycle/state of connectors. Connectors can
   be either `RUNNING`, `FAILED`, or `CANCELED`. We might get a bit more fancy
   by adding things like `STARTING`/`RESTARTING`. Issues that happen during
   execution must change the state, instead of just logging error messages.
 - Add "status" columns to `mz_connector` that indicates the status of a
   connector. These would surface the lifecycle state mentioned above.
 - Add a new view `mz_connector_errors` that surface errors that are only
   reported in the log so far.
 - Add a "status" column to `mz_views` and surface errors in a new
   `mz_view_errors` system table. We need these because errors in depended-upon
   sources propagate to views.
 - Maybe log state transitions in the above new views or add additional views
   for that.
 - Failures in a connector should never bring down Materialize. Right now this
   can happen when re-starting. For those cases we should instead set the
   lifecycle status accordingly and continue. It's somewhat tricky because this
   happens when restoring the catalog and looks to the sink like it's just
   being created. Normally, when creating a sink, failures should cancel the
   statement and not add the sink to the catalog but we cannot do this here.
 - Report a `DEGRADED` state in system tables when a connector is in a
   temporarily degraded state. For example when `rdkafka`  can't reach a broker
   but just sits and waits for them to come back online.

#### Dealing with transitive failures

What should be the state of a sink when one of the sources that it depends on
fails. I think it should also be put in a failed state, with
`mz_connector_errors` indicating that it was failed because one of its inputs
has failed.

## Musing/Future work

 - Add something like `RESTART SOURCE` or `RESTART SINK` that can get a
   source/sink out of a wedged state. This is potentially complicated because
   restarting sources would also require restarting dependent sinks.
 - We can think about letting the system restart a fatally failed connector
   instead of requiring user intervention, as we do now. For this, we would
   need global `Retry` settings. Either defaults or user-settable.
 - When we add *one-shot sinks* later, we can add a state `FINISHED` that would
   indicate finished sinks.
 - Keep entries of dropped/failed/whatever connector in a
   `mz_connector_history` view. Or keep dropped connector in `mz_connector` for
   a while.

## Alternatives

We could introduce global settings for `timeout`, `num retries`, and
potentially `back-off interval`. Not all connector SDKs support specifying all
of these, but those that do would use the global configuration instead of
relying on arbitrary hard-coded values.

I don't think this is to valuable, because usually these are to connector
specific.

## Open Questions

There were some, which lead to the addition of a `DEGRADED` state above.

## References

 - Product thoughts from Nicolle: https://docs.google.com/document/d/10RsEnpJJBN-lQKyYl08KRQ15Ckn-_gCWa5MAnZcc-ts/edit#heading=h.yvax2aldz1n9
 - Meta-issue by Eli: https://github.com/MaterializeInc/database-issues/issues/2208

Critical:
 - https://github.com/MaterializeInc/database-issues/issues/2304: sink error during startup kills materialized
 - https://github.com/MaterializeInc/database-issues/issues/2180: failure in S3 source is printed to log but source doesn't error out
 - https://github.com/MaterializeInc/database-issues/issues/2051: transient broker failure leads to error in log, but source doesn't report as errored and doesn't continue to produce data

Unknown:
 - https://github.com/MaterializeInc/database-issues/issues/2187: fetching in source doesn't continue after transient failure
 - https://github.com/MaterializeInc/database-issues/issues/2153: metadata fetch errors in log

Related:
 - https://github.com/MaterializeInc/database-issues/issues/1024: "no complete timestamps" message is not useful
 - related: https://github.com/MaterializeInc/database-issues/issues/978

Future Work, aka. non-goals:
 - https://github.com/MaterializeInc/materialize/discussions/5340: more structured user errors (error, details, hints)

Merged:
 - https://github.com/MaterializeInc/materialize/pull/6952 graceful retries for S3
