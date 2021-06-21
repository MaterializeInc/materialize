# Differentiate between continuous/"true" sinks and ephemeral, one-shot sinks

## Summary

Some of our users might want an easy way of getting the current data in a view
out of Materialize, without worrying about a continuous stream of changes,
exactly once, and changing topic names or file names. This is at odds with how
current sinks work and at odds with potential stricter requirements of what I'm
retroactively calling _continuous sinks_.

I propose to add the concept of _one-shot sinks_, which only write out data up
to a point and then finish. They should have relaxed requirements and should be
easier to implement in the future.

To facilitate this, we will formally introduce the concept of a _connector_ in
the documentation. A connector is a description of an external system that is
used when creating a source, (continuous) sink, or the new one-shot sink. To
differentiate the new style of sinks, we don't call them sink but instead refer
to them as _exports_ in our messaging/documentation/marketing.

_Below I will often refer to exports as sinks, because in the implementation
they are very close to sinks._

## Goals

- Rework documentation to lift connectors from the sink documentation and
  document them as a concept
- Add export implementations for existing sinks (kafka and avro ocf)
- Add documentation for exports

## Non-Goals

- Add any new sinks, formats, envelopes, what have you.

## Description

Currently, Materialize knows only one type of sink: _continuous sinks_. They
continuously write new data when the sinked relation changes. When Materialize
is restarted, continuous sinks are restarted as well, and they continue producing
data. I submit that not all sinks work well with this model, that there are in
fact use cases for what I will call _one-shot sinks_.

The proposed one-shot sink writes data only up to "now" (or some user-specified
time) and then finishes. One-shot sinks are not restarted when Materialize is
restarted.

This will make it clearer to users and the system what sinks can do and should
make it easier for us to add sinks for just getting data out of Materialize
without worrying about things like exactly once. This latter point was the
initial insight that sparked this proposal. With this we neatly sidestep the
issue that some sinks don't behave well when restarting. Think nonces in the
Kafka topic names and/or changing filenames for OCF sinks which can be
problematic in production use cases. We still keep the existing behaviour
around, in order not to break things but using exports with those types of
sinks will simplify things for users.

### Proposed Syntax Changes

Continuous sinks should keep using the `CREATE SINK ...` syntax while one-shot
sinks/exports would use an extended version of the existing `COPY TO ...`
syntax.

Concretely, a sink is currently created as:

```sql
CREATE SINK sink_name FROM item_name INTO <sink definition>
```

The new proposed syntax for `COPY TO` is:

```sql
COPY (query) TO <sink definition>
```

Where `<sink definition>` should be compatible with both `CREATE SINK` and
`COPY TO`.

### Unified Sink Pipeline

The actual implementation of sinks will not differ much between the sink types.
In the long run, we should probably move all sink like concepts to be
considered as sinks internally.

### What time interval to write out? (`AS OF` and/or `UP TO`)

One-shot sinks write data only up to a point in time and then finish. Should
this point in time be configurable? Should it be "now", whatever that could
mean?

We have to concept of `AS OF`, which is a lower bound for updates. It says:
please emit updates from this time onward. We could think about introducing `UP
TO <time>` for one-shot sinks which allows users to specify up to which time
they want updates to be written.

I'm leaning towards not making this configurable initially and instead only
write up to some notion of "now". That is we would snapshot the state of a
view/relation when the one-shot sink is created.

### Builtins for Sink-like objects

We have system tables for querying information about sinks. Today these are
`mz_sinks`, `mz_kafka_sinks`, and `mz_avro_ocf_sinks`.

I'm proposing that one-shot sinks still be referred to as sinks. As such, they
should still feature in those builtin tables. It's more complicated to have
multiple, differently named tables, more concepts that we need to explain to
users. We should add a column in `mz_sinks` that indicates if a sink is a
one-shot sink/export, though.

For one-shot sinks, it seems good to keep them around in those tables even
after they are finished. The tables are useful for determining what file or
Kafka topic we wrote to.

This part is still an open question, though. If we decide that one-shot sinks
should be more thoroughly separated from "actual" sinks. We have to use
different naming here and create separate builtin tables.

### Future work

- add useful one-shot sinks and formats

## Alternatives

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->
