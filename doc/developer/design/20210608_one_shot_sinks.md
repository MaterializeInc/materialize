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


## Goals

- Re-classify existing sinks as _one-shot sinks_ where it is appropriate. I
  propose that all sinks except Kafka with exactly-once are one-shot sinks for
  now.

## Non-Goals

- Change the behavior of existing sinks.
- Add any new sinks, formats, envelopes, what have you.

## Description

Currently, Materialize knows only one type of sink: _continuous sinks_. They
continuously write new data when the sinked relation changes. When Materialize
is restarted, continuous sinks are restarted as well, and they continue producing
data. I submit that not all sinks work well with this model, that there are in
fact use cases for what I will call _one-shot sinks_ (name TBD), and that some
of our existing sinks should be re-classified as one-shot sinks.

The proposed one-shot sink writes data only up to "now" (or some user-specified
time) and then finishes. One-shot sinks are not restarted when Materialize is
restarted.

This will make it clearer to users and the system what sinks can do and should
make it easier for us to add sinks for just getting data out of Materialize
without worrying about things like exactly once. This latter point was the
initial insight that sparked this proposal. With this we neatly sidestep the
issue that some sinks don't behave well when restarting. Think nonces in the
Kafka topic names and/or changing filenames for OCF sinks which can be
problematic in production use cases.

This is also motivated by the opinion that continuous sinks should behave well
when Materialize is restarting. This is currently only true for the Kafka sink
with exactly once/consistency. Just requiring that all the sinks we offer
support this, without providing a work-around seems difficult to digest,
though. Hence this proposal.

See also the recently introduced [volatile
sources](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210401_volatile_sources.md)
which added a keyword for sources to untie our hands, simplify requirements,
and clarify behavior.

### Proposed Syntax Changes

Continuous sinks should keep using the `CREATE SINK ...` syntax while one-shot
sinks would use an extended version of the existing `COPY TO ...` syntax.

Concretely, a sink is currently created as:

```sql
CREATE SINK sink_name FROM item_name INTO <sink definition>
```

The new proposed syntax for `COPY TO` is:

```sql
COPY (query) TO <sink definition>
```

Where `<sink definition>` should in theory be compatible with both `CREATE
SINK` and `COPY TO` but the sink configuration will determine if a sink is
usable as a continuous sink or one-shot sink. This is similar to volatile
sources, where the volatility label is not necessarily attached to a specific
source type but depends on the concrete instance configuration.

### Immediate User-facing Changes / Breaking Changes

- Avro OCF sinks are not usable with `CREATE SINK` anymore, only write data up
  to "now" and then finish
- Kafka sinks without exactly-once/consistency are not usable with `CREATE SINK
  anymore, only write data up to "now" and then finish
- We need to massage the docs to treat `COPY TO` and `CREATE SINK` together but
  not confuse people too much

### Unified Sink Pipeline

The actual implementation of sinks will not differ between the sink types. In
the long run, we should probably move all sink like concepts to be considered
as sinks internally.

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

I'm proposing that one-off sinks still be referred to as sinks. As such, they
should still feature in those builtin tables. It's more complicated to have
multiple, differently named tables, more concepts that we need to explain to
users. We should add a column in `mz_sinks` that indicates if a sink is a
one-shot sink, though.

For one-shot sinks, it seems good to keep them around in those tables even
after they are finished. The tables are useful for determining what file or
Kafka topic we wrote to.

This part is still an open question, though. If we decide that one-shot sinks
should be more thoroughly separated from "actual" sinks. We have to use
different naming here and create separate builtin tables.

### Future work

- add useful one-off sinks and formats

## Alternatives

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->

## Open questions

### Naming

Do we want to introduce formal names for these? That is, something like
_continuous sink_ and _one-shot sink_.

Potential other names are:

 - ephemeral sink
 - snapshot sink
 - volatile sink

### Should one-shot sinks and continuous sinks both be referred to as sinks in our messaging/documentation?

I think we should keep referring to both of those concepts as sinks. That's
what we have used so far and they are both, in fact, sinks.
