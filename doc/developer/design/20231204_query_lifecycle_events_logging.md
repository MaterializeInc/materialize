# Statement Lifecycle Events logging

- Associated:
* Initial [conceptual doc](https://www.notion.so/materialize/A-Model-for-Materialize-s-Operation-831f0a35782547518a8c05617dc087ca?showMoveTo=true&saveParent=true) from Frank
* [Epic](https://github.com/MaterializeInc/database-issues/issues/7014) in GH.

## The Problem

Materialize is unable to communicate to users why it takes a long time
for their queries to return, which can roughly be broken down into:

* Time for all transitive source (in the storage sense) dependencies to reach the selected
  timestamp
* Time for compute dependencies to reach that timestamp
* Time for the statement to finish executing (this includes the top-level
  computation as well as returning its results to `environmentd`)
* Time for us to flush all results to the network.

With the activity log feature, users can now see how long it takes for
execution to finish (corresponding to the 3rd point above). However,
this has a few flaws:

* It is not granular enough: in general users (or field engineers)
need to know _why_ queries are slow in order to fix them. Is a compute
cluster rebooting? Is a storage cluster the bottleneck?

* We do not guarantee 100% sampling, so it's unreliable for debugging.



## Success Criteria

Users should have a reliable way to detect slow queries and
attribute them conceptually to one of the parts of the system as
described above.


## Out of Scope

### "Synchronous" sources

Frank's [initial
doc](https://www.notion.so/materialize/A-Model-for-Materialize-s-Operation-831f0a35782547518a8c05617dc087ca?showMoveTo=true&saveParent=true)
describes how Storage sources should be made to work in an ideal
world: after Materialize advances to a given timestamp, we should take
note of the amount of data present in the upstream system
corresponding to the source (for example, the max offset in Kafka),
and only advance the source to that timestamp when all that data has
been ingested. This is one of the parts of the currently
semi-abandoned project called "real-time recency".

I am leaving this out of scope for now as it would massively blow up
the size of the project (from adding some relatively simple logging to
conceptually re-architecting how Storage works). Unfortunately, this
means that the time at which Storage sources reached a particular timestamp
will not be very meaningful, as Storage is always allowed to just
record things at however advanced of a timestamp it wants.

However, we should still track and log how long it takes sources to
update to the chosen timestamp, in the hope that someday we will do
the work to make storage work this way.

### Time to result delivery

I originally attempted to report the time it took to deliver the
results over the network. However, after some thought, I realized
there's no good way to do this. We could try to detect when all the
bytes we've been sent have been `ACK`ed by repeatedly polling a
Linux system call to get this information. However, this would not
measure just the time to send the data, but also the time for the ACK
to be returned to us.

## Solution Proposal

Create a collection called `mz_internal.mz_statement_lifecycle_history`
with the schema `(statement_id uuid NOT NULL, event_type text NOT NULL,
occurred_at timestamp with time zone NOT NULL)`.

The `event_type` can be one of the following:

| type                         | description                                                                                                              |
|------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `execution-began`            | Corresponds to today's `mz_statement_execution_history.began_at`                                                         |
| `storage-dependencies-ready` | When all transitive source dependencies advanced to the timestamp. If there are none, same as `execution_began`          |
| `compute-dependencies-ready` | When all compute dependencies (MVs and Indexes) advanced to the timestamp. If there are none, same as `execution_began`. |
| `execution-finished`         | Corresponds to today's `mz_statement_execution_history.finished_at`                                                      |
| `last-row-returned`          | The time at which all rows were sent and we were ready to receive new statements from its session.                       |

When each event is generated, the coordinator will insert it into a
buffer which is periodically dumped to storage, similarly to the
statement logging events that exist today. Since the amount of data is
proportional to the number of statements, we don't envision that this
will result in new scalability concerns beyond those which already
exist for the statement log.

In the following subsections we will explain how to collect each of
the pieces of data.

### `execution-began`

We already collect this [in
`handle_execute`](https://github.com/MaterializeInc/materialize/blob/4ac31d85b7/src/adapter/src/coord/command_handler.rs#L407-L408).

### `storage-dependencies-ready` / `compute-dependencies-ready`

Introduce a mechanism for the sub-controllers (Storage and Compute
controllers) to return all frontier updates to the overall
Controller. When installing a query with the Controller, the
Coordinator will also inform it of one or more "watch sets" of
IDs. Concretely, the Controller will gain a function like:

``` rust
fn add_watch_set(&mut self, id: Box<Any>, global_ids: Vec<GlobalId>, ts: Timestamp);
```

The `id` here is arbitrary; the `Controller` does not attempt to
interpret it.

The Controller will then be expected to return responses of the form:

``` rust
WatchSetFulfilled {
    id: Box<Any>,
}
```

when the write frontier for all the global IDs in the set has passed
`ts`. The coordinator will then emit the corresponding events.

This is a new variant of the `ControllerResponse` enum, and contains
the arbitrary ID that was passed to `add_watch_set`.

### `execution-finished`

We already collect this in various places; the exact place it's logged
depends on the type of statement. When various statements are
considered finished is documented in the [Coordinator
Interface](https://github.com/MaterializeInc/materialize/blob/4ac31d85b7/doc/developer/reference/adapter/coord_interface.md)
document.

### `last-row-returned`

We would need to start sending the statement logging id to the client
(pgwire/sql) layer along with all execute responses. Currently, we
only send it with the execute response variants for which
`finished_at` is determined in that layer.

Whenever we call `flush` in the pgwire layer (or its equivalent in the
HTTP layer), we can log this event for all the statement logging IDs
for which we have enqueued responses since the last call to `flush`.

## Open questions

* Have we gotten the granularity of events right? I'm slightly
  suspicious of the split between "sources" and "compute
  dependencies". If a computation on one cluster depends on an MV
  being computed on another cluster, and so on, might we want to
  record an event when _each cluster_ in that chain has finished its
  work? On the other hand, this would start to duplicate the work for
  the Workflows Graph.
