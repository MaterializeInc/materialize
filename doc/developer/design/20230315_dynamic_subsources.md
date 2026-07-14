- Feature name: Dynamic Subsources

# Summary
[summary]: #summary

This design document describes the method by which a source that has subsources can be dynamically
altered, after its creation, so that subsources can be added or removed. At a high level we want
users to be able to type something like this:

```sql
CREATE SOURCE foo FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'pub') FOR TABLE t1;

# some time passes

ALTER SOURCE foo ADD TABLE t2;

# now t2 is available
SELECT * FROM t2;
```

# Explanation
[explanation]: #explanation

Dynamically changing the subsources of a multi-output ingestion starts out as an `ALTER SOURCE`
statement that specifies the desired change to be performed. This feature will add support for
removing a preexisting postgres table or adding a new one.

When any alteration on the ingestion is made the dataflows responsible for it will be teared down
and restarted with the new configuration. This means that the tables that were not affected by the
`ALTER` will too expierience a hiccup as the dataflows re-initialize.

## Removing a subsource

Removing a subsource is the most straightfoward of all. The adapter can immediately change its
catalog such that the `CREATE SOURCE` statement of the affected source has the named subsources
removed, and subsequently call the appropriate method on the storage controller.

The storage controller will then have to notify the `clusterd` about the new definition of the
ingestion so that the correct dataflows are installed. After switching out an ingestion that
removes subsources we should expect to hear back a `StorageResponse::DroppedIds` response from
`clusterd` at which point the controller will be free to tear down all state associated with the
dropped subsources.

## Adding a subsource

Adding a subsource works the same way as removing it with the only exception being that we might
need to go through purification if we don't already have the information we need about this table.
This can happen if tables were added to the publication after the original source was created in
materialize and the user elects to add one of those.

Once the new purified information is obtained we can again change the catalog definition of the
`CREATE SOURCE` statement and call into the storage controller to propagate the change.

## Time of appearance of new tables

Just like the tables that were created as part of the initial `CREATE SOURCE` command, tables that
are added through an `ALTER SOURCE` will be immediately querable and appear initially empty. The
ingestion dataflow will eventually snapshot the table and make it availalbe at a particular
timestamp, but that timestamp cannot be known by the controller ahead of time.

Fixing this will involve fixing correctness property 2 and is left as future work.

# Reference explanation
[reference-explanation]: #reference-explanation

## Per subsource resumption frontier

The biggest change that we'll need to implement to support dynamically adding subsources is to
compute resumption frontiers per subsource, instead of computing a single combined resumption
frontier for all subsources. We currently calculate the resumption frontier of an ingestion to be
the `antichain_meet` of the upper frontier of all the subsources. Essentially always resuming from
the least advanced subsource.

This is problematic when adding a new subsource, since its initial upper will be `T::minimum()` and
so if we were to apply the same logic we would eventually start an ingestion of `N + 1` tables at
`T::minimum()`. The source implementation, not knowing any better, would then proceed to
re-snapshot all `N + 1` tables and produce their incremental updates, which will all be descarded
by their respective persist sinks, except for the newly added table.

Therefore, we need to make sure that we communicate enough information to the source implementation
to know what needs we have for each of its outputs so that it knows which tables need to be
snapshotted and which ones can simply resume from the replication slot.

## Command stream changes

The command/response protocol between the storage controller and `clusterd` currently includes
`CreateSource` commands, which initialize an ingestion, and `AllowCompaction` comamnds, which tears
down the dataflow when its frontier is the empty frontier.

We have two options here. We can either send a brand new `CreateSource` command for the same global
id where the expectations are that the ingestion will be replaced with the new definition or define
a specific `AlterSource` storage command that specifies what changed. I think I'm leaning towards
re-stating what should be the current state instead of coming up with a way to describe the
alterations as this is simpler to implement but I haven't formed strong opinions on this one.
