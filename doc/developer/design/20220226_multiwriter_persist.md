## Overview

This document describes the design of a persistent collection implementation
with the following properties:

* Supports any number of writers that can come and go with zero coordination
* Supports any number of readers that can read with zero coordination
* Background compaction and garbage collection of no longer needed batches with
  zero coordination with readers or writers

The design is based on Differential Dataflow's [Spine][spine] data structure.

## Collection structure

A persistent collection is made up of consolidated batches of updates. Each
batch is described by a `lower` and `upper` frontier and contains all the
updates to the persistent collection that happened at times beyond `lower` and
not beyond `upper`. Batches are immutable and are never modified after their
creation. When they are no longer needed they are garbage collected.

The batches are organized in two separate areas, a **versioned** "merged area"
and a "pending area" that is sharded by writer id.

### The merged area

**Each version** of the merged area is made of a list of batches that are
contiguous in time. This means that the `upper` frontier of a batch in the list
is equal to the `lower` frontier of the next batch in the list. The merged area
is created as a result of a batch compcation and merging process (discussed
below) which incorporates new batches from the pending area into the merged
area and merges batches that have similar sizes together into one big batch.
This area is morally equivalent of the `merging` area of a `Spine`.

Similarly to the `Spine` implementation, a version of the merged area is made
of layers where every layer `i` can be inhabited by a batch that has at most
`2^i` updates. This exponential increase in batch size as we climb up the
layers causes the merged area to have a logarithmic **count** of batches, which
makes a complete description of it succinct and cheap to make copies of.

Each version of the merged area is durably recorded in a metadata file which
contains the list of batches in the merged area, its version number, and the
overall `lower` and `upper` frontier of the area.

Initially a persistent collection starts with a merged area at version `0` that
contains no batches.

### The pending area

The pending area of a persistent collection is not versioned (there is only
one) and is sharded by writer id. Each shard contains a list of batches
produced by a particular writer that are contiguous in time.

If a collection contains more than one writer shard then the batches of the two
shards must contain identical updates for all times `t`. If this invariant is
not upheld then the contents of the persistent collection are undefined.

Note that there is no requirement that the `lower` and `upper` frontiers of the
batches in two separate shards to match up. Only that the diffs at each
particular time are identical.

## Collection lifecycle

A persisted collection is managed by the PERSIST CONTROLLER which has the following API:

* `CreateCollection(id)` -> ReadHandle: Creates a new persistent collection
  identified by `id` and returns a read handle. After a persistent collection
  has been created anyone can publish new batches into its pending area.
* `AcquireReadHandle(id) -> ReadHandle`: provides a read handle at its current
  `since` frontier and referring at the latest merged area version.
* `DeleteCollection(id)`: Deletes a persistent collection identified by `id`

### Reading

A read handle of a persistent collection is described by a merged area version
and a compaction frontier. **Readers of a collection must use the batches
referred to by their current merged area version for any reads at times not
beyond the merged area's `upper`. For times beyond `upper` the pending area is
used instead.**

This behavior is the key to allow the PERSIST CONTROLLER to decide with
certainty which batches are reachable and perform background garbage collection
of unreachable batches.

When a reader wants to read at a time `t` that is beyond the merged area's
`upper` there might be multiple batches in the pending area that include that
time. This can happen in the case of multiple writers writing out a definite
collection. Readers are free to choose which batches to select based on any
strategy. For example, a reader could pick a writer shard to follow and only
switch to a different shard if its current shard is falling behind too much.
The trade-off for switching which shard is followed is the additional work that
needs to be done if the frontiers of the batches don't exactly line up.

Readers continually try to discover newly minted merged area versions and
increase their associated version to that, indicating that they will never use
the batch metadata of any previous version.

Readers are also able to downgrade their compaction frontier indicating that
they no longer care to distinguish updates that happened not beyond the
frontier.

Both compaction frontier downgrades and merged area version downgrades are
durably persisted.

A read handle can be created by cloning an existing read handle. The newly
created read handle will have its own independent merged area version and
compaction frontier with their initial values set to the values of the read
handle it was cloned from. This is also a durable operation.

### Writing

A writer to a persistent collection can initialize itself completely
independently simply by knowing the `id` of the collection. Writers are
identified by a name and it is up to the user to ensure that unique names are
chosen.

A writer writes to the collection by simply publishing new batches into its
corresponding pending area shard.

## Background maintenance

For each persistent collection an asynchronous background compaction and
garbage collection process is run by the PERSIST CONTROLLER. This controller is
a singleton process and it must be ensured that only one runs at a given time.

In the platform world this would be a process ran and managed by STORAGE.

### Compaction and merged area evolution

Periodically the PERSIST CONTROLLER attempts to create new versions of the
merged area. Its goal is to compact the merged area as much as posisble and
also include batches that have been published by writers in the pending area
into the merged area.

First a lower bound of the compaction frontier is determined by collecting the
compaction frontiers of all the read handles. This can be done by simply
reading their metadata and no coordination with readers is needed.

Then the merge and compaction process creates any new merged and compacted
batches that are needed.

Finally, the metadata file of this new merged area version is written durably,
which makes it atomically discoverable by the readers. As mentioned above, the
exponential increase in batch size as we go up the layers of the merged area
result in a logarithmic number of batches. A collection with 4 billion updates
would consist of only 32 entries. Therefore this version metadata will be
extremely small and cheap to copy even when the collections contain an enormous
amount of data.

### Garbage collection

Periodically the PERSIST CONTROLLER attempts to delete any batches that are no
longer needed.

First a lower bound of the needed merged metadata area version is determined by
collecting the merged area versions of all the read handles. This can be done
by simply reading their metadata and no coordination with readers is needed.

The `upper` frontier of the minimum merged area version is used to determine
the cut-off point before which batches from the pending area can be deleted.
This is safe because all readers promise to use the merged area's batches for
times that are within the merged area's range and each subsequent version of
the merged area must have an equal or greater. Therefore the garbage collector
proceeds to delete all batches in the pending area whose `upper` is not beyond
the cut-off point.

Then it proceeds to garbage collect batches from the merged areas. It collects
the set of active batches by reading the metadata of all the merged areas that
have a version greater than or equal to the minimum version and proceeds to
delete any merged area batches that are not in this set.

Finally, it deletes the metadata files for all version below the minimum version.

## Appendix

This is an example of what the durable state on disk could look like for a persistent collection:

```
.
├── merged_areas
│   ├── v0
│   ├── v1
│   ├── v2
│   └── v3
├── merged_batches
│   ├── batch_0_10
│   ├── batch_10_50
│   └── batch_50_100
├── pending_batches
│   ├── writer1
│   │   ├── batch_100_110
│   │   ├── batch_110_150
│   │   └── batch_70_100
│   └── writer2
│       ├── batch_105_120
│       └── batch_90_105
└── readers
    ├── reader1
    │   ├── compaction_frontier
    │   └── merged_version
    └── reader2
        ├── compaction_frontier
        └── merged_version
```

[spine]: https://docs.rs/differential-dataflow/latest/differential_dataflow/trace/implementations/spine_fueled/index.html
