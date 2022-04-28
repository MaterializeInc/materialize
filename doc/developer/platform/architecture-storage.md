## Storage architecture

STORAGE is responsible for creating and maintaining definite time varying
collections (TVCs) that contain either ingested data from an external source
(e.g Kafka), data sinked from one or more CLUSTER instances, or data coming
from `Insert` requests to the storage controller, providing a table-like
experience.

## The STORAGE collection structure

![STORAGE collection structure](assets/storage-architecture-collection.png)

### Overview

STORAGE is tasked with managing the lifecycle of STORAGE collections (just
collections hereafter). Collections support concurrent, sharded writes from
multiple CLUSTER instances configured in active replication mode. Furthermore,
collections allow writers to "start from scrach" by recreating a completely new
version (rendition) of the collection, perhaps because the writer fixed a bug
that results in slightly different data or because a re-shard is necessary.

A collection consists of one or more renditions. A rendition is a standalone
version of the collection. Each rendition can have one or more shards where the
number of shards is selected at creation and remains static for the lifetime of
that rendition. Each shard maps 1-1 to a `persist` collection.

At any given time `t` the contents of the collection are defined unambiguously
by the contents of **exactly one** of the renditions. The information
describing which rendition is active at any given time is recorded as part of
the metadata of the collection. Readers use this metadata to know the exact
time `t_handover` that they should switch from one rendition to another. This
ensures that even though the collection can transition through multiple
renditions over time every reader always perceives a definite collection
advancing in time.

A shard of a rendition is the endpoint where writing happens. Writers insert
batches of data in a shard by using a write handle on the underlying `persist`
collection backing the shard.

### Renditions

A rendition represents a version of a collection and is described by its name,
its `since` frontier, and its `upper` frontier. The `since` frontier is defined
as the maximum of the `since` frontiers of its shards and its `upper` frontier
is defined as the minimum of the `upper` frontier of its shards (replace
max/min with join/meet for partialy ordered times).

#### Rendition metadata

The contents of the collection overall are described by a metadata `persist`
collection. This rendition metadata collection evolves in time and can be
thought of as instructions to readers that tells them what data they should
produce next. It containing updates of this form:

```
(rendition1, 0, +1)
// At `t_handover` readers should switch from rendition1 to rendition2
(rendition1, t_handover, -1)
(rendition2, t_handover, +1)
```

Readers hold onto read capabilities for the metadata collection that allows
STORAGE to know which renditions should be kept around and which ones should be
garbage collected. In the example above, when all readers downgrade their read
capability beyond `t_handover` the metadata collection can be consolidated and
`rendition1` can be deleted.

Readers interpret advances of the medatata frontier as proof that no rendition
change occured between the previously known frontier and the newly advanced
frontier. Therefore they can proceed with reading their current rendition for
all times not beyond the frontier of the metadata collection.

Readers encountering a change in rendition at some handover time `t_handover`
must produce the necessary diffs such that the resulting collection accumulates
to the contents of `rendition2` at `t_handover`. A simple strategy for doing so
is to produce a snapshot of `rendition1` at `t_handover` with all its diffs
negated and a snapshot of `rendition2` at `t_handover`. More elaborate
strategies could be implemented where for example STORAGE has precalculated the
`rendition2 - rendition1` difference collection for readers to use.

### Shards

A shard is part of a rendition and is the endpoint where writers publish new
data. They can be written to by multiple workers as long as all the workers
produce identical data (i.e their outputs are definite).

It is an error to have multiple writers write conflicting data into a shard and
the contents of the collection are undefined if that happens.

## Components

### Fat client

STORAGE provides a fat client in the form of a library that can be linked into
application that which to consume or produce collections. The fat client will
at the very least be able to import a collection into a timely dataflow cluster
and will handle all the rendition/shard handling logic internally.

### Controller

The STORAGE controller is a library and is hosted by ADAPTER. Its job is to
mediate all STORAGE commands that create, query, and delete collections, create
and handover renditions, and manage read/write capabilities for the collection.
The formalism document expands on the expected API of the STORAGE controller.

All commands are durably persisted and their effect survives crashes and reboots.

## Source ingestion

In the case where the controller is requested to ingest a source into a
collection, an ingestion pipeline must be constructed. When running in the
cloud setting each ingestion instance for a particular source will be hosted by
a dedicated kubernetes pod but when running in the single binary `materialized`
setting each ingestion instance will translate to a particular timely dataflow
being deployed to the STORAGE timely cluster.

For external users of STORAGE, the only thing they will be able to see is the
definite collection containing the result of the ingested data after all
requested processing has been performed (e.g decoding and envelope processing).

However, there are good reasons for STORAGE to internally create and manage
multiple colletion that work together to form the final collection. For
example, in the case where STORAGE is instructed to ingest a source that
requires Avro decoding and DEBEZIUM envelope processing, STORAGE might decide
to first create a definite collection of the raw data before decoding and
envelope processing and compute the final collection based on that. This
ensures that STORAGE will always have the ability to fix bugs in the Avro
decoding logic and generate new renditions based on the original raw data that
have been safely stored in the raw collection.

![source pipeline](assets/storage-architecture-sources.png)
