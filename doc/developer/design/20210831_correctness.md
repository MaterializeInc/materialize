# Materialize Correctness Vocabulary

## Summary

A description of the correctness properties Materialize provides and the terms and concepts we use to describe them.

## Goals

The main goal is to lay the foundation for ongoing work to clearly articulate correctness properties of Materialize.
This includes the introduction of concepts that underlie correctness, the components that will enforce them, and how they can communicate clearly.

## Non-Goals

It is not a goal to prescribe fine detail in how each of the components should behave, or which should have certain responsibilities.
It is not a goal to prescribe which behavior Materialize should support, but only how to speak about a "correct" subset of it.
It is not a goal to advocate (much) for DEFINITE over NON-VOLATILE.

## Description

Materialize means to provide several correctness guarantees, which manifest as various consistency and durability properties.
At the heart of this is the idea that the ordering of events can be explicitly resolved and recorded, after which queries and views over the data become a matter of correct evaluation on completed subsets of time.
The guarantees of Materialize derive from the durability of these recorded data, and the determinism of views over them.

The primary term introduced here is "definite" to modify a collection of data.
A collection of data is "identified" either nominally (by name) or structurally (as a query over named things).
A collection is *definite* when all uses of any one identification will yield exactly the same time-varying collection of data (identical contents at each logical time).
A handle to a collection can be "definite from `since` until `upper`" (names pending) if it reflects the definite results for times greater or equal to `since` but not greater or equal to `upper`;
this intentionally allows for compaction of historical detail and unavailability of future data, without ambiguity about which logical times can be described with certainty.

Among the correctness guarantees of Materialize, a main one will be statements of definiteness over intervals of times for collections it maintains.

Definiteness is a property in part of how we name things.
The contents of a `CREATE SOURCE` statement may not be enough to make a source definite.
Such a source may only become definite once its description makes its way to the persistence layer, responsibility is assumed, and a name is returned.
These names almost certainly want to contain a nonce corresponding to the persistence instance, so that they are not unintentionally equated across instances.
It is fine to scope definiteness more narrowly, e.g. to session-local name bindings, but if names leak beyond that scope they should be flagged as not definite.

### Components

There are three components to discuss: storage, compute, and coordination.

A **storage layer** (`persist`, but potentially others like it) presents streams of updates to collections to other parts of the system (primary the compute layer).
One correctness goal of the `persist` module is to make as many update streams as possible "definite", even if the raw data are indefinite (e.g. perhaps they contain no timestamps, which must be added).
The storage layer does not need to capture the entire contents of a collection to make it definite; durable "offset"-to-timestamp bindings can suffice if the source data are durable but without timestamps, and no data may be required if the source data are durable with timestamps (e.g. MzCDC sources).
Update streams that are definite form the "durable storage" of Materialize, and are the basis for Materialize's durability guarantees.

A **compute layer** (`dataflow`, but potentially others like it) consumes streams of updates to collections, and computes and then incrementally maintains the results of views over these data.
A view is ideally "deterministic" in the sense that it acts as a function on the input update streams, and produces a well-defined output on definite input (we can allow "non-deterministic" views but like "indefinite streams" they taint their results).
One correctness goal of the `dataflow` module is to convert definite inputs into definite outputs, which can be returned to the storage layer or to interactive user sessions.
Views and their dependencies form the "concurrency control" of Materialize, in that outputs are reported as definite only when, and ideally as soon as, their inputs are definite.

A **coordination layer** (`coord`, but potentially others like it) interfaces with user sessions and pilots the storage and compute layers.
It maintains mappings from user names to storage and compute objects (source, views, indexes), and dereferences them in ways that provide a consistent experience for users.
It also chooses timestamps for reads (peeks) and writes (table inserts) to provide the lived experience of continually moving forward through time.
It pilots the storage and compute layers and is responsible for ensuring they communicate clearly and completely with each other.
One correctness goal of the `coord` module is to offer [strict serializability](https://jepsen.io/consistency/models/strict-serializable) to the interactive users, through the careful use of the storage and compute layers.
Another correctness goal of the `coord` module is to ensure that the storage and compute correctly and completely interoperate (e.g. that the compute layer is invoked only on definite inputs, and fully communicates its definite outputs back to storage if appropriate).

### Approaches

The plan of record is that source data are made definite by the storage layer, and results are definite when observed through the compute layer (applied to definite inputs).
Definite data are both durable, and ordered in time (strictly sequentially consistent), and reflect the stated "order" the system presents as having undergone.

The coordination layer has several constraints on its behavior to present as strictly sequentially consistent.
The coordination layer should only report definite data, or indefinite data that are clearly presented as such (the value of definiteness vanishes if there is uncertainty about the definiteness).
The coordination layer's catalog should advance in a similar timeline as the data it references, to avoid apparent mis-ordering of DDL and DML statements.
The coordination layer may need to wait on data becoming definite when returning from e.g. INSERT statements where the return communicates durability.

Data the are not definite should be clearly marked as such, and there are fewer (no?) guarantees about their results.

### Externalizing Consistency

Materialize interacts with systems through mechanisms other than interactive SQL sessions.
For example, Materialize may capture the CDC stream from an OLTP system, and correlate its own output with the transaction markers in the CDC stream.
To provide end-to-end strict serializability guarantees, Materailize must either synchronize its *behavior* to that of the OLTP system, or present its output clearly enough that a user can subsequently synchronize the results.
Materialize does the latter, by providing explicitly timestamped data in various update streams.

Data that are provided as update streams, not through interactive sessions, are treated as occurring at the time assigned to them.
They are not subject to the real-time requirements of strict serializability in the same way that foreign tables are not.
In particular, their *real time occurrence* does not need to overlap with their explicitly recorded logical time.
Anyone reasoning about the ordering of these events should treat each event as occurring as if at the explicitly recorded logical time, rather than the physical time.
Anyone who instead "observes" an event substantially before or after it is stated to occur may fail to build a strictly serializable system from parts that include Materialize.

The definiteness of results provide the tools necessary to build strictly serializable systems out of parts that include but are not limited to Materialize.

### Describing Definite Data

Definite data follow patterns set up by differential dataflow.
Definite data are logically equivalent to a set of update triples `(data, time, diff)` which can be accumulated to determine the contents of the collection at a target `time`.

Any collection of definite data may only be correctly accumulated for some subset of times, for two reasons:
1. A definite collection may not yet be complete for times greater than or equal to a frontier `upper`.
2. A definite collection may have compacted updates not greater than or equal to a frontier `since`.

Definite collections are more precisely "definite for times greater than or equal to `since`, but not greater than or equal to `upper`".
The expectation is that `upper` will likely advance as more data arrive, and that `since` may advance if one does not explicitly ask the system to prevent this.
References and handles to definite data should most commonly have both `since` and `upper` tracked explicitly, so as to be as clear as possible about its nature.

In differential dataflow, one can explicitly hold back the `since` frontier of compactable data, which ensures that historical detail is maintained.
It is only by holding such a "historical capability" that one can be certain that they can access historical detail.
The `dataflow` module provides this ability for indexes it maintains.
It seems likely that the storage layer will want a similar abstraction for sources that it maintains (both explicitly as data, and implicitly as timestamp bindings).

## Alternatives

We have few alternatives that I am aware of, other than more ad hoc correctness guarantees.

We have the term [VOLATILE](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210401_volatile_sources.md) to represent the opposite of DEFINITE.
We could use whatever term, though I recall having some confusion about VOLATILE and whether it is the opposite of DEFINITE (I believe stemming from the pre-Platform era where it wasn't clear if the requirement was required across MZ instances).
I chose a new name to avoid that question, but perhaps we should answer the question at least to ensure that we have a specific, ideally common, meaning in place.
I believe the spirit is very similar between the two; possibly identical.
At the same time, the VOLATILE doc above enumerates several sources that are not (at that time) DEFINITE, in that they do not have durable timestamp bindings.
I have no opinion on whether the definition of VOLATILE is in error, or the application of it to these sources, but I would want to be clear that those sources are not what is meant here by DEFINITE.
DEFINITE has the benefit over NONVOLATILE that it can be qualified by `since` and `upper` without suggesting that outside that range it might just be VOLATILE, but this is bikeshedding we can do using whatever mechanism is appropriate for that.

Kafka provides the opportunity to handle similar properties with transactions, and exactly once semantics.
I believe these are not sufficiently expressive to provide consistency across multiple data streams, as afaik although the writes are transactional, one cannot then subsequently correlate transaction boundaries.
I believe this means that one cannot provide strong consistency across computations that span multiple related topics, unless all writes to these topics are transactional (plus perhaps other requirements).

Traditional OLTP databases use more bespoke concurrency and durability controls.
We could look at something similar (and probably should be aware of capabilities they have that this approach will never have), but by doing such we leave behind assets we are currently sitting on (differential dataflow).

## Open questions

The main ask is whether we feel that the constituent parts are comfortable speaking about their guarantees in the terms provided here.
There may be new idioms and interfaces we need to invent, but if we can agree that this framing is appropriate, we can then fill in the details with time.

There are many details to fill in.

Nothing has been said about the responsibilities of the storage, compute, and coordination layers.
Roughly, the framing above has "making things definite, expanding definite results, and piloting things", respectively.
But, there are reasonable questions like "should the coordinator mediate communication of definiteness from the compute to the storage layer?"
Should any of these components be explicitly agnostic to concepts maintained by the others?

Nothing has been said about the specific APIs of the storage, compute, and coordination layers.
This will probably involve a substantial amount of iteration as we refine who knows what, and who needs to learn it.
At the least, I imagine storage will need to learn about source descriptions and timestamping policies, compute will need to clearly delineate what data it can act on for which times, and the coordinator will want to have its fingers in all of these pies.

What concepts does each layer need to expose to other layers to ensure their correct operation?
If each wanted to be a stickler for detail, what would they require to make sure they weren't the source of correctness errors?

Not much is said here about properties of VOLATILE / INDEFINITE collections.
Perhaps there are correctness requirements we should provide for them.
For example, that each view results in the correct evalution on *some* data; "no impossible results".
Almost certainly "no panics" (so no optimizations that are incorrect for VOLATILE sources).
Should VOLATILE sources be best-effort only, and who would complain if each VOLATILE source were permanently empty / just very, very slow?
