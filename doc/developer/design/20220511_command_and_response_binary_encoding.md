# Command and Response Binary Encoding

The contents of this design doc are a cleaned up version of a subset of [the "Protobuf Support for ComputeCommand" Google Doc](https://docs.google.com/document/d/1OBc3bDZa6ag3a-LoBV8JqMqJ9CKv2qQz2IVgD6TcpjI/edit#heading=h.8ujsjdf1jfuc), which was the primary design doc while working on #11735.

## Summary

As part of the [Platform initiative](https://github.com/MaterializeInc/materialize/tree/main/doc/developer/platform), `materialized` will be broken down into different processes that communicate through a set of well-defined APIs comprised of *commands*[^cc][^sc] (a.k.a. *requests*) and *responses*[^cr][^sr].
API messages therefore need to be serialized into a backwards-compatible binary format in order to facilitate inter-process communication and replay-based failure recovery.
This document proposes an encoding to be adopted for these purposes based on Google's Protocol Buffers[^pb] (a.k.a. Protobuf).

## Goals

1. Propose a general strategy for serializing and deserializing API message types into a binary format that can be sued both for (a) network transfer and (b) durable storage.
2. Consider how the proposed strategy will facilitate maintaining backwards-compatibile code as the set of commands and responses in the API evolves over time.

## Non-Goals

1. Go into implementation specifics for the proposed design.
   These will be discussed into a separate implementation guideline document, which should be  refined and kept up to date as the design evolves.
2. Consider approaches that are not based on Protobuf.
   Those are omitted for the following reasons.
   First, other parts of the codebase (for example `persist`) already use Protobuf, and there is a large overalop of the set of types used both by `persist` and the current API (for example, everything under `Row`).
   Second, Protobuf nicely integrates with adjacent technologies (such as [gRPC](https://grpc.io/)) that might be of interest in the near future.

## Description

We delegate the heavy-lifting of the serialization/deserialization efforts and large chunks of the backwards-compatibility story to Protobuf.
Following suit from the rest of the codebase, the Protobuf integration is handled by the `prost`[^prost] library.

Given the fact that we want to retro-actively add Protobuf support for all Rust types used in the current API, we have the following strategies:

1. ❌ [Migrate the existing Rust types to `*.proto` files](#migrate-the-existing-rust-types-to-proto-files).
2. ❌ [Annotate the existing Rust types with `prost` attributes](#annotate-the-existing-rust-types-with-prost-attributes).
3. ✔️ For each existing Rust type `$T`, create a mirroring type `Proto$T` and define a pair of conversion functions that mediate between the two types.

The design proposed here is based on (3) because this strategy offers the highest degree of flexibility with respect to backwards compatibility.
The pros and cons of (1) and (2) are discussed in the [Alternatives](#alternatives) section.

**Pros**

The selected strategy offers flexibility due to the separation of the serializable type (`Proto$T`) from the client facing type (`$T`). In particular:

1. We are free to evolve `$T` as usual and keep client code simple.
   The technical complexity caused by backwards-compatibility guarantees is accumulated in the `Proto$T` and the associated `Proto$T ⇒ $T` conversion function.
2. We have an obvious solution to the mismatch between the Rust types and the types derived from the `*.proto` defs by `prost-build` (see the rejected [alternatives](#alternatives) for details).
   The existing Rust type `$T` can remain unchaged, while `Proto$T` can deviate in a predictable and consistent way based on the limitations of `prost-build`.
   Moreover, we can offer library functions that mediate between `$T` and `Proto$T` in a consistent way across the codebase.
   For example, we can enforce that the Rust type `usize` is always represented by the Protobuf type `uint64`.

**Cons**

1. Requires the most amount of upfront work, as we need to
   1. define `Proto$T` in a `*.proto` file,
   2. wire up `prost-build` to generate the `Proto$T` Rust type, and
   3. define `$T ⇔ Proto$T` for each `$T`.
2. Requires the most amount of maintenance work, as the same boilerplate needs to be added for each new type `$T` used by the API.
3. Accumulates technical debt, as the `$T ⇔ Proto$T` for complex type is coing to be recursive and therefore susceptible to stack overflow issues (see #9000).
4. We have to pay the runtime and memory penalty if mediating between `$T` and `Proto$T`, possibly in the the hot paths of some processes.

## Alternatives

### Migrate the existing Rust types to `*.proto` files

With this strategy, we will:

1. migrate each existing Rust type `$T` to a corresponding Protobuf message type,
2. derive `$T` from this message using `prost-build`, and
3. replace the original `$T` with its derived version in client code.

**Pros**

1. Requires moderate amount of upfront work.
2. Flexible, because we can reuse the `*.proto` files to derive messages in other languages.
3. Memory-efficient, because client code operates directly on the deserialized Protobuf messages.

**Cons**

1. Reworks most of the existing code.
2. Limited opportunities for introducing backwards-compatible changes to existing types.
3. There is a mismatch between the Rust types that can be derived from `*.proto` message definitions by `prost` and the existing Rust types, so we will need to touch client code. For example:
   1. Enum variants should have exactly one parameter.
   2. Some types that are not directly supported (`usize`, `chrono` types, tuples).
   3. Some of the types are generic (Plan<T>).

### Annotate the existing Rust types with `prost` attributes

With this strategy, we will add Protobuf serialization and deserialization support directly to the existing types by annotating them with `prost-derive` macros (e.g. `::prost::Message`).

**Pros**

1. Requires the least amount of upfront work for types that can be represented as Protobuf messages.
2. Requires the least amount of maintenance work as long as long as we don't change the shape of existing types.
3. Flexible and memory-efficient (same as the other rejected alternative).

**Cons**

Same as for the other rejected alternative.

## Open questions

- To we care about the performance overhead introduced by the additional `$T ⇔ Proto$T` step at the moment? If yes, we need to run some benchmarks to quantify this.

## Appendix A: Related Discussions

1. [`#eng-storage` regarding protobuf representation for `Row`](https://materializeinc.slack.com/archives/C01CFKM1QRF/p1648227606162479)
1. [`#eng-persist` regarding adopting the Codec trait](https://materializeinc.slack.com/archives/C011P87EL2V/p1649249398798509)
1. [`#team-status` thread prior to the meeting on 2022/03/29](https://materializeinc.slack.com/archives/CV33ZAMNH/p1648564370165509)
1. [`#help-rust` thread regarding usize handling](https://materializeinc.slack.com/archives/CMH6PG4CW/p1648638942706409)
1. [`#prost` Discord channel Q1: cross-crate imports](https://discord.com/channels/500028886025895936/664895722121986061/956984930158661632)
1. [`#prost` Discord channel Q2: blanket implementations for tuples](https://discord.com/channels/500028886025895936/664895722121986061/958424010364977192)
1. [`#prost` Discord channel Q3: `FileDescriptorSet` handling](https://discord.com/channels/500028886025895936/664895722121986061/958649689278939156)
1. [`#prost` Discord channel Q4: (`Optional<T>` handling in `proto3`)](https://discord.com/channels/500028886025895936/664895722121986061/958707895900463154)
1. [Notes from a meeting on 2022/03/29 with Daniel Harrison and Moritz Hoffmann](https://docs.google.com/document/d/1Qhddyp324srA8egvUHfb3AEkd0_5Wk-GfL5Ozs1oNeg/edit)

## Appendix B: References

[^cc]: [`mz_dataflow_types::client::ComputeCommand`](https://github.com/aalexandrov/materialize/blob/8d1f3fb92a9b0c42496a6dd2e330c1a7c221e137/src/dataflow-types/src/client.rs#L144-L177)
[^cr]: [`mz_dataflow_types::client::ComputeResponse`](https://github.com/aalexandrov/materialize/blob/8d1f3fb92a9b0c42496a6dd2e330c1a7c221e137/src/dataflow-types/src/client.rs#L452-L461)
[^sc]: [`mz_dataflow_types::client::StorageCommand`](https://github.com/aalexandrov/materialize/blob/8d1f3fb92a9b0c42496a6dd2e330c1a7c221e137/src/dataflow-types/src/client.rs#L207-L231)
[^sr]: [`mz_dataflow_types::client::StorageResponse`](https://github.com/aalexandrov/materialize/blob/8d1f3fb92a9b0c42496a6dd2e330c1a7c221e137/src/dataflow-types/src/client.rs#L463-L473)
[^pb]: [Protocol Buffers](https://developers.google.com/protocol-buffers)
[^prost]: [`prost` GitHub page](https://github.com/tokio-rs/prost)
