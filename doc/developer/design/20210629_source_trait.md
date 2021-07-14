## Overview

This design document aims to define with precision the definition of a source
type. The definition should be complete and cover all aspects of a source, from
sql parsing and planning, to managing external state, to ultimately getting data
in dataflows.

## Goals

* Define a family of traits that capture the behaviour of a source. A correct
  implementation of those traits should be all that's needed to teach
  materialize how to ingest a new source
* Consolidate all the various bits and pieces needed for a source under a
  separate crate.  A minimal source implementation currently requires [touching
  many files](https://github.com/MaterializeInc/materialize/pull/6189) in many
  different crates often in repetitive ways.  In an ideal world you could
  imagine having `src/sources/src/postgres`, `src/sources/src/kafka`, etc, with
  each module containing the corresponding trait implementations and the rest of
  the system automatically picking them up.

## Assumptions

The traits defined below have been based on a few assumptions about the system.
The assumptions are listed here explicitly to both explain the following traits
and also debate them on their own.

* **We need to track and gracefully cleanup external state every time we talk to
  an external system**

For some external systems (e.g Postgres) we need to create foreign objects in
order to consume their data. It is desirable that materialize automatically
creates and crucially, cleans up this state when a dataflow is retired or a user
`DROP`s the source. The consequence of stating this assumption is the definition
of the `InstanceConfig` trait that must be able to be `Serialize`d into the
catalog.

* **Materialize is a multi-node system, with the timely cluster being separate
  from the coordintator node.**

This assumption drives the separation between the `InstanceConfig` and the
`Instance` itself. From the previous assumptions it followed that whenever the
coordinator decides to read a source it needs to create and persist its
configuration. Since the workers that will actually consume this source might be
on another machine the `InstanceConfig` must sendable over the network. It is
only when it reaches the final worker that it can be converted into the actual
`Instance`.

* **A source instance might benefit from shared state per node**

An example of transient state could be a TCP connection that can be reused
between many `Instance`s on workers of the same node. For this reason, each
`SourceConfig` implementation can define a `State` type that will contain any
shared resources between instances. The indended use of this type is to
instanciate a singleton per-node and use it any time a source is instantiated.

It is crucial that any state stored in `State` be considered transient and safe
to lose at any point since it is not persisted in the catalog.

* **Source SQL syntax is simple enough to derive directly type definitions**

This is more of an assumption for the future rather than a statement about the
current sources since if we look at the existing ones we can find violations of
it. The goal of this assumption is to strongly urge new source implementations
to rely on just their struct definition and a `Deserialize` implementation for
their parsing needs instead of parsing directly. The benefit is twofold. Firstly,
it is much easier to define the expected syntax, and secondly our syntax will
naturally follow a pattern as a consequence of not giving arbitrary access to
the parser.

## Source architecture

The following diagram describes the state of the system after a `CREATE
MATERIALIZED SOURCE foo ...` statement. Here is what happened.

When the user typed the statement, the system used `Source::Sql` to parse the
options specified. Then, the system constructed a `Source` using
`Source::from_sql`, which had the opportunity to validate the parameters. Since
`Source` implements `ToSql`, the created instance is serialized and stored in
the catalog in order to re-create it on restarts.

Since the user requested a materialized view the system begins to instanciate
the source. To do that, the coordinator called `Source::create_instances(4)` to
signal to the source that there are 4 workers available. This particular
source has only three shards, and so it returned three `Config`s to the
coordinator. These `Config` objects contain the necessary information to be
converted into Instances once they reach the dataflow layer. `Config` are
serialized and stored in the catalog so that cleanup can be performed once the
source is `DROP`ed. Ultimately, these `Config` objects are packed into a
`DataflowDesc` and sent to the dataflow layer for instantiation.

In this example, the dataflow layer consists of two nodes with two theads each.
Since there were only three `Config` objects in the received `DataflowDesc` the
first node is assigned the first two, and the second one the third. This
assignment is arbitrary and can be based on more elaborate logic to balance the
work out.

The nodes are now ready to create `Instance`s from each `Config` and start
receiving data into the dataflow. They do this using `Config::into_instance()`,
which requires a mutable reference to a `State`. Each dataflow node constructs,
and keeps around, a `State` object that it re-uses across all instantiations of
a `Config`.

[![https://imgur.com/8jx4oll.png](https://imgur.com/8jx4oll.png)](https://imgur.com/8jx4oll.png)

```rust
/// The trait that defines a new kind of source.
trait Source: ToSql + FromSql {
    /// Type holding any parameters than can be specified using SQL keywords.
    /// For example CREATE SOURCE FROM <name> HOST 'example.com'` would
    /// correspond to `Sql` having a field `host` of type `String`.
    ///
    /// Types that implement `Deserialize` automatically implement `Deserialize`
    type Sql: for<'de> Deserialize<'de>;

    /// Type used to construct instances of this source
    type Config: InstanceConfig;

    /// The name of the source
    fn name(&self) -> &'static str;

    /// Constructs a new source from the parsed SQL. This is an opportunity for the implementation
    /// to do any sanity check of the parameters passed
    fn from_sql(sql: Self::Sql) -> Result<Self, Error>;

    /// An opportunity for the source to remove any impurities from its
    /// definition. This will be called before permanently storing the source in
    /// the catalog. The default implementation is a no-op
    async fn purify(&mut self) { }

    /// Constructs a new source from the parsed SQL and WITH option. This is an
    /// opportunity for the implementation to do any sanity check of the
    /// parameters passed
    fn new(sql_options: Self:Options, with_options: Self::WithOptions) -> Result<Self, Error>;

    /// Creates at most `workers` instance configs. These configs will be
    /// persisted in the catalog and be used to do any associated cleanup later.
    /// It is up to the system to decide how the returned configs will be
    /// distributed among the dataflow workers and converted into instances
    fn create_instances(&self, workers: usize) -> Vec<Self::Config>;

    /// The schema that will be produced by this source. The `RelationDesc`
    /// returned conveys information of the name, type, and number of columns
    /// but also of the key, if any exists.
    fn desc(&self) -> RelationDesc;

    /// If true, the source will only be available behind the experimental flag
    fn is_experimental(&self) -> bool {
        false
    }

    /// If false, the source will not be available in the cloud product
    fn is_safe(&self) -> bool {
        true
    }
}

// `TimelyEvent` refers to `timely::dataflow::operators::to_stream::Event`
pub type Event = TimelyEvent<Option<Timestamp>, Result<(Row, Timestamp, Diff), Error>>;

#[async_trait]
pub trait InstanceConfig: Serialize + for<'de> Deserialize<'de> {
    /// Shared state between instances can be stored in this type for sources
    /// that can share resources (e.g TCP sockets). A mutable reference to an
    /// instance of `State` is passed to `Self::into_instance()`
    type State: Default;

    /// The instance type that will produce data into the dataflow from a ///
    /// particular worker
    type Instance: futures::Stream<Item = Event> + Send;

    /// Transforms this configuration into an instance, re-using some of the
    /// provided state if necessary
    fn into_instance(self, state: &mut Self::State) -> Self::Instance;

    /// Computes the timestamp beyond which this instance and `other` will
    /// produce identical updates. Data not beyond this timestamp might have
    /// been subjected to logical compaction.
    /// If this function returns None, the two instances will never converge and
    /// the user should be warned
    fn meet(&self, _other: &Self) -> Option<Antichain<Timestamp>> {
        None
    }

    /// Cleanup logic to run when an instance config is dropped. This is an
    /// opportunity to contact external systems for example. The default
    /// implementation a no-op
    async fn cleanup(&mut self) -> Result<(), Error> {
        Ok(())
    }

```

## Syntax parsing

The trait definitions above mention `FromSql` and `ToSql` which are analogous to
serde's `Deserialize` and `Serialize` traits. Serde's traits can definitely be
used directly for some sources but it's unclear if they can be used for all of
them, hence the separate names.

Whatever the case, the intention is to base all **future** sources on serde's
`Deserialize` and `Serialize` traits and only fall back to manual parsing to
maintain backward compatibility of existing syntax.

In order to use serde's `Deserialize` trait we first need to define a mapping
between [serde's data model](https://serde.rs/data-model.html) onto SQL and then
provide a `Deserializer` implementation of our parser. An initial implementation
of `Deserializer` can be [seen
here](https://github.com/petrosagg/materialize/blob/source-trait-impl/src/sql-parser/src/parser/de.rs).

### Structs

When deserializing a struct the fields are expected to appear in SQL in the
order of definition, followed by their value. The name of the struct does not
appear in SQL (similar to what you'd expect in a JSON serialization). Here is a
simple example:

```rust
// corresponds to `CLIENT 'foo' KEY 'bar'`. `CONNECTION` is not part of SQL
struct Connection {
    client: String
    key: String
}
```

The SQL cannot encode Optional values, only absent values, and so whenever a
field needs to be optional it must have a `#[serde(default)]` annotation:

```rust
enum Compression {
    None,
    Gzip
}

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}

// corresponds to `FILE 'foo'` but also `FILE 'foo' COMPRESSION GZIP`
struct File {
    path: String,
    #[serde(default)]
    compression: Compression,
}
```

### Enums

Enums encode alterative parsing paths. THe SQL string must first name the
variant and then follow the appropriate values for that variant. Similarly to
the structs, the name of the enum does not appear in the SQL string. All enum
declaration must be annotated with `#[serde(rename="lowercase")]` to be
compatible with the parser.

```rust
// corresponds to either `HUMAN '2021-01-01'` or `EPOCH 1626181764`
#[serde(rename="lowercase")]
enum Timestamp {
    Human(String),
    Epoch(u64)
}
```

### More complex examples

Enums and structs can be combined to encode more complex syntax expressions:

```rust
// This source can be parsed from all of the following variations:
// FROM FILE PATH '/foo/bar'
// FROM FILE PATH '/foo/bar' COMPRESSION NONE
// FROM NETWORK HOST '1.2.3.4' RETRIES 10
struct Source {
    from: SourceType,
}

#[serde(rename="lowercase")]
enum SourceType {
    File(FileSource),
    Network(NetworkSource)
}

struct FileSource {
    path: String,
    #[serde(default)]
    compression: Compression,
}

struct NetworkSource {
    host: String,
    retries: u64
}
```

## Implementation

An implementation of all of the above together with a couple example sources can
be found in this very branch. The interface and implementation of sources form a
new crate, named
[`sources`](https://github.com/petrosagg/materialize/tree/source-trait-impl/src/sources).
Under this framework, new sources can be added to the system with minimum
effort. A source named `TickTock` which just gives out integers at a user chosen
interval can be implemented in [70 lines of
code](https://github.com/petrosagg/materialize/blob/source-trait-impl/src/sources/src/sources/ticktock.rs)
and then registered in the [list of
sources](https://github.com/petrosagg/materialize/blob/source-trait-impl/src/sources/src/lib.rs#L20-L23).
No change anywhere else in the code is required.

Use the following query to try it out:

```sql
CREATE MATERIALIZED SOURCE "foo" FROM TICKTOCK MILLIS 1000;
```

Keep in mind that while the implementation of the `sources` crate is very close
to its end state, its integration with the rest of the system is held with duct
tape and is meant only as an illustration of what it looks like to add a working
source to the system.
