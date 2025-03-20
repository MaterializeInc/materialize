# Durable Catalog State

## Context

The [Platform V2](https://www.notion.so/materialize/WG-Platform-v2-90cffaf33d204df3af7b6f01102f2747)
project is aiming to increase use-case isolation. The exact definition of use-case isolation in the
context of Platform V2 is still being refined, but it can roughly be thought of as isolating the
actions of one user from negatively impacting the actions of another user. To put it another way, to
what extent can a user pretend that they are the only user of Materialize without noticing
performance impacts from other users. Part of the use case isolation work is to make a scalable
serving layer. The scalable serving layer needs access to the catalog state and may even want to be
able to react to changes in the catalog state.

This design doc describes an interface for getting data out of a durable catalog as well as how to
implement this interface with both the stash and persist. Persist is a desirable implementation
because it exposes a Listen API which lays the groundwork for responding to catalog changes.

## Goals

- Design interface for durable catalog state.
- Lay the groundwork for splitting out the catalog state from the Coordinator and serving layer.
- Describe implementations for this interface with the stash and persist.
- Lay the groundwork for being able to react to catalog changes.

## Non-Goals

- Split out the catalog state from the Coordinator and serving layer.
- Design a fully differential catalog state that the adapter can react to.
- Remove the timestamp oracle from the catalog state.

## Overview

The durable catalog interface can currently be found in
the [`mz_adapter::catalog::storage`](https://github.com/MaterializeInc/materialize/blob/main/src/adapter/src/catalog/storage.rs)
module. At a high level it stores the following types of objects:

- Audit logs
- Clusters
- Cluster replicas
- Cluster introspection source indexes
- Config: This is a misc catch all that stores the deployment generation and the stash
  version. [This issue](https://github.com/MaterializeInc/database-issues/issues/6298) describes what
  it is and how it should be cleaned up.
- Databases
- Default privileges
- ID allocators
- Items
- Roles
- Schemas
- Settings: This currently only stores the "catalog_content_version" which is subtly different from
  the version stored in config. They are both used for migrations, the config version is used
  for stash migrations while the "catalog_content_version" is used for catalog migrations.
- Storage usage metrics
- System configurations: This stores system variables (like `max_tables`).
- System object fingerprints
- System privileges
- Timestamps (for timestamp oracles)

We will codify the access and mutation of these objects behind a trait that we will create the
following implementations for:

- A stash backed implementation.
- A persist backed implementation.
- A shadow implementation that uses both the stash and persist implementation for every method and
  compares results. This will be useful for testing.

A rough prototype of this design can be found
here (it does not include a persist
implementation): https://github.com/MaterializeInc/materialize/pull/21071

## Detailed description

### Durable Catalog Trait

We will add the following object safe trait in the catalog to describe interacting with the durable
catalog state.

NB: `async` modifiers and trait bounds (such as `Debug`) are left off for easier readability.

```Rust
pub trait DurableCatalogState {
    // Initialization

    /// Reports if the catalog state has been initialized.
    fn is_initialized(&self) -> Result<bool, Error>;

    /// Optionally initialize the catalog if it has not
    /// been initialized and perform any migrations needed.
    fn open(&mut self) -> Result<(), Error>;

    /// Checks to see if opening the catalog would be
    /// successful, without making any durable changes.
    ///
    /// Will return an error in the following scenarios:
    ///   - Catalog not initialized.
    ///   - Catalog migrations fail.
    fn check_open(&self) -> Result<(), Error>;

    /// Opens the catalog in read only mode. All mutating methods
    /// will return an error.
    ///
    /// If the catalog is uninitialized or requires a migrations, then
    /// it will fail to open in read only mode.
    fn open_read_only(&mut self) -> Result<(), Error>;

    /// Returns the epoch of the current durable catalog state. The epoch acts as
    /// a fencing token to prevent split brain issues across two
    /// [`DurableCatalogState`]s. When a new [`DurableCatalogState`] opens the
    /// catalog, it will increment the epoch by one (or initialize it to some
    /// value if there's no existing epoch) and store the value in memory. It's
    /// guaranteed that no two [`DurableCatalogState`]s will return the same value
    /// for their epoch.
    ///
    /// None is returned if the catalog hasn't been opened yet.
    ///
    /// NB: We may remove this in later iterations of Pv2.
    fn epoch(&mut self) -> Option<NonZeroI64>;

    // Read

    /*
     * Each object type will an accessor method of the form
     * get_<object-type>s(&self) -> Vec<ObjectType>;
     */
    /// Get all clusters.
    fn get_clusters(&mut self) -> Result<Vec<Cluster>, Error>;

    /*
     * Currently, there isn't much need for methods of the form
     * get_<object-type>_by_id(&self, id: I) -> ObjectType;
     * As we separate the catalog state out from the rest of the
     * Coordinator, we will most likely need to add these methods
     * which should be fairly straight forward.
     */

    // ...

    // Write

    /// Creates a new durable catalog state transaction.
    fn transaction(&mut self) -> Transaction;

    /// Commits a durable catalog state transaction.
    fn commit_transaction(&mut self, tx: Transaction) -> Result<(), Error>;

    /*
     * The majority of writes will go through a Transaction.
     * However as an optimization it may be useful to have more
     * targeted methods. We can add those on a case-by-case basis.
     */

    /// Persist mapping from system objects to global IDs and fingerprints.
    fn set_system_object_mapping(
        &mut self,
        mappings: Vec<SystemObjectMapping>,
    ) -> Result<(), Error>;

    // ...

    // Misc

    /// Confirms that this catalog is connected as the current leader.
    ///
    /// NB: We may remove this in later iterations of Pv2.
    fn confirm_leadership(&mut self) -> Result<(), Error>;

    /// Dumps the entire catalog contents in human readable JSON.
    fn dump(&self) -> Result<String, Error>;
}
```

We will also add the following methods to initialize the durable catalog state structs. They do
not open the catalog, but they may create connections over the network.

```Rust
fn init_stash_catalog_state(
    now: NowFn,
    stash_factory: StashFactory,
    stash_url: String,
    tls: MakeTlsConnector,
) -> Box<dyn DurableCatalogState> {
    // ...
}

fn init_persist_catalog_state(
    now: NowFn,
    shard_id: ShardId,
    persist_client: PersistClient,
) -> Box<dyn DurableCatalogState> {
    // ...
}

fn init_shadow_catalog_state(
    primary_state: Box<dyn DurableCatalogState>,
    secondary_state: Box<dyn DurableCatalogState>,
) -> Box<dyn DurableCatalogState> {
    // ...
}
```

These methods are needed to solve a bootstrapping problem. Some catalog state, such as deploy
generation, is needed before attempting to open the catalog. So these methods will return unopened
catalog states capable of returning only the state we need for bootstrapping.

All current usages of `mz_adapter::catalog::storage::Connection` will be updated to
use `Box<dyn DurableCatalogState>`. The `environmentd` binary will initialize
the `Box<dyn DurableCatalogState>` using the methods above depending on command line arguments.

### Stash implementation

A stash based implementation will be fairly trivial to implement. This interface is already
implemented using the stash, we will just need to change the name of some methods.

### Persist Implementation

This implementation is based
on [Aljoscha's skunk works project](https://github.com/aljoscha/materialize/tree/skunk-log-for-coord-state).
The entire state will be stored in a single persist shard as raw bytes with a tag to differentiate
between object types. The existing protobuf infrastructure will be used to serialize and deserialize
objects. The implementing struct will maintain a persist write handle, an upper, and
an in-memory cache of all the objects. Certain append only object types, like audit
logs and storage usage, will not be cached in memory since they are only written to and not read.
NB: The storage events are read once at start-time and never read again.

#### Initialization

We will use an environments org ID to deterministically generate the persist shard ID. The org ID
and persist shards are both v4 UUIDs, so we could technically use the org ID as the persist shard
ID. However, we'll likely want to modify the ord ID to something like `hash(ord_id) + "catalog"` for
nicer looking observability in dashboards and such.

- `init_persist_catalog_state`:
    1. Creates a new persist write handle.
    2. Read the shard upper into memory.
    3. Store the boot timestamp in memory.
- `is_initialized`: Checks if upper is equal to the minimum timestamp.
- `open`:
    - If the catalog is not initialized:
        1. Use the write handle to compare and append a batch of initial catalog objects.
        2. Update the in memory upper to the new upper.
    - If the catalog is initialized:
        1. Open a persist read handle.
        2. Read a snapshot of the catalog state into memory.
        3. Run migrations in memory (including incrementing the epoch).
        4. Use the write handle to compare and append a batch of migration changes into persist.
        5. Update the in memory upper to the new upper.
- `check_open`:
    - If the catalog is not initialized then as long as we're the leader return `Ok`.
    - If the catalog is initialized:
        1. Open a persist read handle.
        2. Read a snapshot of the catalog state into memory.
        3. Run migrations in memory.
            - None of the objects will be cached in memory.
- `open_read_only`:
    - If the catalog is not initialized then fail.
    - If the catalog is initialized:
        1. Open a persist read handle.
        2. Read a snapshot of the catalog state into memory.
- `boot_ts`: Return boot timestamp from memory.
- `epoch`: This will be stored the same as any other catalog object so this can be treated as a
  normal [read](#reads).

#### Reads

All reads (except for the deploy generation) will return an object from the in memory cache.

Reading the deploy generation will perform the following steps:

- If the catalog is open, then read perform normal read.
- If the catalog is not open:
    - If the catalog is not initialized then return `None`.
    - If the catalog is initialized:
        1. Open a persist read handle.
        2. Read the deploy generation from persist.

Reading the deployment generation needs to work before the catalog has been opened and before
migrations have been run.

#### Writes

- `transaction`: Use objects from in memory cache to initialize a new `Transaction`.
- `commit_transaction`:
    1. Use the write handle to compare and append the batch of changes into persist.
    2. Update the in memory upper to the new upper.
    3. Update in-memory object state.
- `set_X`:
    1. For each object create the following batch of changes:
        - If the key of the object already exists in memory, add a retraction to the batch.
        - Add an addition to the batch of the new value.
    2. Use the write handle to compare and append the batch of changes into persist.
    3. Update the in memory upper to the new upper.
    4. Update in-memory object state.

#### Misc

- `confirm_leadership`: Check that the persist shard upper is equal to the upper cached in memory.
  This will use a linearized version of the `fetch_recent_upper` method in persist which requires
  fetching the latest state from consensus and is therefore a potentially expensive operation.
- `dump`: Convert the in-memory state to a JSON string.

### Catalog Transactions

The current
[catalog transactions](https://github.com/MaterializeInc/materialize/blob/b5aa928960aa68c031caa8ff3bfab0ff53220361/src/adapter/src/catalog/storage.rs#L892-L916)
will remain mostly the same. It works by reading the entire durable catalog state into memory,
making changes to the state in memory, then committing all changes at once to the backing durable
storage. The few changes needed are:

- It will be the responsibility of the `DurableCatalogState` to commit the `Transaction` instead of
  the `Transaction` committing itself.
- `Transaction` will not hold onto a mutable reference to a `Stash`. Though it will need a mutable
  reference to some part of `DurableCatalogState` to prevent multiple concurrent transactions.
- To avoid cloning the entire catalog state in the persist implementation, we'll
  update `TableTransaction`s to use `CoW`s.

### Catalog Debug Tool

The catalog debug tool is modeled after the stash debug tool and allows users to inspect and modify
the durable catalog state. It will provide the following functionality (descriptions are mostly
taken from the existing stash debug tool):

- Dump: Dumps the catalog contents in a human-readable format. Includes JSON for each key and value
  that can be hand edited and then passed to the `edit` or `delete` commands.
    - [optional arg] `target`: Write output to specified path. Default to stdout.
- Edit: Edits a single catalog object.
    - [arg] `object-type`: name of the object type to edit.
    - [arg] `key`: JSON encoded key of object.
    - [arg] `value`: JSON encoded value of object.
- Delete: Deletes a single catalog object.
    - [arg] `object-type`: name of the object type to edit.
    - [arg] `key`: JSON encoded key of object.
- Upgrade Check: Checks if the specified catalog could be upgraded from its state to the catalog at
  the version of this binary. Prints a success message or error message. Exits with 0 if the upgrade
  would succeed, otherwise non-zero. Can be used on a running `environmentd`. Operates without
  interfering with it or committing any data to that catalog.
    - [optional arg] `cluster-replica-sizes`: Valid cluster replica sizes.

The tool will have two modes:

- Stash: Connects to a stash.
- Persist: Connects to persist.

The tool will initialize a `Box<dyn DurableCatalogState>` and use the trait methods to implement
each functionality. `dyn DurableCatalogState` is safe because the trait is designed to be object
safe.

- Dump: Will use the `dump` method.
- Edit and Delete: Will be implemented using a catalog transaction.
- Upgrade Check: Will use the `check_open` method.

NB: There is an open issue that you need two different versions of the stash debug tool to perform
upgrade checks: https://github.com/MaterializeInc/database-issues/issues/6355. This proposal will not
fix that issue and suffers from the exact same issue.

### Migrations

This section is currently hand-wavy and needs the most additional design work.

Stash migration ownership is currently tangled between the adapter crate and the stash crate,
see [stash: Untangle the stash config collection](https://github.com/MaterializeInc/database-issues/issues/6298).
Ideally the adapter would be in charge of how and why to migrate the catalog contents while the
stash provides the primitives needed to migrate a stash collection.

Once the migrations are de-tangled, at a high level they will take the following steps:

1. Use `impl From<objects_v(X - 1)::(Object)> for objects_v(X)::(Object)` to convert every object
   from their old format into their new format.
2. Use `mz_adapter::catalog::storage::Transaction` to perform any deletions, insertions, or updates
   that require read/write semantics instead of `mz_stash::transaction::Transaction`.

As a bonus, these migrations start to look very similar to the
existing [catalog migrations](https://github.com/MaterializeInc/materialize/blob/main/src/adapter/src/catalog/migrate.rs).
We might be able to combine them into a single logical step instead of having two separate migration
frameworks (not counting the builtin migration framework).

## Alternatives

- Use multiple shards for the persist implementation
  once [multi-shard txns](https://github.com/MaterializeInc/materialize/pull/20954) is merged and
  stable.
- An alternative approach is to put a trait directly in front of the stash, and implement that trait
  using the current CRDB implementation and implement the trait using persist.
    - Pros:
        - Re-using the existing stash migration might be easier.
        - We can re-use the existing stash debug tool.
        - The controller can use persist for durable state for free.
    - Cons:
        - Maintaining a generic durable TVC store adds a lot of complexity to the implementation.
          Much of that complexity is already handled by persist, which is a generic durable TVC
          store with a dedicated team. Overall, I think it might be a net-win if we can at some
          point delete the stash crate because it removes a layer of complexity from the adapter.
        - It's very difficult to get the stash trait right and maintain it. We previously had a
          stash trait but removed it because it was too hard to maintain.
- The persist implementation could not use an in-memory cache, and instead go to persist for every
  read via a read handle. This has better memory usage, but slower reads. The reason that I decided
  on the current proposed implementation is that the Coordinator already reads the entire durable
  catalog state into memory for every catalog transaction. So we already need that amount of memory
  free at any time in order to be able to serve DDL.

## Future Work

- Move storage usage out of the catalog state.
- Update the timestamp oracle to store a catalog timestamp used to make changes to the catalog. We
  can then fetch the epoch AS OF catalog_ts to check if it changed instead of needing a
  linearized `fetch_recent_upper` in persist.
- Differential catalog state; update the catalog state to refresh itself whenever it's changed
  concurrently instead of halting.

## Open questions

- Should the `DurableCatalogTrait` be broken up into two traits? A read only trait and a read/write
  trait?
- For the persist implementation, can we use a schema like `tag: u64, raw: bytes` and take advantage
  of persist filtering based on the tag?
- Is checking that the persist shard upper sufficient for a leadership check or do we need to check
  the epoch?
- Should this work live in its own crate or stay part of the adapter crate? My opinion is that it
  should be in its own crate.
