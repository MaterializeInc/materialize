- Feature name: Improved Stash Migrations
- Associated: [#17466](https://github.com/MaterializeInc/materialize/issues/17466)

# Summary
[summary]: #summary

There are instances where we want to migrate the data stored in the Stash. For example, as we
build Role Based Access Control, we want to add new fields to the [`RoleValue`](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/adapter/src/catalog/storage.rs#L3032-L3037)
type. While today we have a [migration flow](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/adapter/src/catalog/storage.rs#L78)
there are a few issues with it, namely:

1. The [types](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/adapter/src/catalog/storage.rs#L3074-L3104)
we use for the Stash need to support _both_ the current Stash and the new data we're trying trying
to migrate to, which makes the types fairly cumbersome and hard to get right.
2. Individual migrations are a [list of closures](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/adapter/src/catalog/storage.rs#L85-L936)
that we index into, which makes the selection of what migrations to run, pretty fragile.
3. Stashes are initialized with a version of 0, and then run through all existing migrations. This
requires us to modify previous migrations whenever we change the Stash types, which is sketchy.

We can fix problem 1 by maintaining some record of the previous types in the Stash (e.g.
snapshotting), problem 2 by structuring our list of migrations in a more defined way, and problem
3 by creating a specific "initialize" step for the Stash.


# Motivation
[motivation]: #motivation

Our overall goal is to create **"fearless Stash migrations"**. In other words, we want to make it
so we can assign an issue that requires a Stash migration to a new-hire, and have confidence that
if our builds and tests are passing, then the migration won't break anything in production.

There have been several issues caused by Stash migrations:

* [materialize#17824](https://github.com/MaterializeInc/materialize/issues/17824)
  * General difficulty in writing Stash migrations, see [materialize#18719](https://github.com/MaterializeInc/materialize/pull/18719) or [materialize#17727](https://github.com/MaterializeInc/materialize/pull/17727) for examples.
* [materialize#18578](https://github.com/MaterializeInc/materialize/pull/18578)
  * Fixed previous migrations that failed to remove old types from the Stash.
* `_incident-47`
  * Scaling issue, the SQL statement we were running on CockroachDB was >32MiB which was the allowed size.


# Explanation
[explanation]: #explanation

### What is the "Stash"?

The [Stash](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/stash/src/lib.rs#L107-L124)
is a time-varying key-value store, i.e. it stores colletions of `(key, value, timestamp, diff)`,
which we use to persist metadata for a Materialize deployment. For example, we use the Stash to
persist metadata for all `MATERIALIZED VIEWS` that a user has created. This way when restarting an
environment, we know what Materialized Views to recreate on Compute. Concretely we use CockroachDB
and all keys and values are serialized as JSON for human readability.

### Part 1: Snapshotting Types

What makes Stash migrations hard to write and hard to reason about, is that you need to define a
type that supports deserializing the current Stash and serializing the new Stash. For example, if
you want to add a field to a struct you need to wrap the new field in an `Option<...>`. This way
your struct can deserialize when the field doesn't exist, yet provide the Stash with the new data.
There currently is not a way to do a migration like, converting a type from a `u64` to an `enum`.

The proposal to fix this is to "snapshot" our types, this would allow us to, in a type safe way,
represent the current data we're reading from, and the new types we want to migrate to. Then
we'd be able to do arbitrarily complex migrations from one type to another.

Concretely what this means is having types like:

```
struct RoleValuesV5 {
    create_db: bool,
}

struct RoleValuesV6 {
    // A bit flag of permissions this user is allowed.
    perms: u64,
}
```

### Part 2: Structuring Migrations

Today migrations are a [list of closures](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/adapter/src/catalog/storage.rs#L78), and we index
into this list based off of the current version we read from our database. This is fragile
because the list always need to be ever increasing in length, we can't move the position of a
migration in the list, and there is nothing to indicate that a new migration needs to be added.

The proposal is to maintain a notion of a "current stash version" and restructure the migrations
based on this version, for example:

```
const STASH_VERSION: u64 = 3;

async fn migrate(...) -> Result<...> {
    let version = stash.read_version().await?;

    const NEXT_VERSION = STASH_VERSION + 1;
    match version {
        0 => // migrate
        1 => // migrate
        2 => // migrate
        CURRENT_VERSION => return Ok(()),
        NEXT_VERSION.. => panic!("Traveled to the future!"),
    }
    stash.set_version(version + 1).await;
}
```

This way if someone bumps `STASH_VERSION` we'll fail to compile because there will be a case in
the `match` statement which is unhandled, and ideally with the `match` statement it becomes easier
to understand which migrations are running and when. To keep logic simple, we'd also assert an
invariant that we only ever upgrade one version at a time, e.g. from `N` to `N + 1`, we would not
support arbitrary upgrades from `N` to `N + x`.

Also with the match statement we'd be able to add guards on the current build version, e.g.:
```
match version {
    // ... PSEUDO CODE
    3 if BUILD_NUMBER < "0.50" => // migrate
    3 => (),
    // ...
}
```
which is useful if we ever need to ensure that a migration runs for only a specific build.

### Part 3: Initializing a Stash

Today when creating a Stash for the first time, we [initialize it with a version of 0](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/adapter/src/catalog/storage.rs#L1140-L1141),
and run it through all of the existing migrations, the first of which [actually inserts](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/adapter/src/catalog/storage.rs#L90-L418)
the necessary initial data. This is problematic for three reasons:

1. When adding a new type we have to modify this first migration step, which is kind of sketchy.
2. We can never deprecate old Stash versions, since we're forced to always support version 0.
3. As time goes on and we modify the Stash in more ways, the first startup time of `environmentd`
will continuously increase, since we need to run through _all_ of the different migrations.

The proposal is to create a specific "initialization" step for the Stash, that uses the "current"
Stash types, and initializes it to the `STASH_VERSION` that is defined in part 2.

# Reference explanation
[reference-explanation]: #reference-explanation

### Part 1: Snapshotting Types

To snapshot the types we store in the Stash, we should define them in, and store serialized, protobufs.

> Note: the Stash used to be serialized protobufs but we changed the format to JSON for human
read-ability. I believe now with the `stash-debug` tool, there is less of a reason to require
the serialized format to be human read-able. See [materialize#14298](https://github.com/MaterializeInc/materialize/pull/14298)
for the PR that changed it to JSON.

We would define the types we store in the Stash in `.proto` files, and we'd introduce the following
file layout in the `stash` crate:

```
/stash
    /proto
        objects.proto
        /old
            objects_v1.proto
            objects_v2.proto
            ...
    /src
        ...
```

`stash/proto/objects.proto` would contain all of our "current" types, and the "old" types would
exist inside of the `stash/proto/old` folder.

To generate Rust types from our `.proto` schemas, we'd have two steps:

1. Use [`prost_build`](https://docs.rs/prost-build/latest/prost_build/) to do the generation, and
as part of the build process we'd specifically omit any ["includes"](https://docs.rs/prost-build/latest/prost_build/fn.compile_protos.html)
which would force our `.proto` files to have no dependencies.
2. As part of the build process (i.e. in a `build.rs`) we'd also maintain a hash for the current
version of the protos (i.e. `stash/proto/objects.proto`) and a hash for each of the snapshotted
versions. This would allow us to assert at build time that none of our protos have changed. We can
also assert that we have the correct nubmer of snapshots in `stash/proto/old` based on the
`STASH_VERSION` number introduced in the solution to problem 2.

Protobufs also have a [well defined serialization formation](https://protobuf.dev/programming-guides/encoding/)
so there is no risk of the serialization changing, as long as the `.proto` files themselves don't
change.

This setup has several benefits:

1. Snapshotting our previous types is very easy. Because we don't specify any "includes" path when
generating Rust types, the `.proto` files cannot have any dependencies. To snapshot the "current"
version, all you have to do is copy `objects.proto` and paste it into `/old/objects_vX.proto`.
2. Detecting at built time if a change has occurred to any of our `.proto` definitions. This goes a
long way to making the migrations "fearless". If someone changes the current proto definitions, the
old versions, or bumps the `STASH_VERSION` without snapshotting, we can emit helpful build errors.
3. It uses "off the shelf" crates, the only code we need to write and thus maintain would be a
fairly small `build.rs` script.
4. **Nice to have**, protobuf has a more compact serialization format than JSON, and is more
performant. We also already use protobuf in our codebase, so we're not introducing any new
concepts.


### Part 2: Structuring Migrations

Along with maintaining `const STASH_VERSION` and using a `match` statement as described above we'd
setup a pattern of defining migrations in their own modules. Specifically we'd introduce the
following layout in our repo:

```
/stash
    /src
        /migrations
            v0_to_v1.rs
            v1_to_v2.rs
            v2_to_v3.rs
            ...
        ...
    /proto
        ...
```

and our migration code would then look something like this:

```
const STASH_VERSION: u64 = 3;

async fn migrate(...) -> Result<...> {
    let version = stash.read_version().await?;

    const NEXT_VERSION = STASH_VERSION + 1;
    match version {
        0 => migrations::v0_to_v1::migrate(...).await?;
        1 => migrations::v1_to_v2::migrate(...).await?;
        2 => migrations::v2_to_v3::migrate(...).await?;
        3 => // migrate
        NEXT_VERSION.. => panic!("Traveled to the future!"),
    }
}
```

There are a few benefits to putting each migration function in their own modules:

1. The only place we would import the generated protobuf code from our snapshots would be in these
modules. So the only place that would import the `object_v1.proto` would be the `v0_to_v1` and
`v1_to_v2` modules. This helps prevent us from depending on the snapshotted versions anywhere
besides the necessary migration paths.
2. We promote a pattern of documenting the migration and why it exists in a module doc comment.
While it's possible to have doc comments on functions, I believe it would be more obvious/easier to
review module level doc comments.

Each migration function would be provided a `stash::Transaction`, which would allow us to open
[`StashCollection`](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/stash/src/transaction.rs#L136)s
which should provide enough flexibility to facilitate any migration.

> Note: `StashCollection`s do not enforce uniqueness like [`TableTransaction`](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/stash/src/lib.rs#L775-L793)s
do, some more thought is needed here as to whether or not uniqueness checks are required for
migrations. They probably are?


### Part 3: Initializing a Stash

We add a new method to the Stash that is `async fn initialize(...) -> Result<...>`, that will
assert the Stash is empty, and then populate it with our inital values. We can already have logic
to detect when a Stash [has not been initialized](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/adapter/src/catalog/storage.rs#L1138-L1141).
Instead of returning a version of `0`, we would now call this new `initialize(...)` method. The
method itself would largely be very similar, if not the same, to our [first migration step](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/adapter/src/catalog/storage.rs#L90-L418),
a new behavior though is it would initialize `"user_version"` to the `STASH_VERSION` defined in
part 2.

Creating this separate initialization step, allows us to deprecate old migrations, e.g. the
`migrate(...)` function would be able to look something like:

```
const MIN_STASH_VERSION = 3;

async fn migrate(...) -> Result<...> {
    let version = stash.read_version().await?;

    const NEXT_VERSION = STASH_VERSION + 1;
    match version {
        ..MIN_STASH_VERSION => panic!("Deprecated stash version!"),
        3 => migrations::v3_to_v4::migrate(...).await?;
        4 => migrations::v4_to_v5::migrate(...).await?;
        NEXT_VERSION.. => panic!("Traveled to the future!"),
    }
}
```

It also allows us to enforce the invariant of "once a Stash migration is written it should never
be modified", and allows Stash initialization to be a single step instead of `STASH_VERSION` number
of steps.

# Rollout
[rollout]: #rollout

### Migration to this new framework

We need to write a migration from our JSON types today, to this new migration framework that uses
protobufs. I propose we do that in the following steps:

1. Define all of our existing Stash objects in `objects.proto`, also snapshot this initial version
as `objects_v15.proto`. The current Stash version is 14.
2. Move all of the existing Stash objects from [`storage.rs`](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/adapter/src/catalog/storage.rs)
into a new `legacy_json_objects.rs`, these will only continue to exist to facilitate the migration
to the new protobufs, leave an extensive doc comment explaining as much.
3. Introduce the migration flow as described in part 2, re-write the [existing migrations](https://github.com/MaterializeInc/materialize/blob/a8aa3be77ddf152faf4e98596d2d3fff94b1f9b6/src/adapter/src/catalog/storage.rs#L437-L739)
in this flow, using the types from `legacy_json_objects.rs`.
4. Bump the `STASH_VERSION` to 15, write a `v14_to_v15(...)` migration that migrates us from the
types in `legacy_json_objects.rs` to the protos we snapshotted in `objects_v15.proto`. Introduce
the "initialization" step as described in part 3, so new users will immediately have
`STASH_VERSION: 15` which will contain the protobufs.


> Note: There are two alternative approachs I thought of, but don't think are great:
> 1. Before running any of the existing Stash migrations, switch everything to protobufs and
     re-write the existing migrations using protos. I don't like this approach because we'd then
     have two version fields for the Stash, i.e. "version number" and "is proto". And we'd need
     to generate multiple `object_v11.proto`, `object_v12.proto`, etc. files that would be
     identical.
> 2. Write a single `legacy_to_v15(...)` migration code path, that handles upgrading from all of
     the existing versions of the Stash to v15. This wouldn't be too bad, but it does break the
     invariant we wanted to uphold of only every upgrading one version at a time. With this
     approach we could theoretically upgrade from say v11 to v15.

### Other

At our current scale, I don't believe the benefit to partially rolling out a new format for the
Stash outweighs the complexity of concurrently maintaining two separate implementations. To
get similar testing coverage that a partial rollout would, we can validate that the new Stash
format and migration work, by running the `stash-debug`'s `upgrade-check` command against select
customer environments.


## Testing and observability
[testing-and-observability]: #testing-and-observability

This change will be tested manually, via Rust tests, and our existing [testdrive upgrade tests](https://github.com/MaterializeInc/materialize/blob/main/test/upgrade/mzcompose.py).
I don't believe there are any sqllogictest tests that would be able to exercise this change in a
unique way.

We can test this change in the following ways:

1. Manually in a development environment. Start `environmentd` before this change, then restart
`environmentd` after this change has been applied, assert we're able to migrate successfully.
2. As mentioned above, use the `stash-debug` tool's `upgrade-check` command to make sure customer
environments would be able to successfully upgrade.
3. Track the number of rows, and the size of rows, that exist in the Stash [materialize#18871](https://github.com/MaterializeInc/materialize/issues/18871).
If we don't see any change in these numbers, and we don't see any errors otherwise (e.g. in Sentry),
that's a strong indication that the migration is working as expected.
4. Add more cases to the existing [upgrade tests](https://github.com/MaterializeInc/materialize/blob/main/test/upgrade/mzcompose.py)
to cover any features we might be missing.
5. (Future) Re-introduce the builtin-migration tests [materialize#17236](https://github.com/MaterializeInc/materialize/issues/17236)
that start Materialize at a previous version of the repo, and then restarts it with the current
commit.
6. (Future) In a Rust test, use the snapshotted Stash types to generate "old" version of the Stash
and assert we can upgrade all the way to the current version. Bonus points if the "old" versions
are generated relative to the user metrics we get from suggestion 3.
7. (Future) If we ever build a ["Poor man's Snowtrail"](https://www.notion.so/materialize/Poor-man-s-Snowtrail-1c36cdc61a224948b623318a02db3bb8?pvs=4)
we can automatically test the migration code path against old snapshots of users' Stashes.

I believe testing methods 1 - 3 should block the rollout/merging of this change, while 4 - 6 are
changes that can be done in the future, or fast-followed if deemed high enough of a priority.


## Lifecycle
[lifecycle]: #lifecycle

As mentioned above, I don't believe we should do a partial rollout of this change. Therefore we
wouldn't have any feature flags, or any lifecycle for this change.


# Drawbacks
[drawbacks]: #drawbacks

* Snapshotting the types in the Stash does increase the total amount of work required to change
something we store, with the benefit being we get automated checks that the changes we're making
are correct.
* Persisting serialized protobufs makes it such that the data in the Stash is no longer human
readable. Ideally a tool like `stash-debug` removes this requirement because the tool itself
can deserialize the protobufs. Also, possibly a benefit to requiring the use of `stash-debug` is
it discourages the use of tools like `psql` to connect directly to production data, which are
scary because you could accidentally modify user data, unlike `stash-debug` which protects against
this.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

### Other approaches considered:

* Writing a `#[stash_object]` procedural macro that snapshots our Rust types for us.
    * I wrote a PoC procedural macro that we could use on our Stash objects that would
    automatically snapshot them. You could annotate fields in a struct with `#[stash(introduced = 5)]`
    and that would generate all versions of the struct >= 5 with that field, [branch](https://github.com/parker-timmerman/materialize/commit/243d76fb123625121949155ee5168b50f96cb1e4).
    * ❌ The reason I don't like this approach is it essentially requires us to maintain a bespoke
    code generator, built with a seldomly used feature of Rust, procedural macros.
* Using schema-less `serde_json::Value` in the migration code path, to avoid snapshotting
altogether.
    * Instead of snapshotting our types, we could build a migration path that uses "untyped"
    `serde_json::Value` structs, where we'd check and modify the fields at runtime.
    * ❌ We define a [`trait Data`](https://github.com/MaterializeInc/materialize/blob/23d7cad1211b4d1d1de3a26652937f9f2c4df61c/src/stash/src/lib.rs#L101-L103)
    that all of our keys and values must implement. A requirement of `Data` is that the type must
    implement [`Ord`](https://doc.rust-lang.org/std/cmp/trait.Ord.html), which seems to be required
    for consolidating rows. `serde_json::Value` is not order-able, and there isn't a good way to
    define how a generic JSON value should be ordered.
* Maintaining `objects_vX.rs` with Rust types instead of an `objects_vX.proto`.
    * Instead of defining our Stash types in protobuf we could keep them defined in Rust and follow
    the same approach for snapshotting the `objects_vX.rs` files these types would be in.
    * ❌ An issue with this approach is "snapshot hygiene". Nothing would prevent us from adding
    dependencies to these snapshots, and to be correct we'd also need to also snapshot the
    dependency. But we don't have a tool to assert dependencies are snapshotted, so a dependency
    could change which could silently introduce a change to the serialized format, causing bugs.

### Why this approach?

I believe this is the right design because of the following reasons:

* It allows us to write easy to reason about migration functions that convert from `TypeA` to `TypeB`.
* Introduces automated checks to make sure a migration exists whenever Stash objects change.
* Old migrations do not need to change when new fields are introduced, since they can operate on
old versions of the types.
* Introduces the possibility of simulation testing for upgrades, since we maintain the history of
the schema of the Stash, through snapshotting.

Overall I believe it achieves the goal of "fearless migrations", for the lowest possible
engineering cost.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

* How important do we believe human readability of the Stash is?
* ~~Does the Stash need to be JSON for billing purposes? This [issue](https://github.com/MaterializeInc/materialize/issues/14264) indicates it might, but this [comment](https://github.com/MaterializeInc/materialize/issues/14264#issuecomment-1218250985) indicates it does not.~~
    * **Answer:** No. `environmentd` and `stash-debug` are the only two things that directly depend
    on the Stash. If application code wants to access the Stash, it needs to do it through
    `environmentd`.
* Is it important to maintain uniqueness checks when doing Stash migrations?

# Future work
[future-work]: #future-work

* As mentioned in [testing-and-observability] the most immediate followup work could be to
generate old versions of the Stash using our snapshotted types for testing. We could also generate
very large Stashes for load testing.
* Not necessarily related to migrations, but if the Stash is not human readable, and requires
`stash-debug`, that opens up the possibility of compressing the SQL statements stored in the
`create_sql` column of the `"items"` collection with an algorithm like `brotli`. Theoretically
this reduces the amount of data we're storing in CockroachDB, and improves the speed of the Stash.
    * Note: CockroachDB already compresses data with `snappy`, so we probably wouldn't move the
    needle much if we also compressed the data ourselves.
