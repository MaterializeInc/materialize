This feature was implemented, but then removed in May 2022.

# Disable user indexes mode

## Summary

When users have views causing OOM crashes, we want to provide a mechanism that
still lets the cloud team boot into Materialize and take actions to remediate
the issue, e.g. drop the OOMing view. To accomplish this, we'll introduce a new
boot mode that starts Materialize as normal with the exception of not shipping
any dataflows for user-created objects, such as materialized views.

Ideally, we'll also provide a mechanism that lets users enable dataflows for
their objects while Materialize is running using a statement such as `ALTER
INDEX <index name> SET ENABLED`.

### Using "Disable user indexes mode"

This feature's initial iteration will find most use from expert users of
Materialize, e.g. the cloud team. Using it, they'll be able to boot Materialize
in this mode, and use the system's reporting metrics to determine which indexes
caused the user grief, e.g. an OOM crash, and drop it. They can then turn on the
remaining indexes one-by-one to ensure the system doesn't crash. If it the issue
is fixed, they can then reboot with all of the indexes running.

This feature requires "expert"-level use of Materialize to determine which
indexes caused the crashes, which is most easily understood using our system
metrics. We lack comprehensive education around these metrics, so it's unclear
how most users could suss out where there problems are.

## Goals

- Support booting without starting any user-created dataflows.
- Introduce a `--disable-user-indexes` flag to Materialize that lets users
  easily turn on this mode.
- Add support for `ALTER INDEX ... SET ENABLED` to enable dataflows for objects
  while Materialilze is running, and lift the restrictions imposed on
  catalog-only mode for objects that use those dataflows.

  Note that this statement will be "intrinsically idempotent," so `ALTER INDEX
  ... SET ENABLED` will always succeed, even if Materialize was not booted in
  catalog only mode or its dataflows are already active. This matches PG's
  behavior for `ALTER` statements like `ALTER TABLE a ALTER COLUMN x SET NOT
  NULL`, which you can run repeatedly.
- Minimize interference with potentially performance-sensitive operations, such
  as `SELECT`. Note that planning long-running services such as views will
  likely dominate the time that any check here could potentially take.

## Non-Goals

- We are not planning to add a statement to re-enable the dataflows for all
  user-created objects; users can either accomplish this with a script or by
  simply rebooting _not_ in catalog-only mode.
- Future iterations will support enabling indexes on named objects rather than
  specific indexes, e.g. `ALTER TABLE...SET INDEXES ENABLED`.
- This doc does not consider the potential design of disabling indexes at run
  time, e.g. `ALTER INDEX ... SET DISABLED`, though it seems possible to use a
  process similar to booting without user indexes to remove all dataflows that
  use the index, and then remove it from the `Catalog`'s `indexes` field.

## Description

There are two main considerations in implementing this mode:
- Booting without user indexes.
- Enabling user indexes at runtime.

### Booting without user indexes

@mjibson came up with the idea of making this mode behave akin to removing the
`MATERIALIZED` keyword from all catalog objects before booting and letting the
system simply behave as it would in that case. (The exception to this behavior
being tables, which we'll address separately.)

To accomplish this, we can...
- Add an `enabled` field to `Catalog::Index` to express whether or not the index
  should be usable by the system.

  We then want to use this value to prevent the system from using
  disabled indexes:
  - Prevent [adding disabled
    indexes]((https://github.com/MaterializeInc/materialize/blob/8fad35548028ef9eac6866fdf679a57dd90e47c2/src/coord/src/catalog.rs#L1252-L1256))
    to `Catalog.indexes`, which we'll rename to `enabled_indexes` to make its use more clear.
  - Prevent shipping any dataflows on user indexes, by slightly amending
    [`build_index_dataflow`](https://github.com/MaterializeInc/materialize/blob/93466416ba9813ec8ff4e52b039d76036ef36329/src/coord/src/coord/dataflow_builder.rs#L156)
    to return an `Option<DataflowDesc>` that will be `None` on for disabled indexes.
- Propagate the presence of the `--disable-user-indexes` flag to the `Catalog`.

  This lets us create a convenience function, `Catalog::enable_index` to express
  the proper value of any given `Catalog::Index`'s `enabled` field.

#### Table details

This design still lets users `SELECT` from tables, but never returns any data.
I'm a little fuzzy on why this would be the case, but in my prototype that is
the observed behavior.

However, we have to prevent users from inserting data into tables because their
indexes will not exist. To prevent this, we'll need to return an error from
`sequence_send_diffs` when in `disable_user_indexes` mode when we see that the
table has no indexes in the catalog. Checking for indexes is the most reasonable
design because there is no time during `sequence_insert` where we have
visibility into which indexes the dataflow layer plans to use to service the
`INSERT`.

### Enabling indexes

When we receive a `ALTER INDEX ... SET ENABLED` command from the user, we'll:

1. Ensure the named object is an index and error if not.
1. Check if `disable_user_indexes` is `false` and return OK if so.
1. Get the index's `GlobalId`.
1. Get the index's parent object, and see if the index is listed as a member of
   its `indexes`. Return OK if so because it was already enabled.
1. Get the index `CatalogEntry` and amend its `enabled` value to `true`.
1. Use a new `enable_index` function to insert the index in the `Catalog`'s
   `index` field.
1. Use `build_index_dataflow` using the modified `Index` object to start the
   previously avoided dataflow.

This mechanism is also easy to extend for enabling all of the indexes on a
table, etc.

## Alternatives

For the prior consideration, see the section **Previous iteration ––
Catalog-only mode**.

### Disabling _N_ DDLs

@cuongdo suggested an approach that lets users rolllback some number of
index-creating statements, such as `--disable-recent-ddls 1`; this might need to
change to something like `--disable-recent-indexes`, but the thrust of the idea
still stands.

In one sense, this proposal reduces to booting without user indexes for the last
`n` DDL statements, but allowing all that precede it to boot with their indexes.
A straightforward implementation of this approach is immediately tractable and
would let us access nodes with both indexes on and off.

The cloud team doesn't think that this kind of granularity is a hard requirement
for the time being, though, and is OK with the UX of disabling either all or
none of the indexes.

Disabling a subset of indexes also doesn't simplify the mechanism for turning
indexes back on; that work would still need to be done. And if we want to let
users enable arbitrary indexes, we could implement this feature
(`--disable-recent-ddls 1`) using that mechanism.

In summary, I think this approach would be efficient if the cloud team wanted
more granularity in determining which indexes to have on or off.

### Winnowing down users' abilities in this mode

The original proposal for this mode was called "catalog-only mode", which was a
little vague to users as to what it did, so I am suggesting we change the name
to "disable user indexes."

However, in the spirit of a kind of "catalog-only mode", @cuongdo questioned
whether or not we should limit the actions users can take in this mode.

A viable set of exclusions would be to not let users create any objects in the
catalog, aside from temporary views.

I think this could have a great UX in that it continually reminds users that
they are in a debug mode, and prevent them from foot-gunning themselves.
However, this feels like an orthogonal feature and would be more well considered
as another feature or extension of this mode.

For kindling on that topic, I think it might be desirable to call this mode
something like `--debug-disable-user-indexes`, and any kind of `debug` mode we
add in the future would also enter into this state by default.

## Open questions

> How will these errors align with transactions' semantics?

@mjibson says that this shouldn't cause any issues.

# Previous iteration –– Catalog-only mode

## Description

The `Coordinator` will handle the enforcing catalog-only mode on objects, i.e.
disallowing dataflows from shipping, querying user-crated objects, or inserting
into tables; these are the potentially prohibited actions. To support this enforcement,
we'll add a new field to the `Coordinator`:

```rust
pub struct Coordinator {
    ...
    /// When `None`, all user objects can create/access dataflows; when `Some`, only the objects
    /// with an ID in the hashmap are allowed.
    catalog_only_mode_allow: Option<HashSet<GlobalId>>,
}
```

When booting normally, we'll set this field to `None`, and
`Some(HashSet::new())` when booting in catalog-only mode.

Then, whenever a user or the system attempts to perform a potentially prohibited
action on an object, we'll allow the action if any of the conditions are true:
- `catalog_only_mode_allow.is_none()`
- The `GlobalId` is not a user-created object
- Reads/peeks/`SELECT`s occur on the fast path, i.e. they are coming from a
  materialization, and a selected index is enabled.

  Note that:
  - This might mean that the optimizer chooses a set of indexes for a query that
    have not been enabled, even though others have been, so we will message which
    indexes need to be enabled or dropped for the query to potentially work.
    Emphasis on might; I'm not sure how optimistic or pessimistic the generated
    sets of indexes are.
  - Querying unmaterialized sources with unmaterialized views becomes
    impossible. However, users could create a new materialized view, and then
    enable it, to query unmaterialized sources in catalog-only mode.
- Inserts occur on tables IDs that have been implicitly enabled by having a
  covering index enabled.

### Enabling user arrangements

When we receive a `ALTER INDEX ... SET ENABLED` command from the user, we'll:

1. Ensure the named object is an index and error if not.
1. Check if `catalog_only_mode_allow.is_none()` and return OK if so.
1. Get the index's `GlobalId`.
1. Check if the unwrapped `catalog_only_mode_allow` contains the index's ID,
   indicating it's already been restarted and return OK if so.
1. Use something akin following code to ship the dataflow we previously
   prohibited:
    ```rust
    let df = self.dataflow_builder().build_index_dataflow(id);
    self.ship_dataflow(df);
    ```
1. Add the index's `GlobalId` to `catalog_only_mode_allow`.
1. If the named index is an index on a table, add the table's `GlobalId` to
   `catalog_only_mode_allow`. This is necessary because sequencing volatile
   inserts (i.e. writing to tables) doesn't declare which index/arrangement will
   get used, so we have to save this inference. Also, all indexes in Materialize
   are covering, so this should continue to allow writes to all columns.

   If my memory is mistaken and this isn't the case (i.e. indexes can offer
   partial cover), we'll only write the table's `GlobalId` to
   `catalog_only_mode_allow` when a covering index gets enabled.
