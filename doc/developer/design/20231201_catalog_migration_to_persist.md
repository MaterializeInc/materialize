# Catalog Backend Migration

- Associated: https://github.com/MaterializeInc/materialize/issues/20953
- Associated: https://github.com/MaterializeInc/materialize/issues/22392

<!--
The goal of a design document is to thoroughly discover problems and
examine potential solutions before moving into the delivery phase of
a project. In order to be ready to share, a design document must address
the questions in each of the following sections. Any additional content
is at the discretion of the author.

Note: Feel free to add or remove sections as needed. However, most design
docs should at least keep the suggested sections.
-->

## The Problem

Currently, we persist all users catalog data in the stash. We would like to move all of that data
to Persist. This will have the following benefits:

- A step towards architecting a shareable catalog that will contribute to use case isolation.
- Reduced operational overhead related to maintaining the stash. Note, the overhead is completed
  removed because the storage controller still used the stash.

## Success Criteria

All user catalog data is stored in Persist.

## Out of Scope

- Fully shareable differential catalog.
- Use case isolation.
- Zero downtime upgrades.

## Solution Proposal

### Config Collection

The config collection is a subset of catalog data that can never be migrated. As a consequence, this
data can be read before the catalog is fully opened and can be used to bootstrap an environment. The
config collection has keys of type `String` and values of type `u64`. Booleans can be modeled using
this collection by storing a `0` for `false` and `1` for `true`. This proposal will add a key called
`tombstone`, with a boolean value that indicates if a specific catalog backend is retired. A value
of `false` means that the backend is not retired (i.e. is still in use) and a value of `true` means
the backend is retired (i.e. is not still in use).

### Epoch Fencing

The catalog durably stores a fencing token called the epoch. All catalog writers and readers also
store an in-memory epoch. When a new writer or reader connects to the catalog they store the
previous epoch plus one in memory and persist this new value durably. Every time a reader or write
performs a read or write, they compare their in-memory token with the persisted token, and if
there's a difference, the operation fails. As a result, whenever a reader/write increments the epoch
durably, they are effectively fencing out all previous readers/writers.

### Migrate From Stash to Persist

Below are the steps to migrate from the stash to persist.

1. Increment the stash catalog epoch.
2. Increment the persist catalog epoch.
3. Set the stash tombstone to `true`.
4. If the persist tombstone is `false`, then we're done.
5. Read stash catalog snapshot.
6. Write catalog snapshot to persist.
7. Set the persist tombstone to `false`.

NOTE: Steps (6) and (7) can be done as a single compare and append operation as a performance
optimization.

### Rollback From Persist to Stash

Below are the steps to rollback from persist to the stash.

1. Increment the stash catalog epoch.
2. Increment the persist catalog epoch.
3. Set the stash tombstone to `false`.
4. If the persist tombstone is `true` or doesn't exist, then we're done.
5. Read persist catalog snapshot.
6. Write catalog snapshot to stash.
7. Set the persist tombstone to `true`.

### States

Below is a table describing all possible states of (stash_tombstone, persist_tombstone) and what they
indicate about where the source of truth catalog data is located.

| Stash Tombstone | Persist Tombstone | Source of Truth | Explanation                           |
|-----------------|-------------------|-----------------|---------------------------------------|
| None            | _                 | Stash           | Migration has never been initiated.   |
| _               | None              | Stash           | Migration has never fully completed.  |
| Some(true)      | Some(false)       | Persist         | Migration has completed successfully. |
| Some(false)     | Some(true)        | Stash           | Rollback has completed successfully.  |
| Some(true)      | Some(true)        | Stash           | Migration crashed before completing.  |
| Some(false)     | Some(false)       | Persist         | Rollback crashed before completing.   |

### State Transitions

Below is a state transition diagram that shows all possible states of 
(stash_tombstone, persist_tombstone) and how the states transition. Each node has the format of
(stash_tombstone, persist_tombstone). Each edge indicates what steps from the algorithm above
triggers that specific state transition. Next to each node is either `Stash` if the stash is the
source of truth, or `Persist` if persist is the source of truth.

![state-transitions](./static/catalog_migration_to_persist/catalog_migration_multi_tombstone.png)

## Alternatives

<!--
What other solutions were considered, and why weren't they chosen?

This is your chance to demonstrate that you've fully discovered the problem.
Alternative solutions can come from many places, like: you or your Materialize
team members, our customers, our prospects, academic research, prior art, or
competitive research. One of our company values is to "do the reading" and
to "write things down." This is your opportunity to demonstrate both!
-->

## Open questions

None
