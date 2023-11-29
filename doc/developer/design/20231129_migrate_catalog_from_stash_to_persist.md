# Migrate Durable Catalog Storage

- Associated: https://github.com/MaterializeInc/materialize/issues/22392


## The Problem

The catalog currently uses the stash to durably store all of its contents. We would like to migrate
from the stash to persist to support Platform V2 efforts.

## Success Criteria

- Add the ability to migrate an environment from the stash to persist.
- Add the ability to roll back an environment from persist to the stash.

## Out of Scope

- Use case isolation.
- Differential catalog.

## Solution Proposal

This section describes extra metadata to add to the catalog, the process for migrating to persist,
and the process for rolling back to the stash.

### Config Collection

The config collection is a subset of catalog data that can never be migrated. This gives us the
useful property of always being able to read config data, even before migrations and catalog opening
have been executed. Most of the config data is used for bootstrapping a new environment. The keys
of the config collection are `String` and the values are `u64`. This proposal adds a new key
to the config collection called `tombstone`. A value of `1` indicates `true` and a value of `0`
indicates `false`.

### Epoch fencing

All read and write operations from and to both the stash and persist will fail if the catalog
executing the operation has an epoch that doesn't match the largest epoch persisted. As a result,
once a catalog increments the epoch, all previous catalogs are fenced out of reading and writing.

### Migrate to Persist

  1. Increment epoch in the stash.
  2. Increment epoch in persist. 
  3. Set stash tombstone to `true`.
  4. If persist tombstone is `false` then return early.
  5. Read copy of all stash data.
  6. Write stash data to persist and set tombstone to `false` (atomically). 

### Rollback to Stash

  1. Increment epoch in the stash.
  2. Increment epoch in persist.
  3. Set persist tombstone to `true`.
  4. If stash tombstone is `false` then return early.
  5. Read copy of all persist data.
  6. Write persist data to stash and set tombstone to `false` (atomically).

TODO(jkosh44) WRONG!!! what if they're both `true`? then we don't know which is the source of truth.

## Minimal Viable Prototype

<!--
Build and share the minimal viable version of your project to validate the
design, value, and user experience. Depending on the project, your prototype
might look like:

- A Figma wireframe, or fuller prototype
- SQL syntax that isn't actually attached to anything on the backend
- A hacky but working live demo of a solution running on your laptop or in a
  staging environment

The best prototypes will be validated by Materialize team members as well
as prospects and customers. If you want help getting your prototype in front
of external folks, reach out to the Product team in #product.

This step is crucial for de-risking the design as early as possible and a
prototype is required in most cases. In _some_ cases it can be beneficial to
get eyes on the initial proposal without a prototype. If you think that
there is a good reason for skpiping or delaying the prototype, please
explicitly mention it in this section and provide details on why you you'd
like to skip or delay it.
-->

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

<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->
