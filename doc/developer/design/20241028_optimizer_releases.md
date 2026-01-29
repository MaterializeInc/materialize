# Optimizer Release Engineering

## The Problem

Currently, users run on "Materialize weekly", but:

  1. That cannot be case forever, as self-hosted users may not upgrade immediately.
  2. That is not always good for users, who can experience regressions when optimizer changes don't suit their workloads.
  3. That is not always good for people working on the optimizer, who must size their work and gate it with feature flags to accommodate the weekly release schedule.

## Success Criteria

Customers---self-hosted or cloud---will be able to qualify new optimizers before migrating to them.

Optimizer engineers will be able to develop features with confidence, namely:

  - introducing new transforms
  - updating existing transforms
  - targeting new dataflow operators
  - changing AST types for HIR, MIR, or LIR

Optimizer engineers will be able to deploy hotfixes to cloud customers.

## Solution Proposal

We propose the following solution:

  1. Refactor existing `sql`, `expr`, `compute-types`, and `transform` crates to extract `optimizer` and `optimizer-types` crates.
  2. Version the `optimizer` crate. The `optimizer-types` crate will contain traits and types that are stable across all three versions (e.g., sufficient interfaces to calculate the dependencies of various expression `enum`s without fixing any of their details).
  3. Work on three versions of the `optimizer` crate at once:
     - **unstable**/**hot** active development
     - **testing**/**warm** most recently cut release of the optimizer
     - **stable**/**cold** previously cut release of the optimizer
     These versions will be tracked as copies of the crate in the repo (but see ["Alternatives"](#alternatives)).
  4. Rotate versions treadmill style, in some way matching the cadence of the self-hosted release. (Likely, we will want to use the cloud release to qualify a self-hosted release.)

We do not need to commit to this process up front. We can instead begin by:

  - Refactoring the crates (as decribed below)
  - Cutting a **stable**/**cold** V0 release and creating a working **unstable**/**hot** branch
  - Creating a mechanism for dynamically selecting between the two

We can aim for a six month release from **unstable**/**hot** to **testing**/**warm**, but there's no need to hold to that timeline. If things slip much past six months, though, we should consider why we slipped and try to address that in the process going forward.

We will need to think very carefully about the proportion of work done on **stable**/**cold** (for customer needs) and on **unstable**/**hot** (for technical debt paydown and feature development).

## Minimal Viable Prototype

### Version names and numbering

Every version of the `optimizer` crate will be numbered except for the currently active development branch in **unstable**/**hot**, which never receives a number.

### The `optimizer` and `optimizer-types` crates

The `optimizer` crate will contain the definitions of HIR, MIR, and LIR, along with the HIR-to-MIR and MIR-to_LIR lowerings and the MIR-to-MIR transformations. These come from the `sql` (HIR, HIR -> MIR), `expr` (MIR), `transform` (MIR -> MIR), and `compute-types` (LIR, MIR -> LIR) crates.

The `optimizer-types` crate will have traits and definitions that are global across all three live versions of the optimizer. The AST types may change version to version, but the traits will be more stable.

These crates do _not_ include:
  + the SQL parser
  + (\*) SQL -> HIR in `sql`
  + `sequence_*` methods in adapter
  + (\*) LIR -> dataflow in `compute`

The two bullets marked (\*) above are a tricky point in the interface. SQL will _always_ lowers to **unstable**/**hot** HIR; we will have methods to convert **unstable**/**hot** HIR to the other two versions. Similarly, LIR lowers to dataflow from **unstable**/**hot** LIR; we will have methods to convert the other two versions to the latest.

### Versioning the `optimizer` crate

To create a new version of the `optimizer` crate, we will use `git subtree`, which should preserve features.

At first, there will be two versions: V0 **stable**/**cold** and **unstable**/**hot**.

### Supporting qualification

A capture-and-replay mode for events that stores the events in the customer's own environment would allow for (a) customers to easily test and qualify changes, and (b) a push-button way for us to spin up unbilled clusters to evaluate these replays in read-only mode.

## Alternatives

The thorniest thing here is the release cutting and copied code. Would it be better to split off the optimizer entirely into a separate repository? A separate _process_?

## Open questions

- How do we backport features? `git subtree`/`git filter-branch` makes it easy to copy the `optimizer` crate with history, but we will not be able to use `git cherry-pick`.

- Which fixes will we backport? Correctness only? When do we backport a fix and when do we backport a "problem detector" with a notice?

### Where do we select optimizer versions?

Are optimizer versions associated to a cluster or to an environment?

If the former, we'll have:

```sql
ALTER CLUSTER name SET (OPTIMIZER VERSION = version)
```

where `version` is a version number (or, possibly one of our three fixed names). Note that the optimizer is selected on a per cluster basis. Selecting the optimizer per cluster makes it easier to qualify optimizer releases, but raises questions about `CREATE VIEW`, `INSERT`,  `UPDATE`, and `DELETE`, which run (a prefix of) the optimizer not on any particular cluster. (Look at `sequence_read_then_write` for an example.) Should these be moved off? Use the active cluster? Automatically optimize with all three?

If the latter, we'll have a flag to `environmentd`. Flagged environments make it easier to go mztrail in shadow environments and avoids questions about which version of the optimizer to run when not on any particular cluster, but doesn't offer a path to users for release qualification.
