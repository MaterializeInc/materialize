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

  1. Refactor existing `sql`, `expr`, `compute-types`, and `transform` crates into `optimizer` and `optimizer-types` crates.
  2. Version the `optimizer` crate. The `optimizer-types` crate will contain traits and types that are stable across all three versions (e.g., sufficient interfaces to calculate the dependencies of various expression `enum`s without fixing any of their details).
  3. Work on three versions of the `optimizer` crate at once:
     - **unstable**/**hot** active development
     - **testing**/**warm** most recently cut release of the optimizer
     - **stable**/**cold** previously cut release of the optimizer
     These versions will be tracked as copies of the crate in the repo (but see ["Alternatives"](#alternatives)).
  4. Rotate versions treadmill style, in some way matching the cadence of the self-hosted release. (Likely, we will want to use the cloud release to qualify a self-hosted release.)

## Minimal Viable Prototype

### Version names and numbering

Every version of the `optimizer` crate will be numbered. Customers can run SQL like:

```sql
ALTER CLUSTER name SET (OPTIMIZER VERSION = version)
```

where `version` is a version number (or, possibly one of our three fixed names). Note that the optimizer is selected on a per cluster basis. Selecting the optimizer per cluster makes it easier to qualify optimizer releases.

### The `optimizer` and `optimizer-types` crates

The `optimizer` create will containe the definitions of HIR, MIR, and LIR, along with the HIR-to-MIR and MIR-to_LIR lowerings and the MIR-to-MIR transformations. These come from the `sql` (HIR, HIR -> MIR), `expr` (MIR), `transform` (MIR -> MIR), and `compute-types` (LIR, MIR -> LIR) crates.

The `optimizer-types` crate will have traits and definitions that are global across all three live versions of the optimizer. The AST types may change version to version, but the traits will be more stable.

These crates do _not_ include:
  + the SQL parser
  + (\*) SQL -> HIR in `sql`
  + `sequence_*` methods in adapter
  + (\*) LIR -> dataflow in `compute`

The two bullets marked (\*) above are a tricky point in the interface. SQL will _always_ lowers to **unstable**/**hot** HIR; we will have methods to convert **unstable**/**hot** HIR to the other two versions. Similarly, LIR lowers to dataflow from **unstable**/**hot** LIR; we will have methods to convert the other two versions to the latest.

### Supporting qualification

A capture-and-replay mode for events that stores the events in the customer's own environment would allow for (a) customers to easily test and qualify changes, and (b) a push-button way for us to spin up unbilled clusters to evaluate these replays in read-only mode.

## Alternatives

The thorniest thing here is the release cutting and copied code. Would it be better to split off the optimizer entirely into a separate repository? A separate _process_?

## Open questions

- Does **unstable**/**hot** get a version number, or no? (Cf. Debian sid)

- How precisely do we select features out of **unstable**/**hot** to cut a new **testing**/**warm**? If we literally copy the crate three times in the repo, `git cherry-pick` will not be straightfoward to use and it will be easy to lose history.

- Under use-case isolation, can we say select an optimizer per `environmentd` instead of per cluster?

- There are several occasions where the optimizer is called independent of the cluster: views, prepared statements, and as part of `INSERT`, `UPDATE`, and `DELETE`. (Look at `sequence_read_then_write` for an example.) Should these be moved off? Use the active cluster? Automatically optimize with all three?

- Which fixes will we backport? Correctness only? When do we backport a fix and when do we backport a "problem detector" with a notice?
