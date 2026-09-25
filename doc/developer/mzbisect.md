# mzbisect

## Introduction

`bin/mzbisect` localizes data corruption. Given an object that produced, or
participated in a query that produced, a consistency error such as
`Non-positive multiplicity in DistinctBy`, it walks that object's dependency
closure and probes every relation in it to find where the bad data enters.

It replaces the manual incident workflow of connecting as `mz_system`, creating
an unbilled cluster by hand, and querying your way down the dependency tree one
relation at a time.

Everything the tool does is read-only except for creating and dropping its own
scratch cluster.

## Prerequisites

The tool connects to an environment's internal SQL port (6877) as `mz_system`,
because creating an `INTERNAL` replica billed as `free` requires an internal
user. For a production environment that means an approved Teleport access
request. The tool can drive Teleport itself:

```
bin/mzbisect run u4217 \
    --teleport-env aws-us-east-1-<hash>-0 \
    --teleport-request-id <approved-request-id>
```

`--teleport-env` spawns `tsh proxy db --tunnel` and connects through it,
overriding `--db-host` and `--db-port`. `--teleport-request-id` runs `tsh login`
with an approved request first. Both are optional: against a local environment,
the defaults (`localhost:6877`, user `mz_system`) already work, and the standard
`PGHOST`/`PGPORT`/`PGUSER` environment variables are honored.

## Finding a starting point

If you do not already know which object to blame, `scan` lists the persistent
dataflows currently reporting errors and suggests commands to run:

```
bin/mzbisect scan --teleport-env aws-us-east-1-<hash>-0
```

These are candidates, not proof of corruption: the counts include ordinary
dataflow errors such as division by zero. Errors from one-off `SELECT`s never
appear here, because the dataflow is gone by the time you look. In that case
seed the run from the objects the failing query read.

## Running a bisection

```
bin/mzbisect run <object>
```

`<object>` is a name (`name`, `schema.name`, or `database.schema.name`) or an ID
(`u4217`). Naming an index bisects the relation it is built on.

The tool creates a cluster named `mzbisect_<id>_<timestamp>` with one `INTERNAL`
replica `BILLED AS 'free'`, so the customer is not charged and none of their own
replicas do any work. The cluster is dropped on exit. Because cleanup cannot run
if the process dies hard, for example on a network drop or a Teleport
certificate expiry, the tool prints the `DROP CLUSTER` statement up front. Run
it by hand if a cluster is left behind.

## Probes

Each relation in the closure gets up to four probes.

**scan** runs `SELECT count(*)`. This surfaces errors persisted alongside the
data as well as errors thrown while hydrating the relation from persist. If a
relation cannot be read at all, the remaining probes are skipped.

**potato-negative** runs `GROUP BY row(t.*) HAVING count(*) < 1` and reports
rows with non-positive multiplicities. `count` is an accumulable aggregate, so
negative multiplicities flow through as negative counts instead of erroring.
Any such row is corruption.

**potato-duplicates** is the `count(*) > 1` variant. Duplicates are corruption
only where uniqueness is expected, so they are reported informationally and
never on their own change the verdict.

**arrangement** applies to indexed relations. It computes an order-insensitive
multiset checksum at one fixed `AS OF`, both against every replica of the
index's home cluster, which reads the incumbent arrangement, and against the
scratch cluster, which reads fresh from persist. A mismatch means that replica's
arrangement has diverged from the persisted inputs. This is the automated
version of "rehydrate the index and see if the problem goes away".

The arrangement probe is the only one that runs queries on the customer's own
clusters. `--no-fingerprint` confines every read to the scratch cluster, at the
cost of no longer being able to tell a corrupt arrangement from bad persisted
data.

## Reading the output

Each relation prints its probe results as the walk proceeds, then a summary
lists every corrupt object and, for each corrupt object with no corrupt
dependency, a one-line diagnosis. Those are the points where corruption enters
the closure:

* Corruption that reproduces on a fresh read from persist means the data itself
  is bad, or, for a view, that its inputs are bad or its rendering is
  deterministically wrong. Rehydration will not fix it.
* Corruption visible only in a replica's arrangement means the persisted data is
  clean. Recreating those replicas should clear it. Keep one replica around for
  debugging if the incident allows.

Exit code 0 means nothing probed corrupt, 1 means corruption was found, and 2
means the run could not be completed. A probe that fails for an unrelated reason
(a timeout, say) leaves the object's verdict as `unknown` and the summary says
so, so a clean run with failed probes is not a clean bill of health.

## Options worth knowing

`--durable-only` probes only relations that hold state: tables, sources,
materialized views, indexed views, and the seed. Unindexed views are still
walked through, but not probed. Reach for this on deep view stacks, where
probing every view recomputes the same inputs over and over.

`--dry-run` prints the dependency closure and exits without touching the
environment. Use it to see how large a run will be before starting it.

`--skip` leaves an object and its whole subtree out of the walk. Repeatable.
System objects are always skipped.

`--cluster` reuses an existing cluster instead of creating one, and does not
drop it. `--keep` keeps a created cluster so you can keep poking at it by hand.

`--size` sets the scratch cluster size, `50cc` by default. `--timeout` sets the
per-probe statement timeout, 900 seconds by default. A probe that outruns the
timeout is reported as failed, not as clean.
