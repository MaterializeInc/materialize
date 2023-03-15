# Backup and restore
- Feature name: backup/restore
- Associated: [#17605](https://github.com/MaterializeInc/materialize/issues/17605)

# Summary

> 👉 **This is not a complete design** — while it uses the standard design doc template, it does not include an implementation plan, and various details are subject to change. These will be flagged with callouts like this one.
>
> **Why do the design work at all, if we’re not ready to implement it?** The current doc is intended to sketch out the rough scope of a backup system, discuss the various options, and identify systemic risks or important foundational work we have not done. For example: we’ve identified S3 versioning as necessary for the most plausible approaches to backing up our S3 data, and it’ll be much easier to adopt soon than in six months as Materialize grows. We’re glad we figured this out sooner than later!


# Motivation

Materialize does not currently back up its persistent state: if we accidentally delete or corrupt something, it’s gone forever. This is a long-term risk for Materialize, especially as we grow to enable higher-value use cases for our users.

## Scope

Reasons one might want backup/restore, and whether they’re in scope for this design —

| Scenario | Example | In scope? |
| --- | --- | --- |
| User-level backups | A user wants to explicitly back up a table, either ad-hoc or on some regular cadence. | No. (A S3 sink/source would provide similar functionality and integrate better with the rest of the product surface.) |
| User error | A user puts a bunch of valuable data into a table, deletes a bunch of rows by accident, and asks us to restore it for them. | No. (Possibly useful for a system of record, but substantially more complex.) |
| Controller bug | The compute controller fails to hold back the since of a shard far enough in a new version of , losing data needed by a downstream materialized view. | Yes! |
| Persist bug | A harmless-seeming change to garbage collection accidentally deletes too much. | Yes! |
| Snapshot | A shard causes an unusual performance problem for some compute operator, and we’d like to inspect a previous state to investigate. | Nice to have. |
| Operator error | An operator typos an aws CLI command, accidentally deleting blobs that are still referenced. | Yes. (Impossible to prevent an admin from deleting data entirely, but it’s good if we can make ordinary operations less risky.) |

Motivated by the above and [some other feedback](https://github.com/MaterializeInc/materialize/issues/17605), this design doc focuses on infrastructure-level backups (without no product surface area) that optimize for disaster recovery. For other possible approaches or extensions to backup/restore, see the [section on future work](#future-work).

## Goals

Our primary goal is to periodically back up all of the state in every environment. In particular:

- High frequency: at most one hour between restore points. (Point-in-time is great, but not required.)
- Moderate duration: backups retained for about a month. (Long enough to be useful even for long-running incidents; short enough to avoid GDPR concerns.)
- Allow restoring an environment to a particular backup, ad-hoc.
- Backups should be isolated: it should be impossible for ordinary code to mess with historical backups.

It’s helpful if we’re also able to use backups for investigations and debugging historical state, but this is secondary to the goals above.

## Non-goals

- User-facing API, whether in SQL or in the dashboard. If users need to request a restore, they can write to support.
- Long-term backups. It seems unlikely that users will want to restore an entire environment to a months-old state, and we want to avoid any GDPR or other compliance concerns.
- Partial backups. Backups will happen for a full environment.
- Restoring a backup while an environment is up: it’s fine to assume the environment is shut down during a restore.

# Explanation

The state of a Materialize environment is held in a set of Persist shards. (Along with some other CRDB state like the stash.) From Persist’s perspective, each shard is totally independent of the others; `environmentd` is responsible for coordinating across shards in a way that presents a consistent state to the user.

This design is thus roughly composed of two distinct parts:

- Backing up and restoring an individual Persist shard.
- Coordinating the backup and restore process for the set of shards such that the state of the overall environment remains consistent.

> 👉 We’ve chosen to highlight the most promising options in this section, but see the alternatives section below for other options and the tradeoffs between them.

## Single-shard backups

### Correctness

Persist is already a persistent datastructure: the full state of a shard is uniquely identified by the seqno of the shard. A valid backup of a persist shard is just some mechanism for making sure that we can restore the state of a shard to the contents as they were at a particular seqno.

### Implementation

We plan to take advantage of features built into our infrastructure:

- [CRBC changefeeds](https://www.cockroachlabs.com/docs/v22.2/change-data-capture-overview) allow capturing all the changes to your data in CRDB and exporting it as a stream. We already record these changefeeds to S3.
- [S3 Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html) permits keeping around old versions of the objects written to S3. (Deletes are translated into tombstones.) This is usually used in concert with an [S3 lifecycle policy](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html), to avoid retaining data forever.

We’d configure both systems to keep around this fine-grained change data around for the last month, after which it can be deleted.

To restore a shard at a particular time, we would:

- Choose a CRDB timestamp that corresponds to the requested wall-clock time.
- Replay the changefeed back into CRDB until the corresponding timestamp.
- This CRDB data will hold references to objects in S3, some of which will reference yet other objects. Recursively walk the DAG of references; when we encounter a reference to a since-deleted blob, undelete that blob by removing the tombstone.

While this imposes some additional cost, it’s modest both in absolute terms and relative to our other options. See [the appendix](#appendix-a-s3-costs) for more details.

## Whole-environment backups

### Correctness

Unlike for a single shard, the state of an entire environment can not be represented with a single number. (Including the `mztimestamp` — the timestamp of a shard can lag arbitrarily behind, if eg. the cluster that populates that shard is crashing.) Conceptually, an environment backup is a mapping between shard id and the seqno of the per-shard backup. (Along with any additional environment-level state, like the stash.)

Correctness criteria are also a bit more subtle, and involve reasoning about the dependencies between collections. We’re aware of two important correctness properties:

1. **No broken since holds.** If B depends on A, B will maintain some hold on the **since** of A. (Commonly, this will mean that A’s since can’t advance past B’s upper.)
2. **No time travel.** If B depends on A, B’s upper should not be past A’s upper. (If A is a table and gets restored to some old state, B shouldn’t contain any of the updates from A that no longer exist.)

In a running environment, property 1 is carefully enforced by the various controllers, and property 2 is a consequence of causality. We need to ensure make sure that our restore process respects these properties as well.

### Implementation

Our proposed approach to restore relies on the ordering properties guaranteed by CRDB:

- Choose a CRDB timestamp that corresponds to the desired time.
- For every shard in the environment, identify the seqno corresponding to that CRDB timestamp, and run the per-shard restore process described above.

This is straightforward to implement, and does not require any significant changes to the running Materialize environment. It also easily extends to handle non-shard state that’s also kept in CRDB, like the stash.

This is also not strictly safe, since CRDB is not a strictly serializable store; in particular, the docs [describe the risk of a causal reverse](https://www.cockroachlabs.com/blog/consistency-model/#as-of-system-timequeries-and-backups), where a read concurrent with updates A and B may observe B but not A, even if B is a real-time consequence of A. In the context of Materialize, if A is a compare-and-append and B is a compare-and-append on some downstream shard, a causal reverse could cause our backup of B to contain newer data than our backup of A, breaking correctness requirement 2.

A causal reverse is expected to be generally quite rare, and if encountered we can just try a restore at a nearby timestamp. However, [see below for a discussion of the alternatives](#conclusion-and-alternatives).

# Reference explanation

> 👉 **Not yet!** We'll return to this when we're closer to starting work.

# Rollout

We already capture CRDB changefeeds in some environments; we’ll need to enable this everywhere, and to make sure we’re choosing our retention carefully.

We’ll also need to enable versioning and a lifecycle policy. We expect no problems with this, though there is always risk when changing a whole-bucket config; we’ll want to bake any change here in staging before moving to production.

Since there’s no user-facing surface area to this feature, we do not expect to add additional code to the running environment. However, we will need code that can rewrite CRDB and update data in S3 while an environment is down. `persistcli` seems like a natural place for this, and ad-hoc restores can run as ad-hoc Kubernetes jobs.

## Testing and observability

The code we write for the restore process should be tested to the usual standard of Persist code, including extensive unit tests. (This may require writing a new Blob implementation to fake versioning behaviour, or running one of the many S3 test fakes available.)

Since backups are often the recovery option of last resort, it’s useful to be able to test that the real-world production backups are working as expected. We should consider running a periodic job that restores a production backup to some secondary environment, brings that environment back up, and checks that the restored environment is healthy and can respond to queries.

## Lifecycle

Configuration changes like enabling S3 versioning apply bucket-wide, so they cannot be incrementally rolled out to users. We’ll want to roll these changes out carefully through staging.

The restore process does not run in a user’s environment, so it does not go through the usual deployment lifecycle. However, any periodic restore testing we choose to run will need resources provisioned somewhere.

# Drawbacks

Backups are widely considered to be a good idea, though any particular approach we choose will have its own drawbacks.

The proposed approach to backups leans pretty heavily on the specific choices of our blob and consensus stores, otherwise well-abstracted in the code. This means that any change in the stores we use would involve substantial rework of the approach to backup/restore. We consider this to be fairly low likelihood.

The proposed CRDB backup strategy may lead to an unrestorable backup as of a particular CRDB timestamp if we experience a causal reverse. However, we expect this to be rare and easy to work around when it does occur.

# Conclusion and alternatives

A few other options we’ve discussed for per-shard and per-environment backups, with a discussion of the tradeoffs.

## Alternatives to single-shard backups

### Copying-based approaches

One natural way to make a backup of a shard would be to copy out a state rollup and all the files it recursively references out to some external location. This is straightforward to implement using the existing blob and consensus APIs and requires no new infrastructure.

- PUTs in S3 are (relatively) very expensive, and GETs marginally less so. (On the other hand, we spend comparatively very little on storage.) Copying out a significant percentage of the files we write would push up our S3 costs in a way that leaving data in place does not.
- Somebody has to trigger the copying! If `environmentd` is coordinating the environment-wide backup process, making it copy every file in every shard is lots of extra work. (Though thankfully S3 can copy files without transferring all the data through the client.) We might need to introduce an additional sidecar just for backups, which is significant operational burden.

### “Leak”-based approaches

If we’ve decided we don’t want to copy data, another option would be to teach the current persist implementation to designate certain rollups as “backup” rollups, and avoid deleting them and the files they reference. This is a more invasive change to Persist, but would allow us to restore the chosen backups without having to “undelete” any data.

- This complicates the running system, and introduces some new risk: I could easily introduce a bug that caused Persist GC to incorrectly delete blobs that were part of a backup.
    - Other approaches don’t run this risk: in the copying-based approach, a production environment should never need to delete from the backup location, only write to it; if using S3 versioning, our production environments don’t need permissions to read or modify anything but the most current version.

## Alternatives to whole-environment backups

### Leverage CRDB full or incremental backups

While the proposed backup/restore design uses CRDB changefeeds, it could just as easy take advantage of CRDB’s built-in functionality for full or incremental backups. This would also be able to take advantage of CRDB’s built-in restore mechanism instead of requiring us to “replay” a changefeed back into the database.

- CRDB suggests doing full backups overnight. A high frequency of full backups may significantly increase the load on the database, in a relatively “spiky” way.
- Persist causes a large amount of churn in CRDB: there are a small number of rows in the database with a relatively short lifetime. Incremental backups would require us to configure CRDB to store tombstones around for much longer than they already do, which we believe would have an unacceptable impact on read latency.

### Walking the dependency graph in `environmentd`

Imagine a fictional controller that:

- manages all persistent state in an environment, including the stash;
- tracks the dependencies between all collections, even across intermediate non-durable state like indices;
- has the ability to hold back the since on shards it controls.

In this world, a toy backup algorithm for a whole Materialize environment might look like:

- Add the the stash to the backup.
- Enumerate all the shards in the environment, according to the catalog as of the moment it was backed up.
- For every shard S, in reverse dependency order (ie. if A depends on B, process A first):
    - Stop advancing the since hold that S places on its dependencies.
    - Add S to the backup.
    - If any shards depend on S, continue advancing the since holds they place on S.

Correctness property 2 is guaranteed by processing shards in reverse dependency order, so the backup of a shard will always be older than the backups of the shards it depends on. We maintain property 1 with the since hold juggling; if the hold was valid at the time the downstream shard was backed up, that same since hold will be maintained at the time the upstream gets backed up.

- The fictional controller setup described here only vaguely resembles how the controller is set up today. (The catalog is not a persist shard; no single controller or domain of responsibility understands all the dependency relationships between shards.) Changing the system to behave in a way where this sort of backup was possible would be a significant cross-team lift, and it’s unclear if the necessary changes would be considered a net positive.
- This gives extra work to `environmentd`. For our favoured per-shard backup approach, the performance cost should be pretty marginal. However, it does introduce complexity into an already very challenging to reason about codepath.

> 👉 This approach to backups provides stronger correctness guarantees than the proposed CRDB approach. From an engineering perspective I’m not sure the significant additional complexity is justified, though. (And if we did prefer this option, we may want to start with the proposed approach regardless to reduce risk as we build it out.)

### AWS Backup

[AWS Backup](https://docs.aws.amazon.com/aws-backup/latest/devguide/s3-backups.html) allows making periodic or incremental backups of an S3 bucket, with no additional transfer costs. It relies on S3 versioning, and will back up every version of an object in the bucket.

- Since S3 is not strictly serializable, there’s no reason to think that the contents of a restored S3 bucket would be consistent with any CRDB snapshot or even internally consistent.
- AWS Backup relies on having versioning enabled, and we believe using versioning is enough to serve our requirements on its own.

## Alternatives to doing backups at all

Doing nothing is always an option, and maybe a fine one: we’re explicit with our users that we’re not to be treated as a system of record yet.

- Materialize contains a great deal of state that may not be easy to recreate: the exact definition of sources and views; the state of external systems like sources and sinks; the contents of internal tables. Losing Materialize state could result in days or weeks of recovery week for a user.
- As we start thinking about taking on more business-critical use-cases, being able to recover our user’s data after some incident will become increasingly important.

# Unresolved questions

## External state

Materialize’s sources and sinks also store data in the systems they connect to. In general, we’re unable to back up and restore this data fully. (If we restore to a week-old backup, the data our Kafka source had ingested since then might be compacted away.) **How will sources and sinks recover from a restore? If they can’t, is there a good way to signal this to a user?**

## Non-persist state

This document focusses on Persist state, but the stash is not stored in Persist. **Do we want to move the stash to Persist? Can we rely on stash being stored in the same CRDB cluster as Persist’s consensus state indefinitely?**

# Future work

## Partial restores

This document describes approaches to backing up a single Persist shard, as well as the full environment. In some cases, we may actually only want to restore a *subset* of the shards; for example, if a source breaks with some bug, we may want to restore its state along with the state of all its downstreams. This does not even remotely resemble a historical state of the system!

It seems plausible that one could reconstruct such a state by inspecting the full history of the relevant shards and with a good understanding of the dependency relationships between them.

## User-facing backup and restore

One can imagine wanting a user-facing syntax for backup and restore: for example, to make an ad-hoc snapshot of a table before making changes, or to periodically back up some critical dataset.

We expect this need to be served by future sources and sinks, [like a potential S3 integration](https://github.com/MaterializeInc/materialize/issues/17605#issuecomment-1432420387), instead of relying on any infrastructure-level backup.

# Appendix A: S3 costs

It’s difficult to attribute costs exactly, since we have only rough aggregate numbers and both S3 and CRDB are shared resources. Nonetheless, and very approximately:

- 70% of our S3 spend is on PUTs, 25% is on GETs, and 5% is on GB-months of storage.
- The average object we write to S3 is stored for about a day. The average *byte* we write to S3 is stored for a couple of weeks. (The discrepancy is because smaller files tend to be compacted away quickly, while larger ones stick around.)

The only cost of S3 Versioning is the additional object storage. If we decide to adopt S3 Versioning and keep all versions for a month, we’d add ~10% to our overall S3 spend.

If we chose a object-copying approach to backups, and we copy objects hourly, the average object will be present in 24 backups. If we took the naive approach of copying every object at backup time — which is priced as a PUT — this would add an order of magnitude to our S3 costs. Even with the maximally clever approach, where we deduplicate and reference count our backed-up objects, we’d be increasing our spend by about half even before accounting for the additional storage costs.

We spend roughly an order of magnitude more on CRDB than S3, so even significant percentage changes in our S3 usage have a relatively modest impact on our overall spend. (Without accounting for the CPU cost of interacting with Persist in EC2.)
