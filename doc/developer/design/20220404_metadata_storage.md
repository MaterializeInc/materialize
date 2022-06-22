# Metadata database for Materialize Platform

## Summary

Each [Materialize Platform layer](../platform/architecture-db.md) in each
customer environment in Materialize Platform needs to durably record its
metadata. What database to use is an open question. The correctness,
performance, scalability, reliability, and availability goals of Materialize
Platform heavily constrain our options. This document details those constraints
and proposes a candidate system for further evaluation.

## Constraints

### Hard requirements

The following are the non-negotiable requirements that flow from Materialize
Platform's product goals.

* **Resilience to single availability zone outages.** Materialize Platform must
  be resilient to an outage in a single availability zone of a cloud provider
  region. Therefore the metadata database must be resilient to outages in a
  single availability zone.

* **Strong consistency.** Materialize Platform intends to offer strict
  serializability, at least in some configurations, that withstands Jepsen
  analysis. This imposes the same constraints on the underlying metadata
  database.

* **Single-millisecond commit latency.** The metadata database will be on the
  critical path for all write queries issued against Materialize Platform, and
  (likely) all read queries in strictly serializable mode. Transactions against
  the metadata database therefore need to exhibit p95 commit latencies on the
  order of single-digit milliseconds.

* **Easy scalability.** The metadata database must scale to support tens of
  thousands of customer environments in a cloud provider region without undue
  operational burden.

* **Compatibility with single binary and bring-your-own-cloud deployments.** The
  metadata database must provide an API that is easily replicable with SQLite in
  single binary deployments. It must also be easy to operate in
  bring-your-own-cloud deployments.

* **Environment schema isolation.** Version upgrades and rollbacks in one
  environment must not be dependent on version upgrades and rollbacks in other
  environments.

* **Security sandboxing.** The database must enforce that an environment can
  only access the data that belongs to that environment.

### Desirables

The following would be nice to have, but are not strict requirements.

* **Consistency across clouds.** The ideal database would have an equivalent
  offering across all of the major cloud providers.

* **Ability to run in test environments.** The ideal database would be easy
  to run in the Antithesis and Jepsen test environments to permit fault
  injection.

* **Environment performance isolation.** The ideal database would provide
  per-environment performance isolation, so that an active or malicious
  environment would not impact the performance of other environments.

* **Migration plan to STORAGE collections.** In the far future, Materialize may
  be able to use its own [STORAGE](../platform/architecture-db.md#storage) layer
  to store its metadata. The ideal database would not inhibit this future.

### Nonrequirements

The following are explicitly not requirements of the metadata storage layer.

* **Resilience to region outages.** The metadata storage layer does not need to
  tolerate the simultaneous failure of multiple availability zones within a
  cloud provider region.

* **Distribution across multiple regions.** The metadata storage layer does not
  need to support queries across multiple regions. We are comfortable operating
  a separate metadata storage layer per region.

* **Cross-environment reporting queries.** The metadata storage layer does not
  need to support queries across multiple customer environments for
  introspection or reporting.

## Discussion

### Current state of affairs

Materialize Platform currently stores metadata in SQLite databases on an EBS
volume. The hard requirements immediately rule this situation out as a viable
long term option ([details][sqlite-problems]).

### Abstraction

`materialized` is moving towards storing all its metadata via the [`Stash`] API,
which abstracts over any particular database's API. This API is intentionally
kept simple enough that `Stash` can, in principle, be backed by any of the
following:

  * A SQL database with strictly serializable transactions
  * A key–value store with strictly serializable transactions
  * Materialize's own STORAGE layer

There has been some discussion of introducing a networked service to mediate
between `materialized` and the database. The idea is that this service would
decouple `materialized` from the database, allowing for changes to the database
interactions to be deployed on a separate schedule from `materialized`. However,
this network service would introduce at least several milliseconds of latency,
additional operational burden, and additional backwards compatibility surface
area. I think `Stash` provides the same decoupling benefits without any of the
other downsides. `materialized` is code that's entirely under our control; we
can deploy any necessary changes to `materialized` with whatever frequency is
necessary.

### Options for evaluation

* DynamoDB

  Fatal flaw: transactions have an observed median latency of 15ms, with tail
  latencies of 100ms+.

* A PostgreSQL RDS/Aurora instance per customer.

  Fatal flaw: provisioning a new RDS instance takes tens of minutes. Far too
  slow to support user onboarding.

* A PostgreSQL RDS/Aurora instance per cloud provider region.

  Open concern: PostgreSQL has an extremely strict limit on the maximum number
  of connections, and we'll need one connection per environment. Connection
  poolers like PgBouncer only limit this to the extent that the environments
  are not constantly issuing queries.

  Open concern: PostgreSQL is not particularly performant in `SERIALIZABLE`
  mode. Perhaps this won't impact us because we only expect transactions to
  conflict in the unusual and transient case of having two live controller
  processes.

  Open concern: a single Aurora instance has a limit of 5000 connections.

* A CockroachDB cluster per cloud provider region.

  Open concern: CockroachDB does not provide strict serializability. How does
  the anomaly that CockroachDB permits (termed a "causal reverse") impact our
  ability to provide strict serializability?

  Open concern: CockroachDB does not provide single-key linearizability in the
  case of clock skew. Empirically: clock skew happens!

* A FoundationDB cluster per cloud provider region.

  Open concern: there are no vendors that offer enterprise support contracts.

  Open soncern: there is a 10MB limit on write transactions.

## Proposal

I propose that we use a single multi-AZ Aurora PostgreSQL instance per cloud
provider region, with a separate PostgreSQL "database" per customer environment.

Aurora PostgreSQL satisfies all of the hard requirements:

* **Resilience to single availability zone outages.** Supported with multi-AZ
  deployments.

* **Strong consistency.** Supported with the `SERIALIZABLE` isolation mode.
  This is not the default; we'll need to remember

* **Single-millisecond commit latency.** TODO: verify this.

* **Easy scalability.** Mostly check, via vertical scalability. We should be
  able to buy a large enough instance to support up to 5000 customers on a
  single instance.

* **Compatibility with single binary and bring-your-own-cloud deployments.**
  In the single binary, SQLite exposes a comparable API. In BYOC deployments,
  customers will have access to hosted PostgreSQL on all major cloud platforms.

* **Environment schema isolation.** Satisifed by having separate databases
  per environment.

* **Security sandboxing.** Satisfied by having separate users per environment,
  with a user only granted access to the corresponding environment.

It satisfies many of the desirables too:

* **Consistency across clouds.** All three of the major cloud providers offer a
  hosted PostgreSQL solution.

* **Ability to run in test environments.** Check.

* **Migration plan to STORAGE collections.** Facilitated by only accessing
  PostgreSQL via the `Stash` API.

The only desirable that is not satisified is **environment performance
isolation.** A noisy environment may in theory have severe performance impacts
on other environments. We'll have to keep an eye on this.

If we discover scalability problems with Aurora PostgreSQL, we have two easy
fixes. We can run multiple Aurora PostgreSQL RDS instances in each environment,
and randomly assign customer environments to one of the instances. Or we can
migrate to CockroachDB, which is largely PostgreSQL compatible, anad has
effectively no limits on horizontal scalability.

We also have one hard fix, which is to migrate to FoundationDB. The `Stash`
API should facilitate this, if necessary.

I further propose that, for development velocity, we evaluate *only* Aurora
PostgreSQL for now. If we verify that `Stash` observes the required single-digit
millisecond commit latencies when backed by Aurora PostgreSQL in production, I
don't think we need to run PoCs with CockroachDB or FoundationDB. Aurora
PostgreSQL will scale sufficiently well for several months, if not years; that
will give us plenty of time down the road to run PoCs with CockroachDB or
FoundationDB if it becomes necessary.

## Alternatives considered

### Cross-environment reporting queries

The ability to run cross-environment reporting queries has been suggested as
a hard requirement for the metadata database. There are three known use cases
for this:

  1. Exposing a global view of the region to the ops team.
  2. Providing business intelligence via Fivetran-managed CDC to Snowflake.
  3. Powering reporting jobs like billing.

Essentially, instead of having separate databases for each environment...

```sql
CREATE DATABASE environment_1;
CREATE TABLE environment_1.clusters (id int, spec text);
CREATE TABLE environment_1.view s(id int, sql text);

CREATE DATABASE environment_2;
CREATE TABLE environment_2.clusters (id int, spec text);
CREATE TABLE environment_2.views (id int, sql text);
```

...we'd have a single database where each table has an environment ID:


```sql
CREATE DATABASE aws_us_east_1;
CREATE TABLE aws_us_east_1.clusters (environment_id int, id int, spec text);
CREATE TABLE aws_us_east_1.views (environment_id, id int, sql text);
```

This allows:

  1. The ops team to run queries like `SELECT * FROM clusters` to see all
     clusters across all customer environments.
  2. Configuration of a single Fivetran sync job to export all data in this
     database to Snowflake.
  3. A single billing job in each region to query the database to determine
     what clusters were created and deleted across all customer environments.

Unfortunately there are several downsides to this approach:

  * Schema changes would not be isolated to each environment. Every change to the
    schema would require a two step process: first the new column would need to
    be added to PostgreSQL; only *then* could environments be upgraded to the
    new `materialized`. Rollbacks are similarly difficult: it might be
    impossible to roll back an environment if a backwards-incompatible schema
    change has been deployed to the region. The logistics of these schema
    changes would be quite delicate, as the definition of the schema would
    live in the MaterializeInc/materialize repository but the application of the
    migration would be managed by MaterializeInc/cloud.

    I believe this point in particular would have deleterious effects on our
    development velocity, and also seems like the stuff of region-wide outages.

  * We'd need a separate code path to manage migrations in the binary and BYOC
    deployments.

  * Security would be tricky. We'd need to set up complicated row-based access
    control policies to limit each environment to accessing only the rows `WHERE
    environment_id = <allowed_environment_id>`.

  * If performance considerations forced us to shard the database across RDS
    instances, we'd lose the global view, but we're still left with all the
    above downsides.

I believe the downsides here outweigh the benefits. The use cases for having
a per-region metadata database can be satisified in other ways:

  1. A global view of the state of each region is already provided by
     Kubernetes. The state of each environment can still be queried directly
     from the metadata database if necessary; I believe looking at an
     environment in isolation will be the common case when debugging issues
     in production.

  2. Materialize is intending to be a best-in-class CDC tool. We can wire up
     each environment with a sink, outside of the customer's control, that
     streams the system catalog to wherever we like (Snowflake, PostgreSQL,
     another Materialize instance).

  3. To power billing, we can piggyback off the data exported in (2). (And if we
     don't trust Materialize enough to power billing, what business do we have
     selling this product to our customers?)

[`Stash`]: https://dev.materialize.com/api/rust/mz_stash/index.html
[sqlite-problems]: https://github.com/MaterializeInc/materialize/issues/11577#issuecomment-1087873791
