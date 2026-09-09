# mz-demo-data

Continuously-updating, realistic synthetic data for Materialize demos —
generated entirely in SQL, no external load generator.

Based on [this blog post](https://github.com/frankmcsherry/blog/blob/master/posts/2024-05-19.md).

## Quickstart

```sh
PSQL="psql -p 6875 -h localhost -U materialize"

$PSQL -f assets/scaffold.sql            # moments + random (24h window, 1s tick)
$PSQL -f assets/common/people.sql       # shared 256-identity pool
$PSQL -f assets/domains/auctions.sql    # or any other domain
```

Then in a psql session:

```sql
COPY (SUBSCRIBE (SELECT COUNT(*) FROM auctions) WITH (progress = true)) TO STDOUT;
```

To change the retention window or tick:

```sql
\set retention '6 hours'
\set tick '1 second'
\i assets/scaffold.sql
```

Teardown: `$PSQL -f assets/teardown.sql`.

## Domains

* **auctions** — marketplace; auctions with lifecycle and bids
* **ecommerce** — orders, line items, totals (joins shared `people`)
* **banking** — double-entry transactions; `SUM(balances) = 0` invariant
* **iot** — devices, readings, threshold alerts
* **clickstream** — sessions, page views, conversion funnel
* **zoo** — zoo visits, ratings, shipments; four invariants at once

Load more than one to see Materialize keep multiple data products in sync
over the same shared identity space.

## Designing a new domain

See [`SKILL.md`](SKILL.md). The short version: derive PKs from `moment`'s
random bytes, derive FKs by re-hashing the parent's bytes, control
distributions with byte masks, and bake an invariant into the construction.
