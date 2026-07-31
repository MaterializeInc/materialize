---
headless: true
---

Self-managed Materialize uses an external PostgreSQL **metadata database** to
store its catalog and to coordinate the state of the objects it keeps up to
date. Every durable object that updates continuously (materialized views,
sources, sinks, and tables) produces a steady stream of small writes to the
metadata database. Metadata-database load therefore scales with the **number of
continuously-updating objects**, not with the volume of data flowing through
them.

{{< note >}}
The sizing figures below assume the
[`persist_pg_consensus_read_committed`](/self-managed-deployments/configuration-system-parameters/)
system parameter is **enabled**. Enable it before sizing against these
numbers. Materialize version `v26.33+` is required to set this parameter.
{{< /note >}}

### Safe operating point

The primary factor that dictates the size of the metadata database is the
number of durable objects Materialize keeps continuously fresh (materialized
views, sources, sinks, and tables). Data volume, the query rate against
Materialize, and cluster size do not materially change metadata database load.
For example, a larger cluster running the same number of materialized views
places roughly the same load on the metadata database.

It is recommended that you size the metadata database so that its
**steady-state CPU stays below 60%**. The headroom between ~60% and full
utilization provides capacity to absorb everyday load variance, background
database maintenance, and Materialize zero-downtime upgrades.
