Self-managed Materialize uses an external PostgreSQL **metadata database** to
store its catalog and to coordinate the state of the objects it keeps up to
date. Every durable object that updates continuously (materialized views,
sources, and tables) produces a steady stream of small writes to the metadata
database. Metadata-database load therefore scales with the **number of
continuously-updating objects**, not with the volume of data flowing through
them.

{{< note >}}
The sizing figures below assume the
[`persist_pg_consensus_read_committed`](/self-managed-deployments/configuration-system-parameters/)
system parameter is **enabled**. Enable it before sizing against these
numbers.
{{< /note >}}

### Safe operating point

They key parameter that dictates the size of the metadata database is the number
of durable objects Materialize keeps continuously fresh (materialized views,
sources, and tables). Data volume, the query rate against Materialize, and
cluster size do not materially change metadata-database load. A larger cluster
running the same number of materialized views places roughly the same load on
the metadata database.

It is recommended that you size the metadata database so that its
**steady-state CPU stays below 60%**. The headroom between ~60% and full
utilization absorbs everyday load variance, background database maintenance,
and Materialize zero downtime upgrades.
