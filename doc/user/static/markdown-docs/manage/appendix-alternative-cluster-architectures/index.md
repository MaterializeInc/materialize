# Appendix: Alternative cluster architectures
If the recommended 3-tier architecture is infeasible, can use a 2-cluster or a 1-cluster pattern.
If the [recommended three-tier
architecture](/manage/operational-guidelines/#three-tier-architecture)
is infeasible or unnecessary due to low volume or a **non**-production setup, a
two-tier or a one-tier architecture may suffice.

## Two-tier architecture

<p>If the <a href="/manage/operational-guidelines/#three-tier-architecture" >recommended three-tier
architecture</a>
is infeasible or unnecessary due to low volume or a <strong>non</strong>-production setup, a
two-tier architecture may suffice. A two-tier architecture consists of:</p>
<p><img src="/images/2-tier-architecture.svg" alt="Image of the 2-cluster architecture: Source cluster, Compute/Transform &#43;
Serving cluster" ></p>

| Tier | Description |
| --- | --- |
| <strong>Source cluster(s)</strong> | <p><strong>A dedicated cluster(s)</strong> for <a href="/concepts/sources/" >sources</a>.</p> <p>In addition, for upsert sources:</p> <ul> <li> <p>Consider separating upsert sources from your other sources. Upsert sources have higher resource requirements (since, for upsert sources, Materialize maintains each key and associated last value for the key as well as to perform deduplication). As such, if possible, use a separate source cluster for upsert sources.</p> </li> <li> <p>Consider using a larger cluster size during snapshotting for upsert sources. Once the snapshotting operation is complete, you can downsize the cluster to align with the steady-state ingestion.</p> </li> </ul>  |
| <strong>Compute/Transform + Serving cluster</strong> | <p><strong>A dedicated cluster</strong> for both compute/transformation and serving queries:</p> <ul> <li> <p><a href="/concepts/views/#views" >Views</a> that define the transformations.</p> </li> <li> <p>Indexes on views to maintain up-to-date results in memory and serve queries.</p> </li> </ul> <p>With a two-tier architecture, compute and queries compete for the same cluster resources.</p> > **Tip:** Except for when used with a [sink](/serve-results/sink/), > [subscribe](/sql/subscribe/), or [temporal > filters](/transform-data/patterns/temporal-filters/), avoid creating > materialized views on a shared cluster used for both compute/transformat > operations and serving queries. Use indexed views instead.    |

<p>Benefits of a two-tier architecture include:</p>
<ul>
<li>
<p>Support for <a href="/manage/dbt/blue-green-deployments/" >blue/green
deployments</a></p>
</li>
<li>
<p>More cost effective than a three-tier architecture.</p>
</li>
</ul>
<p>However, with a two-tier architecture:</p>
<ul>
<li>
<p>Compute/transform operations and queries compete for the same cluster
resources.</p>
</li>
<li>
<p>Cluster restarts require rehydration of the indexes on views.</p>
</li>
</ul>


## One-tier architecture

<p>If the <a href="/manage/operational-guidelines/#three-tier-architecture" >recommended three-tier
architecture</a>
is infeasible or unnecessary due to low volume or a <strong>non</strong>-production setup, a
one-tier architecture may suffice for your sources, compute objects,
and query serving needs.</p>
<p><img src="/images/1-tier-architecture.svg" alt="Image of the 1-cluster-architecture
architecture" ></p>

| Tier | Description |
| --- | --- |
| <strong>All-in-one cluster</strong> | <p><strong>A cluster</strong> for <a href="/concepts/sources/" >sources</a>, compute/transformation and serving queries:</p> <ul> <li> <p>Sources to ingest data.</p> </li> <li> <p>Views that define the transformations.</p> </li> <li> <p>Indexes on views to maintain up-to-date results in memory and serve queries.</p> </li> </ul> <p>With a 1-tier single-cluster architecture, sources, compute, and queries compete for the same cluster resources.</p> > **Tip:** Except for when used with a [sink](/serve-results/sink/), > [subscribe](/sql/subscribe/), or [temporal > filters](/transform-data/patterns/temporal-filters/), avoid creating > materialized views on a shared cluster used for both compute/transformat > operations and serving queries. Use indexed views instead.    |

<p><strong>Benefits of a one-tier architecture</strong> include:</p>
<ul>
<li>Cost effective</li>
</ul>
<p><strong>Limitations of a one-tier architecture</strong> include:</p>
<ul>
<li>
<p>Sources, compute objects, and queries compete for cluster resources.</p>
</li>
<li>
<p><a href="/manage/dbt/blue-green-deployments/" >Blue/green
deployment</a> is
unsupported since sources would need to be dropped and recreated, putting strain on your upstream system during source recreation.</p>
<p>To support blue/green deployments, use a two-tier architecture by moving
compute objects to a new cluster (i.e., recreating compute objects in a new cluster).</p>
</li>
<li>
<p>Cluster restarts require rehydration of the indexes on views.</p>
</li>
</ul>
