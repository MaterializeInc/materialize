
For example:

{{< tabs >}}
{{< tab "3-tier architecture" >}}

![Image of the 3-tier-architecture
architecture](/images/3-tier-architecture.svg)

In a [3-tier
architecture](/manage/operational-guidelines/#three-tier-architecture)
where queries are served from a cluster different from the compute/transform
cluster that maintains the view results:

- Use materialized view(s) in the compute/transform cluster for the query
  results that will be served.

  {{< include-md file="shared-content/stacked-view-consideration.md" >}}

- Index the materialized view in the serving cluster(s) to serve the results
from memory.

{{</ tab >}}

{{< tab "2-tier architecture" >}}

![Image of the 2-tier-architecture](/images/2-tier-architecture.svg)

In a [2-tier
architecture](/manage/appendix-alternative-cluster-architectures/#two-tier-architecture)
where queries are served from the same cluster that performs the
compute/transform operations:

- Use view(s) in the shared cluster.

- Index the view(s) to incrementally update the view results and serve the
results from memory.

{{< include-md file="shared-content/shared-cluster-indexed-view-tip.md" >}}

{{</ tab >}}
{{< tab "1-tier architecture" >}}

![Image of the 1-tier-architecture](/images/1-tier-architecture.svg)

In a [1-tier
architecture](/manage/appendix-alternative-cluster-architectures/#one-tier-architecture)
where queries are served from the same cluster that performs the
compute/transform operations:

- Use view(s) in the shared cluster.

- Index the view(s) to incrementally update the view results and serve the
results from memory.

{{< include-md file="shared-content/shared-cluster-indexed-view-tip.md" >}}

{{</ tab >}}
{{</ tabs >}}
