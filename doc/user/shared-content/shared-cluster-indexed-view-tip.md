{{< tip >}}
Except for when used with a [sink](/serve-results/sink/),
[subscribe](/sql/subscribe/), or [temporal
filters](/transform-data/patterns/temporal-filters/), avoid creating
materialized views on a shared cluster used for both compute/transformat
operations and serving queries. Use indexed views instead.
{{</ tip >}}
