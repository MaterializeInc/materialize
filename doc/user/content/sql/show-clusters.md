---
title: "SHOW CLUSTERS"
description: "`SHOW CLUSTERS` lists the clusters configured in Materialize."
menu:
  main:
    parent: 'commands'

---

`SHOW CLUSTERS` lists the [clusters](/overview/key-concepts/#clusters) configured in Materialize. A cluster named `default` with a single [replica](/overview/key-concepts/#cluster-replicas) named `default_replica` will exist in every environment; this cluster can be dropped at any time.

## Syntax

{{< diagram "show-clusters.svg" >}}

## Examples

```sql
SHOW CLUSTERS;
```

```nofmt
       name
---------------------
 default
 auction_house
```

```sql
SHOW CLUSTERS LIKE 'auction_%';
```

```nofmt
       name
---------------------
 auction_house
```


## Related pages

- [`CREATE CLUSTER`](../create-cluster)
- [`DROP CLUSTER`](../drop-cluster)
