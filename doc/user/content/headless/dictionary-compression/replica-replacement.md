---
headless: true
---
{{< warning >}}
Changing `EXPERIMENTAL ARRANGEMENT COMPRESSION` on a cluster replaces the
cluster's replicas. While the replacement replicas hydrate, the existing
replicas keep serving until the new ones are ready. As a result, the cluster
temporarily uses roughly twice its usual memory until the switch completes,
regardless of whether you enable or disable dictionary compression.

Plan for this the same way you would plan for resizing a cluster. Because
hydration is slower with compression enabled, the replacement takes longer when
turning compression on than when turning it off.
{{< /warning >}}
