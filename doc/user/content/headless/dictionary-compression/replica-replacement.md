---
headless: true
---
{{< warning >}}
Changing `EXPERIMENTAL ARRANGEMENT COMPRESSION` on a cluster, whether you turn
it on or off, replaces the cluster's replicas, so the cluster re-hydrates. The
new replicas have to rebuild their arrangements, and the existing replicas keep
serving until the new ones are ready, so the cluster temporarily uses roughly
twice its usual memory until the switch completes. Plan for this the same way
you would plan for resizing a cluster. Hydration is slower with compression
enabled, so turning the option on takes longer than turning it off.
{{< /warning >}}
