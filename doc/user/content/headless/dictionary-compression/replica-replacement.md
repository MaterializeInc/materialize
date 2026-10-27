---
headless: true
---
{{< warning >}}
A replica's compression setting is fixed when the replica is created, so
changing `EXPERIMENTAL ARRANGEMENT COMPRESSION` never changes an existing
replica's arrangements. Materialize instead creates a new set of replicas
carrying the new setting. The existing replicas keep serving until the new ones
have hydrated, then Materialize retires them. As a result, the cluster
temporarily uses roughly twice its usual memory until the switch completes,
regardless of whether you enable or disable dictionary compression.

Nothing else is needed to apply the setting. Plan for the switch the same way
you would plan for resizing a cluster. Because hydration is slower with
compression enabled, the switch takes longer when turning compression on than
when turning it off.
{{< /warning >}}
