---
headless: true
---
The Materialize spec itself is unchanged (the node move happens at the
Kubernetes cluster level and not in the Materialize CR), so you need to force
the rollout.

{{< tabs >}}
{{< tab "Materialize CRD v1" >}}

The `v1` version of the Materialize CRD is the default starting in v4.0.0 of
the Terraform modules. Set the `force_rollout` input of the
`materialize-instance` module to a new UUID:

```hcl
module "materialize_instance" {
  # ...
  rollout_strategy = "WaitUntilReady"  # default
  force_rollout    = "00000000-0000-0000-0000-000000000002"  # any new UUID
}
```

{{< /tab >}}
{{< tab "Materialize CRD v1alpha1" >}}

If you have reverted to the `v1alpha1` version of the Materialize CRD, also
set `request_rollout` to the same UUID:

```hcl
module "materialize_instance" {
  # ...
  rollout_strategy = "WaitUntilReady"  # default
  request_rollout  = "00000000-0000-0000-0000-000000000002"  # any new UUID
  force_rollout    = "00000000-0000-0000-0000-000000000002"  # same UUID
}
```

{{< /tab >}}
{{< /tabs >}}
