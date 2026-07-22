---
title: "v1alpha1"
description: "Reference page on Materialize CRD Fields for the v1alpha1 API (before v26.30)"
menu:
  main:
    parent: "materialize-crd-field-descriptions"
    weight: 20
---

{{< note >}}
`v1alpha1` is the default CRD version for the Helm chart. The Terraform
modules default to `v1` starting in v4.0.0. With v1alpha1, rollouts require
manually rotating a UUID. Starting in v26.30, the
[v1](/self-managed-deployments/materialize-crd-field-descriptions/v1/) CRD is
available and provides a simplified rollout behavior.

To switch to `v1`, see [Adopting the v1
CRD](/self-managed-deployments/upgrading/adopting-the-v1-crd/).
{{</ note >}}

{{% self-managed/materialize-crd-descriptions-v1alpha1 %}}
