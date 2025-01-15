---
title: "Troubleshooting"
description: ""
aliases:
  - /self-hosted/troubleshooting/
  - /self-managed/troubleshooting/
suppress_breadcrumb: true
---

If you encounter issues with the Materialize operator, check the operator logs:

```shell
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

To check the status of your Materialize deployment, run:

```shell
kubectl get all -n materialize
```

For additional `kubectl` commands, see [kubectl Quick reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

## See also

- [Materialize Kubernetes Operator Helm Chart](/self-managed/)
- [Configuration](/self-managed/configuration/)
- [Operational guidelines](/self-managed/operational-guidelines/)
- [Installation](/self-managed/installation/)
- [Upgrading](/self-managed/upgrading/)
