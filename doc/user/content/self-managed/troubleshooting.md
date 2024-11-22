---
title: "Troubleshooting"
description: ""
aliases:
  - /self-hosted/troubleshooting/
robots: "noindex, nofollow"
---

If you encounter issues with the Materialize operator, check the operator logs:

```shell
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

To check the status of your Materialize deployment, run:

```shell
kubectl get all -n materialize
```

## See also

- [Materialize Kubernetes Operator Helm Chart](/self-managed/)
- [Configuration](/self-managed/configuration/)
- [Operational guidelines](/self-managed/operational-guidelines/)
- [Installation](/self-managed/installation/)
- [Upgrading](/self-managed/upgrading/)
