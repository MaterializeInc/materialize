---
title: "Troubleshooting"
description: ""
aliases:
  - /self-hosted/troubleshooting/
menu:
  main:
    parent: "installation"
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

- [Configuration](/installation/configuration/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Installation](/installation/)
- [Upgrading](/installation/upgrading/)
