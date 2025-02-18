---
title: "Troubleshooting"
description: ""
aliases:
  - /self-hosted/troubleshooting/
menu:
  main:
    parent: "installation"
---

## Troubleshooting Kubernetes

If you encounter issues with the Materialize operator, check the operator logs:

```shell
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

To check the status of your Materialize deployment, run:

```shell
kubectl get all -n materialize
```

For additional `kubectl` commands, see [kubectl Quick reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

## Troubleshooting Console unresponsiveness

If you experience long loading screens or unresponsiveness in the Materialize Console, we recommend increasing the size of the `mz_catalog_server` cluster. The majority of the Console's queries are ran on `mz_catalog_server`, to which the default size may not be sufficient. To increase the size, you can follow the following steps:

1. Login as the `mz_system` user in order to update `mz_catalog_server`. To login as `mz_system` you'll need the internal-sql port found in the `environmentd` pod (`6877` by default). You can port forward via `kubectl port-forward svc/mzXXXXXXXXXX 6877:6877 -n materialize-environment`. 
2. Connect using a pgwire compatible client (i.e. psql) and connect using the port and user `mz_system`. For example, `psql -h localhost -p 6877 --user mz_system`. 
3. Run the following DDL to change the cluster size `ALTER CLUSTER mz_catalog_server SET (SIZE = '50cc');`
4. Verify your changes via `SHOW CLUSTERS;` 
```shell
mz_system=> show clusters;
       name        | replicas  | comment 
-------------------+-----------+---------
 mz_analytics      |           | 
 mz_catalog_server | r1 (50cc) | 
 mz_probe          |           | 
 mz_support        |           | 
 mz_system         |           | 
 quickstart        | r1 (25cc) | 
(6 rows)
```

## See also

- [Configuration](/installation/configuration/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Installation](/installation/)
