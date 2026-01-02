---
audience: developer
canonical_url: https://materialize.com/docs/installation/troubleshooting/
complexity: advanced
description: 'To check the status of the Materialize operator:'
doc_type: troubleshooting
keywords:
- Troubleshooting
- SHOW CLUSTERS
- ALTER CLUSTER
product_area: Deployment
status: stable
title: Troubleshooting
---

# Troubleshooting

## Purpose


This page provides detailed documentation for this topic.




## Troubleshooting Kubernetes

This section covers troubleshooting kubernetes.

### Materialize operator

To check the status of the Materialize operator:

```shell
kubectl -n materialize get all
```text

If you encounter issues with the Materialize operator,

- Check the operator logs, using the label selector:

  ```shell
  kubectl -n materialize logs -l app.kubernetes.io/name=materialize-operator
  ```text

- Check the log of a specific object (pod/deployment/etc) running in
  your namespace:

  ```shell
  kubectl -n materialize logs <type>/<name>
  ```text

  In case of a container restart, to get the logs for previous instance, include
  the `--previous` flag.

- Check the events for the operator pod:

  - You can use `kubectl describe`, substituting your pod name for `<pod-name>`:

    ```shell
    kubectl -n materialize describe pod/<pod-name>
    ```text

  - You can use `kubectl get events`, substituting your pod name for
    `<pod-name>`:

    ```shell
    kubectl -n materialize get events --sort-by=.metadata.creationTimestamp --field-selector involvedObject.name=<pod-name>
    ```bash

### Materialize deployment

- To check the status of your Materialize deployment, run:

  ```shell
  kubectl  -n materialize-environment get all
  ```text

- To check the log of a specific object (pod/deployment/etc) running in your
  namespace:

  ```shell
  kubectl -n materialize-environment logs <type>/<name>
  ```text

  In case of a container restart, to get the logs for previous instance, include
  the `--previous` flag.

- To describe an object, you can use `kubectl describe`:

  ```shell
  kubectl -n materialize-environment describe <type>/<name>
  ```text

For additional `kubectl` commands, see [kubectl Quick reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

## Troubleshooting Console unresponsiveness

If you experience long loading screens or unresponsiveness in the Materialize
Console, it may be that the size of the `mz_catalog_server` cluster (where the
majority of the Console's queries are run) is insufficient (default size is
`25cc`).

To increase the cluster's size, you can follow the following steps:

1. Login as the `mz_system` user in order to update `mz_catalog_server`.

   1. To login as `mz_system` you'll need the internal-sql port found in the
      `environmentd` pod (`6877` by default). You can port forward via `kubectl
      port-forward svc/mzXXXXXXXXXX 6877:6877 -n materialize-environment`.

   1. Connect using a pgwire compatible client (e.g., `psql`) and connect using
      the port and user `mz_system`. For example:

       ```
       psql -h localhost -p 6877 --user mz_system
       ```text

3. Run the following [ALTER CLUSTER](/sql/alter-cluster/#resizing) statement
   to change the cluster size to `50cc`:

    ```mzsql
    ALTER CLUSTER mz_catalog_server SET (SIZE = '50cc');
    ```text

4. Verify your changes via `SHOW CLUSTERS;`

   ```mzsql
   show clusters;
   ```text

   The output should include the `mz_catalog_server` cluster with a size of `50cc`:

   ```none
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
- [Installation](/installation/)

