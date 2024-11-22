---
title: "Install locally on kind"
description: ""
aliases:
  - /self-hosted/install-on-local-kind/
---

The following tutorial deploys Materialize onto a local
[`kind`](https://kind.sigs.k8s.io/) cluster. The tutorial deploys the following
components onto your local `kind` cluster:

- Materialize Operator using Helm into your local `kind` cluster.
- MinIO object storage service as the blob storage.
- PostgreSQL database as the  metadata database.


{{< important >}}

For testing purposes only. For testing purposes only.  For testing purposes only. ....

{{< /important >}}

## Prerequisites

### kind

Install [`kind`](https://kind.sigs.k8s.io/docs/user/quick-start/).

### Docker

Install [`Docker`](https://docs.docker.com/get-started/get-docker/).

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, refer to the [Helm
documentation](https://helm.sh/docs/intro/install/).

### Kubernetes

Materialize supports [Kubernetes 1.19+](https://kubernetes.io/docs/setup/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl` documentationq](https://kubernetes.io/docs/tasks/tools/).

### Materialize repo

The following instructions assume that you are installing from the [Materialize
repo](https://github.com/MaterializeInc/materialize).

{{< important >}}

{{% self-managed/git-checkout-branch %}}

{{< /important >}}

## Installation

1. Start Docker if it is not already running.

1. Open a Terminal window.

1. Create a kind cluster.

   ```shell
   kind create cluster
   ```

1. Create the `materialize` namespace.

   ```shell
   kubectl create namespace materialize
   ```

1. Install the Materialize Helm chart using the files provided in the
   Materialize repo.

   1. Go to the Materialize repo directory.

   1. Install the Materialize operator with the release name
      `my-materialize-operator`:

      ```shell
      helm install my-materialize-operator -f misc/helm-charts/operator/values.yaml misc/helm-charts/operator
      ```

   1. Verify the installation and check the status:

      ```shell
      kubectl get all -n materialize
      ```

      Wait for the components to be in the `Running` state:

      ```shell
      NAME                                           READY   STATUS              RESTARTS   AGE
      pod/my-materialize-operator-776b98455b-w9kkl   0/1     ContainerCreating   0          6s

      NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
      deployment.apps/my-materialize-operator   0/1     1            0           6s

      NAME                                                 DESIRED   CURRENT   READY   AGE
      replicaset.apps/my-materialize-operator-776b98455b   1         1         0       6s
      ```

      If you run into an error during deployment, refer to the
      [Troubleshooting](/self-hosted/troubleshooting) guide.

1. Install PostgreSQL and minIO.

    1. Go to the Materialize repo directory.

    1. Use the provided `postgres.yaml` file to install PostgreSQL as the
       metadata database:

        ```shell
        kubectl apply -f misc/helm-charts/testing/postgres.yaml
        ```

    1. Use the provided `minio.yaml` file to install minIO as the blob storage:

        ```shell
        kubectl apply -f misc/helm-charts/testing/minio.yaml
        ```

1. Optional. Install the following metrics service for certain system metrics
   but not required.

   ```shell
   kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
   ```

1. Install Materialize into a new `materialize-environment` namespace:

   1. Go to the Materialize repo directory.

   1. Use the provided `environmentd.yaml` file to create the
      `materialize-environment` namespace and install Materialize:

      ```shell
      kubectl apply -f misc/helm-charts/testing/environmentd.yaml
      ```

    1. Verify the installation and check the status:

       ```shell
       kubectl get all -n materialize-environment
       ```

       Wait for the components to be in the `Running` state.

       ```shell
       NAME                                             READY   STATUS     RESTARTS   AGE
       pod/mzfhj38ptdjs-balancerd-6dd5bb645d-p7r2j      1/1     Running    0          3m21s
       pod/mzfhj38ptdjs-cluster-s1-replica-s1-gen-1-0   1/1     Running    0          3m25s
       pod/mzfhj38ptdjs-cluster-s2-replica-s2-gen-1-0   1/1     Running    0          3m25s
       pod/mzfhj38ptdjs-cluster-s3-replica-s3-gen-1-0   1/1     Running    0          3m25s
       pod/mzfhj38ptdjs-cluster-u1-replica-u1-gen-1-0   1/1     Running    0          3m25s
       pod/mzfhj38ptdjs-console-84cb5c98d6-9zlc4        1/1     Running    0          3m21s
       pod/mzfhj38ptdjs-console-84cb5c98d6-rjjcs        1/1     Running    0          3m21s
       pod/mzfhj38ptdjs-environmentd-1-0                1/1     Running    0          3m29s

       NAME                                               TYPE         CLUSTER-IP      EXTERNAL-IP   PORT (S)                                                                                      AGE
       service/mzfhj38ptdjs-balancerd                     NodePort    10.96.60. 152    <none>        6876:32386/TCP,6875:31334/ TCP                                                               3m21s
       service/mzfhj38ptdjs-cluster-s1-replica-s1-gen-1   ClusterIP   10.96.162. 190   <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/ TCP                                                3m25s
       service/mzfhj38ptdjs-cluster-s2-replica-s2-gen-1   ClusterIP   10.96.120. 116   <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/ TCP                                                3m25s
       service/mzfhj38ptdjs-cluster-s3-replica-s3-gen-1   ClusterIP   10.96.187. 199   <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/ TCP                                                3m25s
       service/mzfhj38ptdjs-cluster-u1-replica-u1-gen-1   ClusterIP   10.96.92. 133    <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/ TCP                                                3m25s
       service/mzfhj38ptdjs-console                       NodePort    10.96.97. 5      <none>        9000:30847/ TCP                                                                               3m21s
       service/mzfhj38ptdjs-environmentd                  NodePort    10.96.188. 140   <none>        6875:30525/TCP,6876:31052/TCP,6877:31711/TCP, 6878:31367/TCP,6880:30141/TCP,6881:30283/TCP   3m21s
       service/mzfhj38ptdjs-environmentd-1                NodePort    10.96.228. 68    <none>        6875:32253/TCP,6876:31876/TCP,6877:31886/TCP, 6878:31643/TCP,6880:32409/TCP,6881:30932/TCP   3m29s
       service/mzfhj38ptdjs-persist-pubsub-1              ClusterIP    None            <none>        6879/ TCP                                                                                     3m29s

       NAME                                     READY   UP-TO-DATE   AVAILABLE    AGE
       deployment.apps/mzfhj38ptdjs-balancerd   1/1     1            1            3m21s
       deployment.apps/mzfhj38ptdjs-console     2/2     2            2            3m21s

       NAME                                                DESIRED   CURRENT    READY   AGE
       replicaset.apps/mzfhj38ptdjs-balancerd-6dd5bb645d   1         1          1       3m21s
       replicaset.apps/mzfhj38ptdjs-console-84cb5c98d6     2         2          2       3m21s

       NAME                                                      READY   AGE
       statefulset.apps/mzfhj38ptdjs-cluster-s1-replica-s1-gen-1   1/1     3m25s
       statefulset.apps/mzfhj38ptdjs-cluster-s2-replica-s2-gen-1   1/1     3m25s
       statefulset.apps/mzfhj38ptdjs-cluster-s3-replica-s3-gen-1   1/1     3m25s
       statefulset.apps/mzfhj38ptdjs-cluster-u1-replica-u1-gen-1   1/1     3m25s
       statefulset.apps/mzfhj38ptdjs-environmentd-1                1/1     3m29s
       ```

       If you run into an error during deployment, refer to the
       [Troubleshooting](/self-hosted/troubleshooting) guide.

1. Open the Materialize console in your browser:

   1. From the `kubectl` output, find the Materialize console service.

      ```shell
      NAME                           TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)
      service/mzfhj38ptdjs-console   NodePort    10.96.97.5      <none>        9000:30847/TCP
      ```

   1. Forward the Materialize console service to your local machine:

      ```shell
      kubectl port-forward svc/mzfhj38ptdjs-console 9000:9000 -n materialize-environment
      ```

   1. Open a browser and navigate to
      [http://localhost:9000](http://localhost:9000).

      ![Image of  self-managed Materialize console running on local kind](/images/self-managed/self-managed-console-kind.png)

## See also

- [Materialize Kubernetes Operator Helm Chart](/self-hosted/)
- [Materialize Operator Configuration](/self-hosted/configuration/)
- [Troubleshooting](/self-managed/troubleshooting/)
- [Operational guidelines](/self-managed/operational-guidelines/)
- [Installation](/self-managed/installation/)
- [Upgrading](/self-managed/upgrading/)
