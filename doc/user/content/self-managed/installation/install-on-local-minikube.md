---
title: "Install locally on minikube"
description: ""
---

The following tutorial deploys Materialize onto a local
[`minikube`](https://minikube.sigs.k8s.io/docs/start/) cluster. The tutorial
deploys the following components onto your local `minikube` cluster:

- Materialize Operator using Helm into your local `minikube` cluster.
- MinIO object storage service as the blob storage.
- PostgreSQL database as the  metadata database.

{{< important >}}

For testing purposes only. For testing purposes only.  For testing purposes only. ....

{{< /important >}}

## Prerequisites

### minikube

Install [`minikube`](https://minikube.sigs.k8s.io/docs/start/).

### Container or virtual machine manager

The following tutorial uses `Docker` as the container or virtual machine
manager.

Install [`Docker`](https://docs.docker.com/get-started/get-docker/).

To use another container or virtual machine manager as listed on the
[`minikube` documentation](https://minikube.sigs.k8s.io/docs/start/), refer to
the specific container/VM manager documentation.

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

1. Create a minikube cluster.

   ```shell
   minikube start
   ```

1. Create the `materialize` namespace.

   ```shell
   kubectl create namespace materialize
   ```

1. Install the Materialize Helm chart using the files provided in the
   Materialize repo.

   1. Go to the Materialize repo directory.

   1. Optional. Edit the `misc/helm-charts/operator/values.yaml` to update (in
      the `operator.args` section) the `region` value to `minikube`:

      ```yaml
          region: "minikube"
      ```

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
      NAME                                          READY   STATUS    RESTARTS   AGE
      pod/my-materialize-operator-8fc75cd7d-vcvl8   1/1     Running   0          8s

      NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
      deployment.apps/my-materialize-operator   1/1     1            1           8s

      NAME                                                DESIRED   CURRENT   READY   AGE
      replicaset.apps/my-materialize-operator-8fc75cd7d   1         1         1       8s
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
       NAME                                             READY   STATUS    RESTARTS   AGE
       pod/mz2f4guf58oj-balancerd-69b5486554-xdpzq      1/1     Running   0          4m38s
       pod/mz2f4guf58oj-cluster-s1-replica-s1-gen-1-0   1/1     Running   0          4m43s
       pod/mz2f4guf58oj-cluster-s2-replica-s2-gen-1-0   1/1     Running   0          4m43s
       pod/mz2f4guf58oj-cluster-s3-replica-s3-gen-1-0   1/1     Running   0          4m43s
       pod/mz2f4guf58oj-cluster-u1-replica-u1-gen-1-0   1/1     Running   0          4m43s
       pod/mz2f4guf58oj-console-557fdb88db-gb7zn        1/1     Running   0          4m38s
       pod/mz2f4guf58oj-console-557fdb88db-xfv8w        1/1     Running   0          4m38s
       pod/mz2f4guf58oj-environmentd-1-0                1/1     Running   0          4m55s

       NAME                                               TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                                                                                     AGE
       service/mz2f4guf58oj-balancerd                     NodePort    10.102.116.95    <none>        6876:32161/TCP,6875:31896/TCP                                                               4m38s
       service/mz2f4guf58oj-cluster-s1-replica-s1-gen-1   ClusterIP   10.105.24.34     <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP                                                4m43s
       service/mz2f4guf58oj-cluster-s2-replica-s2-gen-1   ClusterIP   10.97.165.188    <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP                                                4m43s
       service/mz2f4guf58oj-cluster-s3-replica-s3-gen-1   ClusterIP   10.107.119.66    <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP                                                4m43s
       service/mz2f4guf58oj-cluster-u1-replica-u1-gen-1   ClusterIP   10.103.70.133    <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP                                                4m43s
       service/mz2f4guf58oj-console                       NodePort    10.111.141.122   <none>        9000:30793/TCP                                                                              4m38s
       service/mz2f4guf58oj-environmentd                  NodePort    10.100.113.144   <none>        6875:30588/TCP,6876:31828/TCP,6877:31859/TCP,6878:30579/TCP,6880:31895/TCP,6881:30263/TCP   4m38s
       service/mz2f4guf58oj-environmentd-1                NodePort    10.96.37.132     <none>        6875:32689/TCP,6876:30816/TCP,6877:32014/TCP,6878:30266/TCP,6880:32366/TCP,6881:31536/TCP   4m55s
       service/mz2f4guf58oj-persist-pubsub-1              ClusterIP   None             <none>        6879/TCP                                                                                    4m55s

       NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
       deployment.apps/mz2f4guf58oj-balancerd   1/1     1            1           4m38s
       deployment.apps/mz2f4guf58oj-console     2/2     2            2           4m38s

       NAME                                                DESIRED   CURRENT   READY   AGE
       replicaset.apps/mz2f4guf58oj-balancerd-69b5486554   1         1         1       4m38s
       replicaset.apps/mz2f4guf58oj-console-557fdb88db     2         2         2       4m38s

       NAME                                                        READY   AGE
       statefulset.apps/mz2f4guf58oj-cluster-s1-replica-s1-gen-1   1/1     4m43s
       statefulset.apps/mz2f4guf58oj-cluster-s2-replica-s2-gen-1   1/1     4m43s
       statefulset.apps/mz2f4guf58oj-cluster-s3-replica-s3-gen-1   1/1     4m43s
       statefulset.apps/mz2f4guf58oj-cluster-u1-replica-u1-gen-1   1/1     4m43s
       statefulset.apps/mz2f4guf58oj-environmentd-1                1/1     4m55s
       ```

       If you run into an error during deployment, refer to the
       [Troubleshooting](/self-hosted/troubleshooting) guide.

1. Open the Materialize console in your browser:

   1. From the `kubectl` output, find the Materialize console service.

      ```shell
      NAME                            TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)
      service/mz2f4guf58oj-console    NodePort    10.111.141.122  <none>        9000:30793/TCP
      ```

   1. Forward the Materialize console service to your local machine:

      ```shell
      kubectl port-forward service/mzfhj38ptdjs-console 9000:9000 -n materialize-environment
      ```

   1. Open a browser and navigate to
      [http://localhost:9000](http://localhost:9000).

      ![Image of self-managed Materialize console running on local minikube](/images/self-managed/self-managed-console-minkiube.png)


## See also

- [Materialize Kubernetes Operator Helm Chart](/self-hosted/)
- [Materialize Operator Configuration](/self-hosted/configuration/)
- [Troubleshooting](/self-managed/troubleshooting/)
- [Operational guidelines](/self-managed/operational-guidelines/)
- [Installation](/self-managed/installation/)
- [Upgrading](/self-managed/upgrading/)
