---
title: "Install locally on minikube"
description: ""
aliases:
  - /self-hosted/install-on-local-minikube/
menu:
  main:
    parent: "installation"
---

The following tutorial deploys Materialize onto a local
[`minikube`](https://minikube.sigs.k8s.io/docs/start/) cluster. Self-managed Materialize requires:

{{% self-managed/materialize-components-list %}}

The tutorial deploys the following components onto your local `minikube`
cluster:

- Materialize Operator using Helm into your local `minikube` cluster.
- MinIO object storage as the blob storage for your Materialize.
- PostgreSQL database as the metadata database for your Materialize.
- Materialize as a containerized application into your local `minikube` cluster.

{{< important >}}

For testing purposes only.

{{< /important >}}

## Resource requirements

{{% self-managed/local-resource-requirements %}}

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

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl` documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### Sample configuration files

Download the following sample configuration files from the Materialize repo:

- `values.yaml`
- `postgres.yaml`
- `minio.yaml`
- `materialize.yaml`

```shell
curl -o values.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/lts-v0.130/misc/helm-charts/operator/values.yaml
curl -o sample-postgres.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/lts-v0.130/misc/helm-charts/testing/postgres.yaml
curl -o sample-minio.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/lts-v0.130/misc/helm-charts/testing/minio.yaml
curl -o sample-materialize.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/lts-v0.130/misc/helm-charts/testing/materialize.yaml
```

## Installation

1. Start Docker if it is not already running.

1. Open a Terminal window.

1. Create a minikube cluster.

   ```shell
   minikube start
   ```

1. Install the Materialize Helm chart.

   1. Add the Materialize Helm chart repository.

      ```shell
      helm repo add materialize https://materializeinc.github.io/materialize
      helm repo update materialize
      ```

   1. Install the Materialize Operator.

      ```shell
      helm install my-materialize-operator materialize/materialize-operator \
          --namespace=materialize --create-namespace \
          --version v25.1.0 \
          --set operator.cloudProvider.region=minikube \
          -f values.yaml
      ```

   1. Verify the installation and check the status:

      ```shell
      kubectl get all -n materialize
      ```

      Wait for the components to be in the `Running` state:

      ```none
      NAME                                           READY   STATUS    RESTARTS   AGE
      pod/my-materialize-operator-669d674ccf-h5842   1/1     Running   0          12m

      NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
      deployment.apps/my-materialize-operator   1/1     1            1           12m

      NAME                                                 DESIRED   CURRENT         READY   AGE
      replicaset.apps/my-materialize-operator-669d674ccf   1         1               1       12m
      ```

      If you run into an error during deployment, refer to the
      [Troubleshooting](/installation/troubleshooting) guide.

1. Install PostgreSQL and minIO.

    1. Use the `sample-postgres.yaml` file to install PostgreSQL as the
       metadata database:

        ```shell
        kubectl apply -f sample-postgres.yaml
        ```

    1. Use the `sample-minio.yaml` file to install minIO as the blob storage:

        ```shell
        kubectl apply -f sample-minio.yaml
        ```

1. Optional. Install the following metrics service for certain system metrics
   but not required.

   ```shell
   kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
   ```

1. Install Materialize into a new `materialize-environment` namespace:

   1. Use the `sample-materialize.yaml` file to create the
      `materialize-environment` namespace and install Materialize:

      ```shell
      kubectl apply -f sample-materialize.yaml
      ```

    1. Verify the installation and check the status:

       ```shell
       kubectl get all -n materialize-environment
       ```

       Wait for the components to be in the `Running` state.


       ```none
       NAME                                             READY   STATUS      RESTARTS   AGE
       pod/mzk7x050omzi-balancerd-5b99dff555-7qltf      1/1     Running     0          19s
       pod/mzk7x050omzi-cluster-s1-replica-s1-gen-1-0   1/1     Running     0          22s
       pod/mzk7x050omzi-cluster-s2-replica-s2-gen-1-0   1/1     Running     0          22s
       pod/mzk7x050omzi-cluster-s3-replica-s3-gen-1-0   1/1     Running     0          22s
       pod/mzk7x050omzi-cluster-u1-replica-u1-gen-1-0   1/1     Running     0          22s
       pod/mzk7x050omzi-console-8c45c4b95-8ng29         1/1     Running     0          12s
       pod/mzk7x050omzi-console-8c45c4b95-jwj6c         1/1     Running     0          12s
       pod/mzk7x050omzi-environmentd-1-0                1/1     Running     0          36s

       NAME                                               TYPE          CLUSTER-IP   EXTERNAL-IP   PORT(S)                                          AGE
       service/mzk7x050omzi-balancerd                     ClusterIP     None         <none>        6876/TCP,6875/TCP                                19s
       service/mzk7x050omzi-cluster-s1-replica-s1-gen-1   ClusterIP     None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP     22s
       service/mzk7x050omzi-cluster-s2-replica-s2-gen-1   ClusterIP     None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP     22s
       service/mzk7x050omzi-cluster-s3-replica-s3-gen-1   ClusterIP     None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP     22s
       service/mzk7x050omzi-cluster-u1-replica-u1-gen-1   ClusterIP     None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP     22s
       service/mzk7x050omzi-console                       ClusterIP     None         <none>        8080/TCP                                         12s
       service/mzk7x050omzi-environmentd                  ClusterIP     None         <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP              19s
       service/mzk7x050omzi-environmentd-1                ClusterIP     None         <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP              36s
       service/mzk7x050omzi-persist-pubsub-1              ClusterIP     None         <none>        6879/TCP                                         36s

       NAME                                     READY   UP-TO-DATE   AVAILABLE     AGE
       deployment.apps/mzk7x050omzi-balancerd   1/1     1            1             19s
       deployment.apps/mzk7x050omzi-console     2/2     2            2             12s

       NAME                                                DESIRED   CURRENT     READY   AGE
       replicaset.apps/mzk7x050omzi-balancerd-5b99dff555   1         1           1       19s
       replicaset.apps/mzk7x050omzi-console-8c45c4b95      2         2           2       12s

       NAME                                                        READY   AGE
       statefulset.apps/mzk7x050omzi-cluster-s1-replica-s1-gen-1   1/1     22s
       statefulset.apps/mzk7x050omzi-cluster-s2-replica-s2-gen-1   1/1     22s
       statefulset.apps/mzk7x050omzi-cluster-s3-replica-s3-gen-1   1/1     22s
       statefulset.apps/mzk7x050omzi-cluster-u1-replica-u1-gen-1   1/1     22s
       statefulset.apps/mzk7x050omzi-environmentd-1                1/1     36s
       ```

       If you run into an error during deployment, refer to the
       [Troubleshooting](/self-hosted/troubleshooting) guide.

1. Open the Materialize console in your browser:

   1. From the previous `kubectl` output, find the Materialize console service.

      ```none
      NAME                            TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)       AGE
      service/mzk7x050omzi-console    ClusterIP   None           <none>        8080/TCP      12s
      ```

   1. Forward the Materialize console service to your local machine:

      ```shell
      while true;
      do kubectl port-forward svc/mzlvmx9h6dpx-console 8080:8080 -n materialize-environment 2>&1 |
      grep -q "portforward.go" && echo "Restarting port forwarding due to an error." || break;
      done;
      ```
      {{< note >}}
      Due to a [known Kubernetes
      issue](https://github.com/kubernetes/kubernetes/issues/78446), interrupted
      long-running requests through a standard port-forward cause the port
      forward to hang. The command above automatically restarts the port
      forwarding if an error occurs, ensuring a more stable connection. It
      detects failures by monitoring for `"portforward.go"` error messages.
      {{< /note >}}

   1. Open a browser to
      [http://localhost:8080](http://localhost:8080).

      ![Image of self-managed Materialize console running on local minikube](/images/self-managed/self-managed-console-minkiube.png)


## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Installation](/installation/)
- [Upgrading](/installation/upgrading/)
