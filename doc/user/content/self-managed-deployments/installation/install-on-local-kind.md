---
title: "Install locally on kind"
description: "Deploy Self-managed Materialize to a local kind cluster."
aliases:
  - /self-hosted/install-on-local-kind/
  - /installation/install-on-local-minikube/
  - /self-managed/v25.1/installation/install-on-local-kind/
  - /self-managed/v25.1/installation/install-on-local-minikube/
  - /installation/install-on-local-kind/
menu:
  main:
    parent: "installation"
    identifier: "install-on-local-kind"
    weight: 10
---

{{% self-managed/materialize-components-sentence %}}

The following tutorial uses a local [`kind`](https://kind.sigs.k8s.io/) cluster
and deploys the following components:

- Materialize Operator using Helm into your local `kind` cluster.
- MinIO object storage as the blob storage for your Materialize.
- PostgreSQL database as the metadata database for your Materialize.
- Materialize as a containerized application into your local `kind` cluster.

{{< important >}}

This tutorial is for local evaluation/testing purposes only.

- The tutorial uses sample configuration files that are for evaluation/testing
  purposes only.
- The tutorial uses a Kubernetes metrics server with TLS disabled. In practice,
  refer to your organization's official security practices.

{{< /important >}}

## Prerequisites

### kind

Install [`kind`](https://kind.sigs.k8s.io/docs/user/quick-start/).

### Docker

Install [`Docker`](https://docs.docker.com/get-started/get-docker/).

#### Docker resource requirements

{{% self-managed/local-resource-requirements %}}

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, install. For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl`
documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### License key

Starting in v26.0, Self-Managed Materialize requires a license key.

{{< yaml-table data="self_managed/license_key" >}}

## Installation

1. Start Docker if it is not already running.

   {{% self-managed/local-resource-requirements %}}

1. Open a Terminal window.

1. Create a working directory and go to the directory.

   ```shell
   mkdir my-local-mz
   cd my-local-mz
   ```

1. Create a `kind` cluster.

   ```shell
   kind create cluster
   ```

1. Add labels `materialize.cloud/disk=true`, `materialize.cloud/swap=true` and
   `workload=materialize-instance` to the `kind` node (in this example, the
   `kind-control-plane` node).

   ```shell
   MYNODE=$(kubectl get nodes --no-headers | awk '{print $1}')
   kubectl label node  $MYNODE materialize.cloud/disk=true
   kubectl label node  $MYNODE materialize.cloud/swap=true
   kubectl label node  $MYNODE workload=materialize-instance
   ```

   Verify that the labels were successfully applied by running the following
   command:

   ```shell
   kubectl get nodes --show-labels
   ```

1. To help you get started for local evaluation/testing, Materialize provides
   some sample configuration files. Download the sample configuration files from
   the Materialize repo:

   {{% self-managed/versions/curl-sample-files-local-install %}}

1. Add your license key:

   a. To get your license key:

      {{% yaml-table data="self_managed/license_key" %}}

   b. Edit `sample-materialize.yaml` to add your license key to the
   `license_key` field in the backend secret.

   ```yaml {hl_lines="10"}
   ---
   apiVersion: v1
   kind: Secret
   metadata:
   name: materialize-backend
   namespace: materialize-environment
   stringData:
   metadata_backend_url: "postgres://materialize_user:materialize_pass@postgres.materialize.svc.cluster.local:5432/materialize_db?sslmode=disable"
   persist_backend_url: "s3://minio:minio123@bucket/12345678-1234-1234-1234-123456789012?endpoint=http%3A%2F%2Fminio.materialize.svc.cluster.local%3A9000&region=minio"
   license_key: "<enter your license key here>"
   ---
   ```

1. Install the Materialize Helm chart.

   1. Add the Materialize Helm chart repository.

      ```shell
      helm repo add materialize https://materializeinc.github.io/materialize
      ```

   1. Update the repository.

      ```shell
      helm repo update materialize
      ```

   {{% self-managed/versions/step-install-helm-version-local-kind-install %}}

   1. Verify the installation and check the status:

      ```shell
      kubectl get all -n materialize
      ```

      Wait for the components to be ready and in the `Running` state:

      ```none
      NAME                                           READY   STATUS    RESTARTS   AGE
      pod/my-materialize-operator-6c4c7d6fc9-hbzvr   1/1     Running   0          16s

      NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
      deployment.apps/my-materialize-operator   1/1     1            1           16s

      NAME                                                 DESIRED   CURRENT         READY   AGE
      replicaset.apps/my-materialize-operator-6c4c7d6fc9   1         1               1       16s
      ```

      If you run into an error during deployment, refer to the
      [Troubleshooting](/installation/troubleshooting) guide.

1. Install PostgreSQL and MinIO.

    1. Use the `sample-postgres.yaml` file to install PostgreSQL as the
       metadata database:

        ```shell
        kubectl apply -f sample-postgres.yaml
        ```

    1. Use the `sample-minio.yaml` file to install MinIO as the blob storage:

        ```shell
        kubectl apply -f sample-minio.yaml
        ```

    1. Verify the installation and check the status:

       ```shell
       kubectl get all -n materialize
       ```

       Wait for the components to be ready and in the `Running` state:

       ```none
       NAME                                           READY   STATUS     RESTARTS   AGE
       pod/minio-777db75dd4-zcl89                     1/1     Running    0          84s
       pod/my-materialize-operator-6c4c7d6fc9-hbzvr   1/1     Running    0          107s
       pod/postgres-55fbcd88bf-b4kdv                  1/1     Running    0          86s

       NAME               TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)    AGE
       service/minio      ClusterIP   10.96.51.9     <none>        9000/TCP   84s
       service/postgres   ClusterIP   10.96.19.166   <none>        5432/TCP   86s

       NAME                                      READY   UP-TO-DATE    AVAILABLE   AGE
       deployment.apps/minio                     1/1     1             1           84s
       deployment.apps/my-materialize-operator   1/1     1             1           107s
       deployment.apps/postgres                  1/1     1             1           86s

       NAME                                                 DESIRED    CURRENT         READY   AGE
       replicaset.apps/minio-777db75dd4                     1          1               1       84s
       replicaset.apps/my-materialize-operator-6c4c7d6fc9   1          1               1       107s
       replicaset.apps/postgres-55fbcd88bf                  1          1               1       86s
       ```

1. Install the metrics service to the `kube-system` namespace.

   1. Add the metrics server Helm repository.

      ```shell
      helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
      ```

   1. Update the repository.

      ```shell
      helm repo update metrics-server
      ```

   1. Install the metrics server to the `kube-system` namespace.

      {{< important >}}

      This tutorial is for local evaluation/testing purposes only. For simplicity,
      the tutorial uses a Kubernetes metrics server with TLS disabled. In practice,
      refer to your organization's official security practices.

      {{< /important >}}

      ```shell
      helm install metrics-server metrics-server/metrics-server \
         --namespace kube-system \
         --set args="{--kubelet-insecure-tls,--kubelet-preferred-address-types=InternalIP\,Hostname\,ExternalIP}"
      ```

      You can verify the installation by running the following command:

      ```bash
      kubectl get pods -n kube-system -l app.kubernetes.io/instance=metrics-server
      ```

      Wait for the `metrics-server` pod to be ready and in the `Running` state:

      ```none
      NAME                             READY   STATUS    RESTARTS   AGE
      metrics-server-89dfdc559-bq59m   1/1     Running   0          2m6s
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

       Wait for the components to be ready and in the `Running` state.

       ```none
       NAME                                             READY   STATUS    RESTARTS   AGE
       pod/mz32bsnzerqo-balancerd-756b65959c-6q9db      1/1     Running   0                 12s
       pod/mz32bsnzerqo-cluster-s2-replica-s1-gen-1-0   1/1     Running   0                 14s
       pod/mz32bsnzerqo-cluster-u1-replica-u1-gen-1-0   1/1     Running   0                 14s
       pod/mz32bsnzerqo-console-6b7c975fb9-jkm8l        1/1     Running   0          5s
       pod/mz32bsnzerqo-console-6b7c975fb9-z8g8f        1/1     Running   0          5s
       pod/mz32bsnzerqo-environmentd-1-0                1/1     Running   0                 19s

       NAME                                               TYPE        CLUSTER-IP          EXTERNAL-IP   PORT(S)                                        AGE
       service/mz32bsnzerqo-balancerd                     ClusterIP   None                <none>        6876/TCP,6875/TCP                              12s
       service/mz32bsnzerqo-cluster-s2-replica-s1-gen-1   ClusterIP   None                <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   14s
       service/mz32bsnzerqo-cluster-u1-replica-u1-gen-1   ClusterIP   None                <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   14s
       service/mz32bsnzerqo-console                       ClusterIP   None                <none>        8080/TCP                                       5s
       service/mz32bsnzerqo-environmentd                  ClusterIP   None                <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            12s
       service/mz32bsnzerqo-environmentd-1                ClusterIP   None                <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            19s
       service/mz32bsnzerqo-persist-pubsub-1              ClusterIP   None                <none>        6879/TCP                                       19s

       NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
       deployment.apps/mz32bsnzerqo-balancerd   1/1     1            1           12s
       deployment.apps/mz32bsnzerqo-console     2/2     2            2           5s

       NAME                                                DESIRED   CURRENT   READY          AGE
       replicaset.apps/mz32bsnzerqo-balancerd-756b65959c   1         1         1              12s
       replicaset.apps/mz32bsnzerqo-console-6b7c975fb9     2         2         2              5s

       NAME                                                        READY   AGE
       statefulset.apps/mz32bsnzerqo-cluster-s2-replica-s1-gen-1   1/1     14s
       statefulset.apps/mz32bsnzerqo-cluster-u1-replica-u1-gen-1   1/1     14s
       statefulset.apps/mz32bsnzerqo-environmentd-1                1/1     19s
       ```

       If you run into an error during deployment, refer to the
       [Troubleshooting](/self-hosted/troubleshooting) guide.

1. Open the Materialize Console in your browser:

   {{% self-managed/port-forwarding-handling-local %}}

      {{< tip >}}

      {{% self-managed/troubleshoot-console-mz_catalog_server_blurb %}}

      {{< /tip >}}

## Next steps

{{% self-managed/next-steps %}}

## Clean up

To delete the whole local deployment (including Materialize instances and data):

```bash
kind delete cluster
```

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Installation](/installation/)
