---
title: "Install locally on minikube"
description: "Deploy Self-managed Materialize to a local minikube cluster."
aliases:
  - /self-hosted/install-on-local-minikube/
menu:
  main:
    parent: "installation"
    identifier: "install-on-local-minikube"
    weight: 15
disable_list: true
---

{{% self-managed/materialize-components-sentence %}}

The following tutorial deploys the following components onto your local
[`minikube`](https://minikube.sigs.k8s.io/docs/start/):

- Materialize Operator using Helm into your local `minikube` cluster.
- MinIO object storage as the blob storage for your Materialize.
- PostgreSQL database as the metadata database for your Materialize.
- Materialize as a containerized application into your local `minikube` cluster.

{{< important >}}

This tutorial is for local evaluation/testing purposes only.

- The tutorial uses sample configuration files that are for evaluation/testing
  purposes only.
- The tutorial uses a Kubernetes metrics server with TLS disabled. In practice,
  refer to your organization's official security practices.

{{< /important >}}

## Prerequisites

### minikube

Install [`minikube`](https://minikube.sigs.k8s.io/docs/start/).

### Container or virtual machine manager

The following tutorial uses `Docker` as the container or virtual machine
manager. To use another container or virtual machine manager as listed on the
[`minikube` documentation](https://minikube.sigs.k8s.io/docs/start/), refer to
the specific container/VM manager documentation.

Install [`Docker`](https://docs.docker.com/get-started/get-docker/).

#### Resource requirements for your container/virtual machine manager

{{% self-managed/local-resource-requirements %}}

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, install. For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl` documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

## Installation

1. Start Docker if it is not already running.

   {{% self-managed/local-resource-requirements %}}

1. Open a Terminal window.

1. Create a working directory and go to the directory.

   ```shell
   mkdir my-local-mz
   cd my-local-mz
   ```

1. Create a minikube cluster.

   ```shell
   minikube start
   ```

1. Add labels `materialize.cloud/disk=true`, `materialize.cloud/swap=true` and
   `workload=materialize-instance` to the `minikube` node (in this example,
   named `minikube`).

   ```shell
   MYNODE=$(kubectl get nodes --no-headers | awk '{print $1}')
   kubectl label node  $MYNODE materialize.cloud/disk=true
   kubectl label node  $MYNODE materialize.cloud/swap=true
   kubectl label node  $MYNODE workload=materialize-instance
   ```

   Verify that the labels were successfully applied by running the following command:

   ```shell
   kubectl get nodes --show-labels
   ```

1. To help you get started for local evaluation/testing, Materialize provides
   some sample configuration files. Download the sample configuration files from
   the Materialize repo:

   {{% self-managed/versions/curl-sample-files-local-install %}}

1. Install the Materialize Helm chart.

   1. Add the Materialize Helm chart repository.

      ```shell
      helm repo add materialize https://materializeinc.github.io/materialize
      ```

   1. Update the repository.

      ```shell
      helm repo update materialize
      ```

   {{% self-managed/versions/step-install-helm-version-local-minikube-install %}}

   1. Verify the installation and check the status:

      ```shell
      kubectl get all -n materialize
      ```

      Wait for the components to be ready and in the `Running` state:

      ```none
      NAME                                           READY   STATUS    RESTARTS   AGE
      pod/my-materialize-operator-7c75785df9-6cn88   1/1     Running   0          9s

      NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
      deployment.apps/my-materialize-operator   1/1     1            1           9s

      NAME                                                 DESIRED   CURRENT         READY   AGE
      replicaset.apps/my-materialize-operator-7c75785df9   1         1               1       9s
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
       NAME                                           READY   STATUS    RESTARTS   AGE
       pod/minio-777db75dd4-7cd77                     1/1     Running   0          30s
       pod/my-materialize-operator-7c75785df9-6cn88   1/1     Running   0          88s
       pod/postgres-55fbcd88bf-zkwvz                  1/1     Running   0          34s

       NAME               TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)    AGE
       service/minio      ClusterIP   10.98.114.67    <none>        9000/TCP   30s
       service/postgres   ClusterIP   10.98.144.251   <none>        5432/TCP   34s

       NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
       deployment.apps/minio                     1/1     1            1           30s
       deployment.apps/my-materialize-operator   1/1     1            1           88s
       deployment.apps/postgres                  1/1     1            1           34s

       NAME                                                 DESIRED   CURRENT          READY   AGE
       replicaset.apps/minio-777db75dd4                     1         1                1       30s
       replicaset.apps/my-materialize-operator-7c75785df9   1         1                1       88s
       replicaset.apps/postgres-55fbcd88bf                  1         1                1       34s
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
      metrics-server-89dfdc559-jt94n   1/1     Running   0          14m
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
       NAME                                             READY   STATUS    RESTARTS          AGE
       pod/mz10wiu7fyr7-balancerd-fddc4bd7c-lwqmp       1/1     Running   0                 19s
       pod/mz10wiu7fyr7-cluster-s2-replica-s1-gen-1-0   1/1     Running   0                 26s
       pod/mz10wiu7fyr7-cluster-u1-replica-u1-gen-1-0   1/1     Running   0                 25s
       pod/mz10wiu7fyr7-console-6cbcd997dc-95tf2        1/1     Running   0                 12s
       pod/mz10wiu7fyr7-console-6cbcd997dc-bbm5h        1/1     Running   0                 12s
       pod/mz10wiu7fyr7-environmentd-1-0                1/1     Running   0                 32s

       NAME                                               TYPE        CLUSTER-IP          EXTERNAL-IP   PORT(S)                                        AGE
       service/mz10wiu7fyr7-balancerd                     ClusterIP   None                <none>        6876/TCP,6875/TCP                              19s
       service/mz10wiu7fyr7-cluster-s2-replica-s1-gen-1   ClusterIP   None                <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   26s
       service/mz10wiu7fyr7-cluster-u1-replica-u1-gen-1   ClusterIP   None                <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   26s
       service/mz10wiu7fyr7-console                       ClusterIP   None                <none>        8080/TCP                                       12s
       service/mz10wiu7fyr7-environmentd                  ClusterIP   None                <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            19s
       service/mz10wiu7fyr7-environmentd-1                ClusterIP   None                <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            32s
       service/mz10wiu7fyr7-persist-pubsub-1              ClusterIP   None                <none>        6879/TCP                                       32s

       NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
       deployment.apps/mz10wiu7fyr7-balancerd   1/1     1            1           19s
       deployment.apps/mz10wiu7fyr7-console     2/2     2            2           12s

       NAME                                               DESIRED   CURRENT   READY          AGE
       replicaset.apps/mz10wiu7fyr7-balancerd-fddc4bd7c   1         1         1              19s
       replicaset.apps/mz10wiu7fyr7-console-6cbcd997dc    2         2         2              12s

       NAME                                                        READY   AGE
       statefulset.apps/mz10wiu7fyr7-cluster-s2-replica-s1-gen-1   1/1     26s
       statefulset.apps/mz10wiu7fyr7-cluster-u1-replica-u1-gen-1   1/1     26s
       statefulset.apps/mz10wiu7fyr7-environmentd-1                1/1     32s
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
minikube delete
```

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Installation](/installation/)
