---
title: "Install locally on kind"
description: ""
aliases:
  - /self-hosted/install-on-local-kind/
menu:
  main:
    parent: "installation"
---

Self-managed Materialize requires:

{{% self-managed/materialize-components-list %}}

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

#### Resource requirements
{{% self-managed/local-resource-requirements %}}

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, refer to the [Helm
documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl`
documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

## Installation

1. Start Docker if it is not already running.

1. Open a Terminal window.

1. Create a kind cluster.

   ```shell
   kind create cluster
   ```

1. Add labels `materialize.cloud/disk=true` and
   `workload=materialize-instance` to the node.

   ```shell
   kubectl get nodes --show-labels
   ```

   Add the labels to the node, substituting `<node-name>` with the name of the
   node (e.g., `kind-control-plane`).

   ```shell
   kubectl label node <node-name> materialize.cloud/disk=true
   kubectl label node <node-name> workload=materialize-instance
   ```

   Verify that the labels were successfully applied by running the following command again:

   ```shell
   kubectl get nodes --show-labels
   ```

1. To help you get started for local evaluation/testing, Materialize provides
   some sample configuration files. Download the sample configuration files from
   the Materialize repo:

   ```shell
   curl -o sample-values.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/lts-v0.130/misc/helm-charts/operator/values.yaml
   curl -o sample-postgres.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/lts-v0.130/misc/helm-charts/testing/postgres.yaml
   curl -o sample-minio.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/lts-v0.130/misc/helm-charts/testing/minio.yaml
   curl -o sample-materialize.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/lts-v0.130/misc/helm-charts/testing/materialize.yaml
   ```

   - `sample-values.yaml`: Used to configure the Materialize Operator.
   - `sample-postgres.yaml`: Used to configure PostgreSQL as the metadata
     database.
   - `sample-minio.yaml`: Used to configure minIO as the blob storage.
   - `sample-materialize.yaml`: Used to configure Materialize instance.

   These configuration files are for local evaluation/testing purposes only and
   not intended for production use.

1. Install the Materialize Helm chart.

   1. Add the Materialize Helm chart repository.

      ```shell
      helm repo add materialize https://materializeinc.github.io/materialize
      ```

   1. Update the repository.

      ```shell
      helm repo update materialize
      ```

   1. Install the Materialize Operator. The operator will be installed in the
      `materialize` namespace.

      ```shell
      helm install my-materialize-operator materialize/materialize-operator \
          --namespace=materialize --create-namespace \
          --version v25.1.1 \
          --set observability.podMetrics.enabled=true \
          -f sample-values.yaml
      ```

   1. Verify the installation and check the status:

      ```shell
      kubectl get all -n materialize
      ```

      Wait for the components to be ready and in the `Running` state:

      ```none
      NAME                                           READY   STATUS    RESTARTS   AGE
      pod/my-materialize-operator-776b98455b-w9kkl   1/1     Running   0          6s

      NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
      deployment.apps/my-materialize-operator   1/1     1            1           6s

      NAME                                                 DESIRED   CURRENT   READY   AGE
      replicaset.apps/my-materialize-operator-776b98455b   1         1         1       6s
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
      metrics-server-89dfdc559-tgvtg   1/1     Running   0          14m
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
       pod/mzlvmx9h6dpx-balancerd-f5c689b95-kjtzf       1/1     Running   0          45s
       pod/mzlvmx9h6dpx-cluster-s1-replica-s1-gen-1-0   1/1     Running   0          51s
       pod/mzlvmx9h6dpx-cluster-s2-replica-s2-gen-1-0   1/1     Running   0          51s
       pod/mzlvmx9h6dpx-cluster-s3-replica-s3-gen-1-0   1/1     Running   0          51s
       pod/mzlvmx9h6dpx-cluster-u1-replica-u1-gen-1-0   1/1     Running   0          51s
       pod/mzlvmx9h6dpx-console-6b746b7d57-p24n4        1/1     Running   0          32s
       pod/mzlvmx9h6dpx-console-6b746b7d57-qjs4p        1/1     Running   0          32s
       pod/mzlvmx9h6dpx-environmentd-1-0                1/1     Running   0          60s

       NAME                                               TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                                        AGE
       service/mzlvmx9h6dpx-balancerd                     ClusterIP   None         <none>        6876/TCP,6875 TCP                              45s
       service/mzlvmx9h6dpx-cluster-s1-replica-s1-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878 TCP   51s
       service/mzlvmx9h6dpx-cluster-s2-replica-s2-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878 TCP   51s
       service/mzlvmx9h6dpx-cluster-s3-replica-s3-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878 TCP   51s
       service/mzlvmx9h6dpx-cluster-u1-replica-u1-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878 TCP   51s
       service/mzlvmx9h6dpx-console                       ClusterIP   None         <none>        8080 TCP                                       32s
       service/mzlvmx9h6dpx-environmentd                  ClusterIP   None         <none>        6875/TCP,6876/TCP,6877/TCP,6878 TCP            45s
       service/mzlvmx9h6dpx-environmentd-1                ClusterIP   None         <none>        6875/TCP,6876/TCP,6877/TCP,6878 TCP            60s
       service/mzlvmx9h6dpx-persist-pubsub-1              ClusterIP   None         <none>        6879 TCP                                       60s

       NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
       deployment.apps/mzlvmx9h6dpx-balancerd   1/1     1            1           45s
       deployment.apps/mzlvmx9h6dpx-console     2/2     2            2           32s

       NAME                                               DESIRED   CURRENT   READY   AGE
       replicaset.apps/mzlvmx9h6dpx-balancerd-f5c689b95   1         1         1       45s
       replicaset.apps/mzlvmx9h6dpx-console-6b746b7d57    2         2         2       32s

       NAME                                                        READY   AGE
       statefulset.apps/mzlvmx9h6dpx-cluster-s1-replica-s1-gen-1   1/1     51s
       statefulset.apps/mzlvmx9h6dpx-cluster-s2-replica-s2-gen-1   1/1     51s
       statefulset.apps/mzlvmx9h6dpx-cluster-s3-replica-s3-gen-1   1/1     51s
       statefulset.apps/mzlvmx9h6dpx-cluster-u1-replica-u1-gen-1   1/1     51s
       statefulset.apps/mzlvmx9h6dpx-environmentd-1                1/1     60s
       ```

       If you run into an error during deployment, refer to the
       [Troubleshooting](/self-hosted/troubleshooting) guide.

1. Open the Materialize Console in your browser:

   1. From the previous `kubectl` output, find the Materialize Console service.

      ```none
      NAME                           TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
      service/mzlvmx9h6dpx-console   ClusterIP   None         <none>        8080 TCP   32s
      ```

   1. Forward the Materialize Console service to your local machine (substitute
      your service name for `mzlvmx9h6dpx-console`):

      ```shell
      while true;
      do kubectl port-forward svc/mzlvmx9h6dpx-console 8080:8080 -n materialize-environment 2>&1 |
      grep -q "portforward.go" && echo "Restarting port forwarding due to an error." || break;
      done;
      ```

      {{< annotation type="Kubernetes issue 78446" >}}

      Due to a [known Kubernetes
      issue](https://github.com/kubernetes/kubernetes/issues/78446), interrupted
      long-running requests through a standard port-forward cause the port
      forward to hang, and the Console will display **"We're having trouble
      reaching your environment."** error message. The command above
      automatically restarts the port forwarding if an error occurs, ensuring a
      more stable connection. It detects failures by monitoring for
      "portforward.go" error messages. You can refresh the Console page to
      reconnect.

      {{< /annotation>}}

   1. Open a browser to
      [http://localhost:8080](http://localhost:8080).

      ![Image of  self-managed Materialize Console running on local kind](/images/self-managed/self-managed-console-kind.png)


   {{< tip >}}

   {{% self-managed/troubleshoot-console-mz_catalog_server_blurb %}}

   {{< /tip >}}

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Installation](/installation/)
