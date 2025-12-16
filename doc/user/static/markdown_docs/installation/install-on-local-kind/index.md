<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [Install/Upgrade
(Self-Managed)](/docs/self-managed/v25.2/installation/)

</div>

# Install locally on kind

Self-managed Materialize requires: a Kubernetes (v1.31+) cluster;
PostgreSQL as a metadata database; and blob storage.

The following tutorial uses a local [`kind`](https://kind.sigs.k8s.io/)
cluster and deploys the following components:

- Materialize Operator using Helm into your local `kind` cluster.
- MinIO object storage as the blob storage for your Materialize.
- PostgreSQL database as the metadata database for your Materialize.
- Materialize as a containerized application into your local `kind`
  cluster.

<div class="important">

**! Important:**

This tutorial is for local evaluation/testing purposes only.

- The tutorial uses sample configuration files that are for
  evaluation/testing purposes only.
- The tutorial uses a Kubernetes metrics server with TLS disabled. In
  practice, refer to your organization’s official security practices.

</div>

## Prerequisites

### kind

Install [`kind`](https://kind.sigs.k8s.io/docs/user/quick-start/).

### Docker

Install [`Docker`](https://docs.docker.com/get-started/get-docker/).

#### Docker resource requirements

For this local deployment, you will need the following Docker resource
requirements:

- 3 CPUs
- 10GB memory

### Helm 3.2.0+

If you don’t have Helm version 3.2.0+ installed, install. For details,
see the [Helm documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl`
documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

## Installation

1.  Start Docker if it is not already running.

    For this local deployment, you will need the following Docker
    resource requirements:

    - 3 CPUs
    - 10GB memory

2.  Open a Terminal window.

3.  Create a working directory and go to the directory.

    <div class="highlight">

    ``` chroma
    mkdir my-local-mz
    cd my-local-mz
    ```

    </div>

4.  Create a `kind` cluster.

    <div class="highlight">

    ``` chroma
    kind create cluster
    ```

    </div>

5.  Add labels `materialize.cloud/disk=true`,
    `materialize.cloud/swap=true` and `workload=materialize-instance` to
    the `kind` node (in this example, the `kind-control-plane` node).

    <div class="highlight">

    ``` chroma
    MYNODE=$(kubectl get nodes --no-headers | awk '{print $1}')
    kubectl label node  $MYNODE materialize.cloud/disk=true
    kubectl label node  $MYNODE materialize.cloud/swap=true
    kubectl label node  $MYNODE workload=materialize-instance
    ```

    </div>

    Verify that the labels were successfully applied by running the
    following command:

    <div class="highlight">

    ``` chroma
    kubectl get nodes --show-labels
    ```

    </div>

6.  To help you get started for local evaluation/testing, Materialize
    provides some sample configuration files. Download the sample
    configuration files from the Materialize repo:

    <div class="highlight">

    ``` chroma
    mz_operator_version=self-managed-v25.2.16
    mz_version=v0.147.20

    curl -o sample-values.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_operator_version/misc/helm-charts/operator/values.yaml
    curl -o sample-postgres.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/testing/postgres.yaml
    curl -o sample-minio.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/testing/minio.yaml
    curl -o sample-materialize.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/testing/materialize.yaml
    ```

    </div>

    - `sample-values.yaml`: Used to configure the Materialize Operator.
    - `sample-postgres.yaml`: Used to configure PostgreSQL as the
      metadata database.
    - `sample-minio.yaml`: Used to configure minIO as the blob storage.
    - `sample-materialize.yaml`: Used to configure Materialize instance.

    These configuration files are for local evaluation/testing purposes
    only and not intended for production use.

7.  Install the Materialize Helm chart.

    1.  Add the Materialize Helm chart repository.

        <div class="highlight">

        ``` chroma
        helm repo add materialize https://materializeinc.github.io/materialize
        ```

        </div>

    2.  Update the repository.

        <div class="highlight">

        ``` chroma
        helm repo update materialize
        ```

        </div>

    3.  Install the Materialize Operator. The operator will be installed
        in the `materialize` namespace.

        <div class="highlight">

        ``` chroma
        helm install my-materialize-operator materialize/materialize-operator \
            --namespace=materialize --create-namespace \
            --version v25.2.16 \
            --set observability.podMetrics.enabled=true \
            -f sample-values.yaml
        ```

        </div>

    4.  Verify the installation and check the status:

        <div class="highlight">

        ``` chroma
        kubectl get all -n materialize
        ```

        </div>

        Wait for the components to be ready and in the `Running` state:

        ```
        NAME                                           READY   STATUS    RESTARTS   AGE
        pod/my-materialize-operator-6c4c7d6fc9-hbzvr   1/1     Running   0          16s

        NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
        deployment.apps/my-materialize-operator   1/1     1            1           16s

        NAME                                                 DESIRED   CURRENT         READY   AGE
        replicaset.apps/my-materialize-operator-6c4c7d6fc9   1         1               1       16s
        ```

        If you run into an error during deployment, refer to the
        [Troubleshooting](/docs/self-managed/v25.2/installation/troubleshooting)
        guide.

8.  Install PostgreSQL and MinIO.

    1.  Use the `sample-postgres.yaml` file to install PostgreSQL as the
        metadata database:

        <div class="highlight">

        ``` chroma
        kubectl apply -f sample-postgres.yaml
        ```

        </div>

    2.  Use the `sample-minio.yaml` file to install MinIO as the blob
        storage:

        <div class="highlight">

        ``` chroma
        kubectl apply -f sample-minio.yaml
        ```

        </div>

    3.  Verify the installation and check the status:

        <div class="highlight">

        ``` chroma
        kubectl get all -n materialize
        ```

        </div>

        Wait for the components to be ready and in the `Running` state:

        ```
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

9.  Install the metrics service to the `kube-system` namespace.

    1.  Add the metrics server Helm repository.

        <div class="highlight">

        ``` chroma
        helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
        ```

        </div>

    2.  Update the repository.

        <div class="highlight">

        ``` chroma
        helm repo update metrics-server
        ```

        </div>

    3.  Install the metrics server to the `kube-system` namespace.

        <div class="important">

        **! Important:** This tutorial is for local evaluation/testing
        purposes only. For simplicity, the tutorial uses a Kubernetes
        metrics server with TLS disabled. In practice, refer to your
        organization’s official security practices.

        </div>

        <div class="highlight">

        ``` chroma
        helm install metrics-server metrics-server/metrics-server \
           --namespace kube-system \
           --set args="{--kubelet-insecure-tls,--kubelet-preferred-address-types=InternalIP\,Hostname\,ExternalIP}"
        ```

        </div>

        You can verify the installation by running the following
        command:

        <div class="highlight">

        ``` chroma
        kubectl get pods -n kube-system -l app.kubernetes.io/instance=metrics-server
        ```

        </div>

        Wait for the `metrics-server` pod to be ready and in the
        `Running` state:

        ```
        NAME                             READY   STATUS    RESTARTS   AGE
        metrics-server-89dfdc559-bq59m   1/1     Running   0          2m6s
        ```

10. Install Materialize into a new `materialize-environment` namespace:

    1.  Use the `sample-materialize.yaml` file to create the
        `materialize-environment` namespace and install Materialize:

        <div class="highlight">

        ``` chroma
        kubectl apply -f sample-materialize.yaml
        ```

        </div>

    2.  Verify the installation and check the status:

        <div class="highlight">

        ``` chroma
        kubectl get all -n materialize-environment
        ```

        </div>

        Wait for the components to be ready and in the `Running` state.

        ```
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
        [Troubleshooting](/docs/self-managed/v25.2/self-hosted/troubleshooting)
        guide.

11. Open the Materialize Console in your browser:

    1.  Find your console service name.

        <div class="highlight">

        ``` chroma
        MZ_SVC_CONSOLE=$(kubectl -n materialize-environment get svc \
          -o custom-columns="NAME:.metadata.name" --no-headers | grep console)
        echo $MZ_SVC_CONSOLE
        ```

        </div>

    2.  Port forward the Materialize Console service to your local
        machine:<sup>[^1]</sup>

        <div class="highlight">

        ``` chroma
        (
          while true; do
             kubectl port-forward svc/$MZ_SVC_CONSOLE 8080:8080 -n materialize-environment 2>&1 | tee /dev/stderr |
             grep -q "portforward.go" && echo "Restarting port forwarding due to an error." || break;
          done;
        ) &
        ```

        </div>

        The command is run in background.  
        - To list the background jobs, use `jobs`.  
        - To bring back to foreground, use `fg %<job-number>`.  
        - To kill the background job, use `kill %<job-number>`.

    3.  Open a browser and navigate to <http://localhost:8080>.

    <div class="tip">

    **💡 Tip:** If you experience long loading screens or
    unresponsiveness in the Materialize Console, we recommend increasing
    the size of the `mz_catalog_server` cluster. Refer to the
    [Troubleshooting Console
    Unresponsiveness](/docs/self-managed/v25.2/installation/troubleshooting/#troubleshooting-console-unresponsiveness)
    guide.

    </div>

## Next steps

- From the Console, you can get started with the
  [Quickstart](/docs/self-managed/v25.2/get-started/quickstart/).

- To start ingesting your own data from an external system like Kafka,
  MySQL or PostgreSQL, check the documentation for
  [sources](/docs/self-managed/v25.2/sql/create-source/).

## Clean up

To delete the whole local deployment (including Materialize instances
and data):

<div class="highlight">

``` chroma
kind delete cluster
```

</div>

## See also

- [Materialize Operator
  Configuration](/docs/self-managed/v25.2/installation/configuration/)
- [Troubleshooting](/docs/self-managed/v25.2/installation/troubleshooting/)
- [Installation](/docs/self-managed/v25.2/installation/)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/installation/install-on-local-kind/_index.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>

[^1]: The port forwarding command uses a while loop to handle a [known
    Kubernetes issue
    78446](https://github.com/kubernetes/kubernetes/issues/78446), where
    interrupted long-running requests through a standard port-forward
    cause the port forward to hang. The command automatically restarts
    the port forwarding if an error occurs, ensuring a more stable
    connection. It detects failures by monitoring for “portforward.go”
    error messages. 
