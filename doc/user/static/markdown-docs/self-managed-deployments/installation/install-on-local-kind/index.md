# Install locally on kind

Deploy Self-managed Materialize to a local kind cluster.



Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; and a license key.


The following tutorial uses a local [`kind`](https://kind.sigs.k8s.io/) cluster
and deploys the following components:

- Materialize Operator using Helm into your local `kind` cluster.
- MinIO object storage as the blob storage for your Materialize.
- PostgreSQL database as the metadata database for your Materialize.
- Materialize as a containerized application into your local `kind` cluster.

> **Important:** This tutorial is for local evaluation/testing purposes only.
>
> - The tutorial uses sample configuration files that are for evaluation/testing
>   purposes only.
> - The tutorial uses a Kubernetes metrics server with TLS disabled. In practice,
>   refer to your organization's official security practices.
>
>


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

If you don't have Helm version 3.2.0+ installed, install. For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl`
documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### License key

Starting in v26.0, Self-Managed Materialize requires a license key.


| License key type | Deployment type | Action |
| --- | --- | --- |
| Community | New deployments | <p>To get a license key:</p> <ul> <li>If you have a Cloud account, visit the <a href="https://console.materialize.com/license/" ><strong>License</strong> page in the Materialize Console</a>.</li> <li>If you do not have a Cloud account, visit <a href="https://materialize.com/self-managed/community-license/" >https://materialize.com/self-managed/community-license/</a>.</li> </ul> |
| Community | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |
| Enterprise | New deployments | Visit <a href="https://materialize.com/self-managed/enterprise-license/" >https://materialize.com/self-managed/enterprise-license/</a> to purchase an Enterprise license. |
| Enterprise | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |


## Installation

1. Start Docker if it is not already running.

   For this local deployment, you will need the following Docker resource
   requirements:

   - 3 CPUs
   - 10GB memory


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



   ```shell
   mz_version=v26.8.0

   curl -o sample-values.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/operator/values.yaml
   curl -o sample-postgres.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/testing/postgres.yaml
   curl -o sample-minio.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/testing/minio.yaml
   curl -o sample-materialize.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/testing/materialize.yaml
   ```

   - `sample-values.yaml`: Used to configure the Materialize Operator.
   - `sample-postgres.yaml`: Used to configure PostgreSQL as the metadata
     database.
   - `sample-minio.yaml`: Used to configure minIO as the blob storage.
   - `sample-materialize.yaml`: Used to configure Materialize instance.

   These configuration files are for local evaluation/testing purposes only and
   not intended for production use.


1. Add your license key:

   a. To get your license key:


      | License key type | Deployment type | Action |
      | --- | --- | --- |
      | Community | New deployments | <p>To get a license key:</p> <ul> <li>If you have a Cloud account, visit the <a href="https://console.materialize.com/license/" ><strong>License</strong> page in the Materialize Console</a>.</li> <li>If you do not have a Cloud account, visit <a href="https://materialize.com/self-managed/community-license/" >https://materialize.com/self-managed/community-license/</a>.</li> </ul> |
      | Community | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |
      | Enterprise | New deployments | Visit <a href="https://materialize.com/self-managed/enterprise-license/" >https://materialize.com/self-managed/enterprise-license/</a> to purchase an Enterprise license. |
      | Enterprise | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |


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



   1. Install the Materialize Operator. The operator will be installed in the
      `materialize` namespace.

      ```shell
      helm install my-materialize-operator materialize/materialize-operator \
          --namespace=materialize --create-namespace \
          --version v26.8.0 \
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

      > **Important:** This tutorial is for local evaluation/testing purposes only. For simplicity,
>       the tutorial uses a Kubernetes metrics server with TLS disabled. In practice,
>       refer to your organization's official security practices.
>
>


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


   1. Find your console service name.

      ```shell
      MZ_SVC_CONSOLE=$(kubectl -n materialize-environment get svc \
        -o custom-columns="NAME:.metadata.name" --no-headers | grep console)
      echo $MZ_SVC_CONSOLE
      ```

   1. Port forward the Materialize Console service to your local machine:[^1]

      ```shell
      (
        while true; do
           kubectl port-forward svc/$MZ_SVC_CONSOLE 8080:8080 -n materialize-environment 2>&1 | tee /dev/stderr |
           grep -q "portforward.go" && echo "Restarting port forwarding due to an error." || break;
        done;
      ) &
      ```

      The command is run in background.
      <br>- To list the background jobs, use `jobs`.
      <br>- To bring back to foreground, use `fg %<job-number>`.
      <br>- To kill the background job, use `kill %<job-number>`.

   1. Open a browser and navigate to
      [http://localhost:8080](http://localhost:8080).

   [^1]: The port forwarding command uses a while loop to handle a [known
   Kubernetes issue 78446](https://github.com/kubernetes/kubernetes/issues/78446),
   where interrupted long-running requests through a standard port-forward cause
   the port forward to hang. The command automatically restarts the port forwarding
   if an error occurs, ensuring a more stable connection. It detects failures by
   monitoring for "portforward.go" error messages.


      > **Tip:** If you experience long loading screens or unresponsiveness in the Materialize
>       Console, we recommend increasing the size of the `mz_catalog_server` cluster.
>       Refer to the [Troubleshooting Console
>       Unresponsiveness](/self-managed-deployments/troubleshooting/#troubleshooting-console-unresponsiveness)
>       guide.
>
>
>


## Next steps


- From the Console, you can get started with the
[Quickstart](/get-started/quickstart/).

- To start ingesting your own data from an external system like Kafka, MySQL or
  PostgreSQL, see [Ingest data](/ingest-data/).


## Clean up

To delete the whole local deployment (including Materialize instances and data):

```bash
kind delete cluster
```

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Installation](/installation/)
