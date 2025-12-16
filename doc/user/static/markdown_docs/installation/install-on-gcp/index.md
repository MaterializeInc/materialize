<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [Install/Upgrade
(Self-Managed)](/docs/self-managed/v25.2/installation/)

</div>

# Install on GCP

Self-managed Materialize requires: a Kubernetes (v1.31+) cluster;
PostgreSQL as a metadata database; and blob storage.

This tutorial deploys Materialize to GCP Google Kubernetes Engine (GKE)
cluster with a Cloud SQL PostgreSQL database as the metadata database
and Cloud Storage bucket for blob storage. Specifically, the tutorial
uses [Materialize on Google Cloud Provider Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize)
to:

- Set up the GCP environment.

- Call
  [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
  module to deploy Materialize Operator and Materialize instances to the
  GKE cluster.

<div class="warning">

**WARNING!**

The Terraform modules used in this tutorial are intended for
evaluation/demonstration purposes and for serving as a template when
building your own production deployment. The modules should not be
directly relied upon for production deployments: **future releases of
the modules will contain breaking changes.** Instead, to use as a
starting point for your own production deployment, either:

- Fork the repo and pin to a specific version; or

- Use the code as a reference when developing your own deployment.

For simplicity, this tutorial stores various secrets in a file as well
as prints them to the terminal. In practice, refer to your
organization’s official security and Terraform/infrastructure practices.

</div>

## Prerequisites

### Google cloud provider project

You need a GCP project for which you have a role (such as
`roles/resourcemanager.projectIamAdmin` or `roles/owner`) that includes
[permissions to manage access to the
project](https://cloud.google.com/iam/docs/granting-changing-revoking-access).

### gcloud CLI

If you do not have gcloud CLI, install. For details, see the [Install
the gcloud CLI
documentation](https://cloud.google.com/sdk/docs/install).

### Google service account

The tutorial assumes the use of a service account. If you do not have a
service account to use for this tutorial, create a service account. For
details, see [Create service
accounts](https://cloud.google.com/iam/docs/service-accounts-create#creating).

### Terraform

If you do not have Terraform installed, [install
Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### kubectl and plugins

<div class="tip">

**💡 Tip:** Using `gcloud` to install `kubectl` will also install the
needed plugins. Otherwise, you will need to manually install the
`gke-gcloud-auth-plugin` for `kubectl`.

</div>

- If you do not have `kubectl`, install `kubectl`. To install, see
  [Install kubectl and configure cluster
  access](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl)
  for details. You will configure `kubectl` to interact with your GKE
  cluster later in the tutorial.

- If you do not have `gke-gcloud-auth-plugin` for `kubectl`, install the
  `gke-gcloud-auth-plugin`. For details, see [Install the
  gke-gcloud-auth-plugin](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#install_plugin).

### Helm 3.2.0+

If you do not have Helm version 3.2.0+ installed, install. For details,
see the [Helm documentation](https://helm.sh/docs/intro/install/).

### jq (Optional)

*Optional*. `jq` is used to parse the GKE cluster name and region from
the Terraform outputs. Alternatively, you can manually specify the name
and region. If you want to use `jq` and do not have `jq` installed,
install.

## A. Configure GCP project and service account

1.  Open a Terminal window.

2.  Initialize the gcloud CLI (`gcloud init`) to specify the GCP project
    you want to use. For details, see the [Initializing the gcloud CLI
    documentation](https://cloud.google.com/sdk/docs/initializing#initialize_the).

    <div class="tip">

    **💡 Tip:** You do not need to configure a default Compute Region
    and Zone as you will specify the region.

    </div>

3.  Enable the following services for your GCP project, if not already
    enabled:

    <div class="highlight">

    ``` chroma
    gcloud services enable container.googleapis.com        # For creating Kubernetes clusters
    gcloud services enable sqladmin.googleapis.com         # For creating databases
    gcloud services enable cloudresourcemanager.googleapis.com # For managing GCP resources
    gcloud services enable servicenetworking.googleapis.com  # For private network connections
    gcloud services enable iamcredentials.googleapis.com     # For security and authentication
    ```

    </div>

4.  To the service account that will run the Terraform script, grant the
    following IAM roles:

    - `roles/editor`
    - `roles/iam.serviceAccountAdmin`
    - `roles/servicenetworking.networksAdmin`
    - `roles/storage.admin`
    - `roles/container.admin`

    1.  Enter your GCP project ID.

        <div class="highlight">

        ``` chroma
        read -s PROJECT_ID
        ```

        </div>

    2.  Find your service account email for your GCP project

        <div class="highlight">

        ``` chroma
        gcloud iam service-accounts list --project $PROJECT_ID
        ```

        </div>

    3.  Enter your service account email.

        <div class="highlight">

        ``` chroma
        read -s SERVICE_ACCOUNT
        ```

        </div>

    4.  Grant the service account the neccessary IAM roles.

        <div class="highlight">

        ``` chroma
        gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/editor"

        gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/iam.serviceAccountAdmin"

        gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/servicenetworking.networksAdmin"

        gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/storage.admin"

        gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/container.admin"
        ```

        </div>

5.  For the service account, authenticate to allow Terraform to interact
    with your GCP project. For details, see [Terraform: Google Cloud
    Provider Configuration
    reference](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#authentication).

    For example, if using [User Application Default
    Credentials](https://cloud.google.com/sdk/gcloud/reference/auth/application-default),
    you can run the following command:

    <div class="highlight">

    ``` chroma
    gcloud auth application-default login
    ```

    </div>

    <div class="tip">

    **💡 Tip:** If using `GOOGLE_APPLICATION_CREDENTIALS`, use absolute
    path to your key file.

    </div>

## B. Set up GCP Kubernetes environment and install Materialize

<div class="warning">

**WARNING!**

The Terraform modules used in this tutorial are intended for
evaluation/demonstration purposes and for serving as a template when
building your own production deployment. The modules should not be
directly relied upon for production deployments: **future releases of
the modules will contain breaking changes.** Instead, to use as a
starting point for your own production deployment, either:

- Fork the repo and pin to a specific version; or

- Use the code as a reference when developing your own deployment.

</div>

<div class="code-tabs">

<div class="tab-content">

<div id="tab-deployed-components" class="tab-pane"
title="Deployed components">

[Materialize on GCP Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize)
deploys a sample infrastructure on GCP (region `us-central1`) with the
following components:

| Component | Version |
|----|----|
| Google Kubernetes Engine (GKE) cluster | All |
| Cloud Storage bucket for blob storage | All |
| Cloud SQL PostgreSQL database for metadata storage | All |
| Dedicated VPC | All |
| Service accounts with proper IAM permissions | All |
| Materialize Operator | All |
| Materialize instances (Deployed during subsequent runs after the Operator is running) | All |
| Load balancers for each Materialize instance | [v0.3.0+](/docs/self-managed/v25.2/installation/appendix-terraforms/#materialize-on-gcp-terraform-module) |
| `cert-manager` and a self-signed `ClusterIssuer`. `ClusterIssuer` is deployed on subsequent runs after the `cert-manager` is running. | [v0.3.0+](/docs/self-managed/v25.2/installation/appendix-terraforms/#materialize-on-gcp-terraform-module) |
| OpenEBS and NVMe instance storage to enable spill-to-disk | [v0.4.0+](/docs/self-managed/v25.2/installation/appendix-terraforms/#materialize-on-gcp-terraform-module) |

<div class="tip">

**💡 Tip:**

The tutorial uses the `main.tf` found in the `examples/simple/`
directory, which requires minimal user input. For details on the
`examples/simple/` infrastructure configuration (such as the node
instance type, etc.), see the
[examples/simple/main.tf](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/examples/simple/main.tf).

For more configuration options, you can use the `main.tf` file at the
[root of the
repository](https://github.com/MaterializeInc/terraform-google-materialize/)
instead. When running with the root `main.tf`, see [GCP required
configuration](/docs/self-managed/v25.2/installation/install-on-gcp/appendix-gcp-configuration/).

</div>

</div>

<div id="tab-releases" class="tab-pane" title="Releases">

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Terraform version</th>
<th>Notable changes</th>
</tr>
</thead>
<tbody>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/">v0.5.5</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.26.</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/">v0.5.4</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.25.</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/">v0.4.6</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.19.</li>
<li>Bumps Materialize release to <a
href="/docs/self-managed/v25.2/release-notes">self-managed 25.2</a></li>
<li>Adds support for password authentication and enabling RBAC</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/">v0.4.3</a></td>
<td><ul>
<li><p>Adds support for passing in additional Materialize instance
configuration options via <a
href="https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances"><code>environmentd_extra_args</code></a></p>
<p>To use, set the instance’s <code>environmentd_extra_env</code> to an
array of strings; for example:</p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>materialize_instances = [
  {
    ...
    environmentd_extra_args = [
      &quot;--system-parameter-default=&lt;param&gt;=&lt;value&gt;&quot;,
      &quot;--bootstrap-builtin-catalog-server-cluster-replica-size=50cc&quot;
    ]
  }
]</code></pre>
</div></li>
<li><p>Uses <code>terraform-helm-materialize</code> v0.1.15.</p></li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/tag/v0.4.1">v0.4.1</a></td>
<td><ul>
<li>Defaults to using Materialize Operator v25.1.11 (via
<code>terraform-helm-materialize</code> v0.1.13).</li>
</ul></td>
</tr>
</tbody>
</table>

</div>

</div>

</div>

1.  Fork the [Materialize’s sample Terraform
    repo](https://github.com/MaterializeInc/terraform-google-materialize).

2.  Set `MY_ORGANIZATION` to your github organization name, substituting
    your organization’s name for `<enter-your-organization>`:

    <div class="highlight">

    ``` chroma
    MY_ORGANIZATION=<enter-your-organization>
    ```

    </div>

3.  Clone your forked repo and checkout the `v0.5.10` tag. For example,

    - If cloning via SSH:

      <div class="highlight">

      ``` chroma
      git clone --depth 1 -b v0.5.10 git@github.com:${MY_ORGANIZATION}/terraform-google-materialize.git
      ```

      </div>

    - If cloning via HTTPS:

      <div class="highlight">

      ``` chroma
      git clone --depth 1 -b v0.5.10 https://github.com/${MY_ORGANIZATION}/terraform-google-materialize.git
      ```

      </div>

4.  Go to the `examples/simple` folder in the Materialize Terraform repo
    directory.

    <div class="highlight">

    ``` chroma
    cd terraform-google-materialize/examples/simple
    ```

    </div>

    <div class="tip">

    **💡 Tip:**
    The tutorial uses the `main.tf` found in the `examples/simple/`
    directory, which requires minimal user input. For details on the
    `examples/simple/` infrastructure configuration (such as the node
    instance type, etc.), see the
    [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/examples/simple/main.tf).

    For more configuration options, you can use the `main.tf` file at
    the [root of the
    repository](https://github.com/MaterializeInc/terraform-google-materialize/)
    instead. When running with the root `main.tf`, see [GCP required
    configuration](/docs/self-managed/v25.2/installation/install-on-gcp/appendix-gcp-configuration/).

    </div>

5.  Create a `terraform.tfvars` file (you can copy from the
    `terraform.tfvars.example` file) and specify the following
    variables:

    | **Variable** | **Description** |
    |----|----|
    | `project_id` | Your GCP project ID. |
    | `prefix` | A prefix (e.g., `mz-simple`) for your resources. Prefix has a maximum of 15 characters and contains only alphanumeric characters and dashes. |
    | `region` | The region for the GKE cluster. |

    <div class="highlight">

    ``` chroma
    project_id = "enter-your-gcp-project-id"
    prefix  = "enter-your-prefix" //  Maximum of 15 characters, contain lowercase alphanumeric and hyphens only (e.g., mz-simple)
    region = "us-central1"
    ```

    </div>

    <div class="tip">

    **💡 Tip:**
    The tutorial uses the `main.tf` found in the `examples/simple/`
    directory, which requires minimal user input. For details on the
    `examples/simple/` infrastructure configuration (such as the node
    instance type, etc.), see the
    [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/examples/simple/main.tf).

    For more configuration options, you can use the `main.tf` file at
    the [root of the
    repository](https://github.com/MaterializeInc/terraform-google-materialize/)
    instead. When running with the root `main.tf`, see [GCP required
    configuration](/docs/self-managed/v25.2/installation/install-on-gcp/appendix-gcp-configuration/).

    </div>

6.  Initialize the terraform directory.

    <div class="highlight">

    ``` chroma
    terraform init
    ```

    </div>

7.  Run terraform plan and review the changes to be made.

    <div class="highlight">

    ``` chroma
    terraform plan
    ```

    </div>

8.  If you are satisfied with the changes, apply.

    <div class="highlight">

    ``` chroma
    terraform apply
    ```

    </div>

    To approve the changes and apply, enter `yes`.

    Upon successful completion, various fields and their values are
    output:

    <div class="highlight">

    ``` chroma
    Apply complete! Resources: 27 added, 0 changed, 0 destroyed.

    Outputs:

    connection_strings = <sensitive>
    gke_cluster = <sensitive>
    load_balancer_details = {}
    network = {
       "network_id" = "projects/my-project/global/networks/mz-simple-network"
       "network_name" = "mz-simple-network"
       "subnet_name" = "mz-simple-subnet"
    }
    service_accounts = {
       "gke_sa" = "mz-simple-gke-sa@my-project.iam.gserviceaccount.com"
       "materialize_sa" = "mz-simple-materialize-sa@my-project.iam.gserviceaccount.com"
    }
    ```

    </div>

9.  Configure `kubectl` to connect to your GKE cluster, specifying:

    - `<cluster name>`. Your cluster name has the form
      `<your prefix>-gke`; e.g., `mz-simple-gke`.

    - `<region>`. By default, the example Terraform module uses the
      `us-central1` region.

    - `<project>`. Your GCP project ID.

    <div class="highlight">

    ``` chroma
    gcloud container clusters get-credentials <cluster-name>  \
     --region <region> \
     --project <project>
    ```

    </div>

    Alternatively, you can use the following command to get the cluster
    name and region from the Terraform output and the project ID from
    the environment variable set earlier.

    <div class="highlight">

    ``` chroma
    gcloud container clusters get-credentials $(terraform output -json gke_cluster | jq -r .name) \
     --region $(terraform output -json gke_cluster | jq -r .location) --project $PROJECT_ID
    ```

    </div>

    To verify that you have configured correctly, run the following
    command:

    <div class="highlight">

    ``` chroma
    kubectl cluster-info
    ```

    </div>

    For help with `kubectl` commands, see [kubectl Quick
    reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

10. By default, the example Terraform installs the Materialize Operator
    and, starting in v0.3.0, a `cert-manager`. Verify the installation
    and check the status:

    <div class="code-tabs">

    <div class="tab-content">

    <div id="tab-materialize-operator" class="tab-pane"
    title="Materialize Operator">

    Verify the installation and check the status:

    <div class="highlight">

    ``` chroma
    kubectl get all -n materialize
    ```

    </div>

    Wait for the components to be in the `Running` state:

    ```
    NAME                                                              READY       STATUS    RESTARTS   AGE
    pod/materialize-mz-simple-materialize-operator-74d8f549d6-lkjjf   1/1         Running   0          36m
    NAME                                                         READY       UP-TO-DATE   AVAILABLE   AGE
    deployment.apps/materialize-mz-simple-materialize-operator   1/1         1            1           36m
    NAME                                                                        DESIRED   CURRENT   READY   AGE
    replicaset.apps/materialize-mz-simple-materialize-operator-74d8f549d6       1         1         1       36m
    ```

    </div>

    <div id="tab-cert-manager-starting-in-version-030" class="tab-pane"
    title="cert-manager (Starting in version 0.3.0)">

    Verify the installation and check the status:

    <div class="highlight">

    ``` chroma
    kubectl get all -n cert-manager
    ```

    </div>

    Wait for the components to be in the `Running` state:

    ```
    NAME                                           READY   STATUS    RESTARTS   AGE
    pod/cert-manager-6794b8d569-vt264              1/1     Running   0          22m
    pod/cert-manager-cainjector-7f69cd69f7-7brqw   1/1     Running   0          22m
    pod/cert-manager-webhook-6cc5dccc4b-7tmd4      1/1     Running   0          22m
    NAME                              TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)            AGE
    service/cert-manager              ClusterIP   10.52.3.63     <none>        9402/TCP           22m
    service/cert-manager-cainjector   ClusterIP   10.52.15.171   <none>        9402/TCP           22m
    service/cert-manager-webhook      ClusterIP   10.52.5.148    <none>        443/TCP,9402/TCP   22m
    NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
    deployment.apps/cert-manager              1/1     1            1           22m
    deployment.apps/cert-manager-cainjector   1/1     1            1           22m
    deployment.apps/cert-manager-webhook      1/1     1            1           22m
    NAME                                                 DESIRED   CURRENT   READY   AGE
    replicaset.apps/cert-manager-6794b8d569              1         1         1       22m
    replicaset.apps/cert-manager-cainjector-7f69cd69f7   1         1         1       22m
    replicaset.apps/cert-manager-webhook-6cc5dccc4b      1         1         1       22m
    ```

    </div>

    </div>

    </div>

    If you run into an error during deployment, refer to the
    [Troubleshooting](/docs/self-managed/v25.2/installation/troubleshooting/).

11. Once the Materialize operator is deployed and running, you can
    deploy the Materialize instances. To deploy Materialize instances,
    create a `mz_instances.tfvars` file with the Materialize instance
    configuration.

    For example, the following specifies the configuration for a `demo`
    instance.

    <div class="highlight">

    ``` chroma
    cat <<EOF > mz_instances.tfvars

    materialize_instances = [
        {
          name           = "demo"
          namespace      = "materialize-environment"
          database_name  = "demo_db"
          cpu_request    = "1"
          memory_request = "2Gi"
          memory_limit   = "2Gi"
        }
    ]
    EOF
    ```

    </div>

    - **Starting in v0.3.0**, the Materialize on GCP Terraform module
      also deploys, by default:

      - [Load
        balancers](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances)
        for Materialize instances (i.e., the
        [`create_load_balancer`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances)
        flag defaults to `true`). The load balancers, by default, are
        configured to be internal (i.e., the
        [`internal_load_balancer`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances)
        flag defaults to `true`).

      - A self-signed `ClusterIssuer`. The `ClusterIssuer` is deployed
        after the `cert-manager` is deployed and running.

    - **Starting in v0.4.3**, you can specify addition configuration
      options via `environmentd_extra_args`.

    <div class="tip">

    **💡 Tip:**
    If upgrading from a deployment that was set up using an earlier
    version of the Terraform modules, additional considerations may
    apply when using an updated Terraform modules to your existing
    deployments.

    See [Materialize on GCP
    releases](/docs/self-managed/v25.2/installation/appendix-terraforms/#materialize-on-gcp-terraform-module)
    for notable changes.

    </div>

12. Run `terraform plan` with both `.tfvars` files and review the
    changes to be made.

    <div class="highlight">

    ``` chroma
    terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
    ```

    </div>

    The plan should show the changes to be made, with a summary similar
    to the following:

    ```
    Plan: 9 to add, 1 to change, 0 to destroy.
    ```

13. If you are satisfied with the changes, apply.

    <div class="highlight">

    ``` chroma
    terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
    ```

    </div>

    To approve the changes and apply, enter `yes`.

    <span id="gcp-terraform-output"></span> Upon successful completion,
    you should see output with a summary similar to the following:

    <div class="highlight">

    ``` chroma
    Apply complete! Resources: 9 added, 1 changed, 0 destroyed.

    Outputs:

    connection_strings = <sensitive>
    gke_cluster = <sensitive>
    load_balancer_details = {
       "demo" = {
          "balancerd_load_balancer_ip" = "192.0.2.10"
          "console_load_balancer_ip" = "192.0.2.254"
       }
    }
    network = {
       "network_id" = "projects/my-project/global/networks/mz-simple-network"
       "network_name" = "mz-simple-network"
       "subnet_name" = "mz-simple-subnet"
    }
    service_accounts = {
       "gke_sa" = "mz-simple-gke-sa@my-project.iam.gserviceaccount.com"
       "materialize_sa" = "mz-simple-materialize-sa@my-project.iam.gserviceaccount.com"
    }
    ```

    </div>

14. Verify the installation and check the status:

    <div class="highlight">

    ``` chroma
    kubectl get all -n materialize-environment
    ```

    </div>

    Wait for the components to be in the `Running` state.

    ```
    NAME                                             READY   STATUS      RESTARTS   AGE
    pod/db-demo-db-wrvhw                             0/1     Completed   0          4m26s
    pod/mzdtwvu4qe4q-balancerd-6989df5c75-mpmqx      1/1     Running     0          3m54s
    pod/mzdtwvu4qe4q-cluster-s2-replica-s1-gen-1-0   1/1     Running     0          3m53s
    pod/mzdtwvu4qe4q-cluster-u1-replica-u1-gen-1-0   1/1     Running     0          3m52s
    pod/mzdtwvu4qe4q-console-7c9bc94bcb-6t7lg        1/1     Running     0          3m41s
    pod/mzdtwvu4qe4q-console-7c9bc94bcb-9x5qq        1/1     Running     0          3m41s
    pod/mzdtwvu4qe4q-environmentd-1-0                1/1     Running     0          4m9s

    NAME                                               TYPE           CLUSTER-IP    EXTERNAL-IP     PORT(S)                                        AGE
    service/mzdtwvu4qe4q-balancerd                     ClusterIP      None          <none>          6876/TCP,6875/TCP                              3m54s
    service/mzdtwvu4qe4q-balancerd-lb                  LoadBalancer   10.52.5.105   192.0.2.10      6875:30844/TCP,6876:32307/TCP                  4m9s
    service/mzdtwvu4qe4q-cluster-s2-replica-s1-gen-1   ClusterIP      None          <none>          2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   3m53s
    service/mzdtwvu4qe4q-cluster-u1-replica-u1-gen-1   ClusterIP      None          <none>          2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   3m52s
    service/mzdtwvu4qe4q-console                       ClusterIP      None          <none>          8080/TCP                                       3m41s
    service/mzdtwvu4qe4q-console-lb                    LoadBalancer   10.52.4.2     192.0.2.254     8080:32193/TCP                                 4m9s
    service/mzdtwvu4qe4q-environmentd                  ClusterIP      None          <none>          6875/TCP,6876/TCP,6877/TCP,6878/TCP            3m54s
    service/mzdtwvu4qe4q-environmentd-1                ClusterIP      None          <none>          6875/TCP,6876/TCP,6877/TCP,6878/TCP            4m9s
    service/mzdtwvu4qe4q-persist-pubsub-1              ClusterIP      None          <none>          6879/TCP                                       4m9s

    NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
    deployment.apps/mzdtwvu4qe4q-balancerd   1/1     1            1           3m54s
    deployment.apps/mzdtwvu4qe4q-console     2/2     2            2           3m41s

    NAME                                                DESIRED   CURRENT   READY   AGE
    replicaset.apps/mzdtwvu4qe4q-balancerd-6989df5c75   1         1         1       3m54s
    replicaset.apps/mzdtwvu4qe4q-console-7c9bc94bcb     2         2         2       3m41s

    NAME                                                        READY   AGE
    statefulset.apps/mzdtwvu4qe4q-cluster-s2-replica-s1-gen-1   1/1     3m53s
    statefulset.apps/mzdtwvu4qe4q-cluster-u1-replica-u1-gen-1   1/1     3m52s
    statefulset.apps/mzdtwvu4qe4q-environmentd-1                1/1     4m9s

    NAME                   STATUS     COMPLETIONS   DURATION   AGE
    job.batch/db-demo-db   Complete   1/1           12s        4m27s
    ```

    If you run into an error during deployment, refer to the
    [Troubleshooting](/docs/self-managed/v25.2/installation/troubleshooting/).

15. Open the Materialize Console in your browser:

    <div class="code-tabs">

    <div class="tab-content">

    <div id="tab-via-network-load-balancer" class="tab-pane"
    title="Via Network Load Balancer">

    Starting in v0.3.0, for each Materialize instance, Materialize on
    GCP Terraform module also deploys load balancers (by default,
    internal) with the following listeners, including a listener on port
    8080 for the Materialize Console:

    | Port     | Description                                        |
    |----------|----------------------------------------------------|
    | 6875     | For SQL connections to the database                |
    | 6876     | For HTTP(S) connections to the database            |
    | **8080** | **For HTTP(S) connections to Materialize Console** |

    The load balancer details are found in the `load_balancer_details`
    in the [Terraform output](#gcp-terraform-output).

    The example uses a self-signed ClusterIssuer. As such, you may
    encounter a warning with regards to the certificate. In production,
    run with certificates from an official Certificate Authority (CA)
    rather than self-signed certificates.

    </div>

    <div id="tab-via-port-forwarding" class="tab-pane"
    title="Via port forwarding">

    1.  Find your console service name.

        <div class="highlight">

        ``` chroma
        MZ_SVC_CONSOLE=$(kubectl -n materialize-environment get svc \
          -o custom-columns="NAME:.metadata.name" --no-headers | grep console-lb)
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

    3.  Open a browser and navigate to <https://localhost:8080> (or, if
        you have not enabled TLS, <http://localhost:8080>).

        The example uses a self-signed ClusterIssuer. As such, you may
        encounter a warning with regards to the certificate. In
        production, run with certificates from an official Certificate
        Authority (CA) rather than self-signed certificates.

    </div>

    </div>

    </div>

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

## Cleanup

To delete the whole sample infrastructure and deployment (including the
Materialize operator and Materialize instances and data), run from the
Terraform directory:

<div class="highlight">

``` chroma
terraform destroy
```

</div>

When prompted to proceed, type `yes` to confirm the deletion.

## See also

- [Troubleshooting](/docs/self-managed/v25.2/installation/troubleshooting/)
- [Materialize Operator
  Configuration](/docs/self-managed/v25.2/installation/configuration/)
- [Appendix: Google deployment
  guidelines](/docs/self-managed/v25.2/installation/install-on-gcp/appendix-deployment-guidelines/)
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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/installation/install-on-gcp/_index.md"
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
