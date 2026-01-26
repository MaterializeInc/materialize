# Install Guides (Legacy)

Install Self-Managed Materialize using legacy Terraform modules




<h3 id="install-using-legacy-terraform-modules">Install using Legacy Terraform Modules</h3>
> **Tip:** The Terraform modules are provided as examples. They are not required for
> installing Materialize.
>

<table>
<thead>
<tr>
<th>Guide</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><a href="/self-managed-deployments/installation/legacy/install-on-aws-legacy/" >Install on AWS (Legacy Terraform)</a></td>
<td>Uses legacy Terraform module to deploy Materialize to AWS Elastic Kubernetes Service (EKS).</td>
</tr>
<tr>
<td><a href="/self-managed-deployments/installation/legacy/install-on-azure-legacy/" >Install on Azure (Legacy Terraform)</a></td>
<td>Uses legacy Terraform module to deploy Materialize to Azure Kubernetes Service (AKS).</td>
</tr>
<tr>
<td><a href="/self-managed-deployments/installation/legacy/install-on-gcp-legacy/" >Install on GCP (Legacy Terraform)</a></td>
<td>Uses legacy Terraform module to deploy Materialize to Google Kubernetes Engine (GKE).</td>
</tr>
</tbody>
</table>




---

## Install on AWS(Legacy Terraform)



Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; and a license key.


The tutorial deploys Materialize to AWS Elastic Kubernetes Service (EKS) with a
PostgreSQL RDS database as the metadata database and AWS S3 for blob storage.
The tutorial uses the [Legacy Materialize on AWS Terraform
module](https://github.com/MaterializeInc/terraform-aws-materialize) to:

- Set up the AWS Kubernetes environment.
- Call
  [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
  module to deploy Materialize Operator and Materialize instances to that EKS
  cluster.

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
>
> - Fork the repo and pin to a specific version; or
>
> - Use the code as a reference when developing your own deployment.
>
>
>


When operating in AWS, we recommend the following instances:

| EC2 Instances  |
| ---------------|
| `r8g`, `r7g`, and `r6g` families when running without local disk. |
| `r7gd` and `r6gd` families (and `r8gd` once available) when running with local disk.  *Recommended for production.* |

Starting in v0.3.1, the Materialize on AWS Terraform uses `["r7gd.2xlarge"]` as
the default [`node_group_instance_types`].

[`node_group_instance_types`]:
    https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_node_group_instance_types


See [AWS Deployment guidelines](/self-managed-deployments/deployment-guidelines/aws-deployment-guidelines/) for
more information.

## Prerequisites

### Terraform

If you don't have Terraform installed, [install
Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### AWS CLI

If you do not have the AWS CLI installed, install. For details, see the [AWS
documentation](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).

### kubectl

If you do not have `kubectl`, install. See the [Amazon EKS: install `kubectl`
documentation](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html)
for details.

### Helm 3.2.0+

If you do not have Helm 3.2.0+, install. For details, see the [Helm
documentation](https://helm.sh/docs/intro/install/).

### License key



## Set up AWS Kubernetes environment and install Materialize

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
>
> - Fork the repo and pin to a specific version; or
>
> - Use the code as a reference when developing your own deployment.
>
>
>
> For simplicity, this tutorial stores various secrets in a file as well as prints
> them to the terminal. In practice, refer to your organization's official
> security and Terraform/infrastructure practices.
>
>
>




**Deployed components:**

[Materialize on AWS Terraform
module](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/README.md)
deploys a sample infrastructure on AWS (region `us-east-1`) with the following
components:


| Component | Version |
| --- | --- |
| Kubernetes (EKS) cluster | All |
| Dedicated VPC | All |
| S3 for blob storage | All |
| RDS PostgreSQL cluster and database | All |
| Materialize Operator | All |
| Materialize instances (Deployed during subsequent runs after the Operator is running) | All |
| AWS Load Balancer Controller and Network Load Balancers for each Materialize instance | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-aws-terraform-module" >v0.3.0+</a> |
| OpenEBS and NVMe instance storage to enable spill-to-disk | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-aws-terraform-module" >v0.3.1+</a> |
| <code>cert-manager</code> and a self-signed <code>ClusterIssuer</code>. <code>ClusterIssuer</code> is deployed on subsequent runs after the <code>cert-manager</code> is running. | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-aws-terraform-module" >v0.4.0+</a> |


> **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory, which
> requires minimal user input. For details on the `examples/simple/`
> infrastructure configuration (such as the node instance type, etc.), see the
> [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/examples/simple/main.tf).
>
> For more configuration options, you can use the `main.tf` file at the [root of
> the repository](https://github.com/MaterializeInc/terraform-aws-materialize/)
> instead. When running with the root `main.tf`, see [AWS required
> configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-aws/).
>
>



**Releases:**


| Terraform version | Notable changes |
| --- | --- |
| <a href="https://github.com/MaterializeInc/terraform-aws-materialize/releases/tag/v0.6.4" >v0.6.4</a> | <ul> <li>Released as part of v26.0.0.</li> <li>Uses <code>terraform-helm-materialize</code> version <code>v0.1.35</code>.</li> </ul>  |





1. Open a Terminal window.

1. Configure AWS CLI with your AWS credentials. For details, see the [AWS
   documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).



1. Fork the [Materialize's sample Terraform
   repo](https://github.com/MaterializeInc/terraform-aws-materialize).

1. Set `MY_ORGANIZATION` to your github organization name, substituting your
   organization's name for `<enter-your-organization>`:

   ```bash
   MY_ORGANIZATION=<enter-your-organization>
   ```

1. Clone your forked repo and checkout the `v0.8.10` tag. For example,

   - If cloning via SSH (replace `YOUR_ORGANIZATION` with your organization's
     name):

     ```bash
     git clone --depth 1 -b v0.8.10 git@github.com:${MY_ORGANIZATION}/terraform-aws-materialize.git
     ```

   - If cloning via HTTPS (replace `YOUR_ORGANIZATION` with your
     organization's name):

     ```bash
     git clone --depth 1 -b v0.8.10 https://github.com/${MY_ORGANIZATION}/terraform-aws-materialize.git
     ```


1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-aws-materialize/examples/simple
   ```

   > **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory, which
>    requires minimal user input. For details on the `examples/simple/`
>    infrastructure configuration (such as the node instance type, etc.), see the
>    [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/examples/simple/main.tf).
>
>    For more configuration options, you can use the `main.tf` file at the [root of
>    the repository](https://github.com/MaterializeInc/terraform-aws-materialize/)
>    instead. When running with the root `main.tf`, see [AWS required
>    configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-aws/).
>
>


1. Create a `terraform.tfvars` file (you can copy from the
   `terraform.tfvars.example` file) and specify the following variables:

   | Variable          | Description |
   |--------------------|-------------|
   | `namespace`       | A namespace (e.g., `my-demo`) that will be used to form part of the prefix for your AWS resources. <br> **Requirements:** <br> - Maximum of 12 characters <br> - Must start with a lowercase letter <br> - Must be lowercase alphanumeric and hyphens only |
   | `environment`     | An environment name (e.g., `dev`, `test`) that will be used to form part of the prefix for your AWS resources. <br> **Requirements:** <br> - Maximum of 8 characters <br> - Must be lowercase alphanumeric only |


   ```bash
   # The namespace and environment variables are used to construct the names of   the resources
   # e.g. ${namespace}-${environment}-storage, ${namespace}-${environment}-db   etc.

   namespace = "enter-namespace"   // maximum 12 characters, start with a   letter, contain lowercase alphanumeric and hyphens only (e.g. my-demo)
   environment = "enter-environment" // maximum 8 characters, lowercase   alphanumeric only (e.g., dev, test)
   ```

   > **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory, which
>    requires minimal user input. For details on the `examples/simple/`
>    infrastructure configuration (such as the node instance type, etc.), see the
>    [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/examples/simple/main.tf).
>
>    For more configuration options, you can use the `main.tf` file at the [root of
>    the repository](https://github.com/MaterializeInc/terraform-aws-materialize/)
>    instead. When running with the root `main.tf`, see [AWS required
>    configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-aws/).
>
>


1. Initialize the terraform directory.

    ```bash
    terraform init
    ```

1. Use terraform plan to review the changes to be made.

    ```bash
    terraform plan
    ```

1. If you are satisfied with the changes, apply.

   ```bash
   terraform apply
   ```

   To approve the changes and apply, enter `yes`.

   <a name="terraform-output"></a>

   Upon successful completion, various fields and their values are output:

   ```none
   Apply complete! Resources: 89 added, 0 changed, 0 destroyed.

   Outputs:

   cluster_certificate_authority_data = <sensitive>
   database_endpoint = "my-demo-dev-db.abcdefg8dsto.us-east-1.rds.amazonaws.com:5432"
   eks_cluster_endpoint = "https://0123456789A00BCD000E11BE12345A01.gr7.us-east-1.eks.amazonaws.com"
   eks_cluster_name = "my-demo-dev-eks"
   materialize_s3_role_arn = "arn:aws:iam::000111222333:role/my-demo-dev-mz-role"
   metadata_backend_url = <sensitive>
   nlb_details = []
   oidc_provider_arn = "arn:aws:iam::000111222333:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/7D14BCA3A7AA896A836782D96A24F958"
   persist_backend_url = "s3://my-demo-dev-storage-f2def2a9/dev:serviceaccount:materialize-environment:12345678-1234-1234-1234-12345678912"
   s3_bucket_name = "my-demo-dev-storage-f2def2a9"
   vpc_id = "vpc-0abc000bed1d111bd"
   ```

1. Note your specific values for the following fields:

   - `eks_cluster_name` (Used to configure `kubectl`)

1. Configure `kubectl` to connect to your EKS cluster, replacing:

   - `<your-eks-cluster-name>` with the name of your EKS cluster. Your cluster
       name has the form `{namespace}-{environment}-eks`; e.g.,
       `my-demo-dev-eks`.

   - `<your-region>` with the region of your EKS cluster. The
     simple example uses `us-east-1`.

   ```bash
   aws eks update-kubeconfig --name <your-eks-cluster-name> --region <your-region>
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

1. By default, the example Terraform installs the Materialize Operator and,
   starting in v0.4.0, a `cert-manager`. Verify the installation and check the
   status:


   **Materialize Operator:**

   Verify the installation and check the status:

   ```shell
   kubectl get all -n materialize
   ```

   Wait for the components to be in the `Running` state:

   ```none
   NAME                                                           READY  STATUS    RESTARTS   AGE
   pod/my-demo-dev-materialize-operator-84ff4b4648-brjhl   1/1     Running  0          12s

   NAME                                                      READY  UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/my-demo-dev-materialize-operator   1/1     1           1           12s

   NAME                                                             DESIRED    CURRENT   READY   AGE
   replicaset.apps/my-demo-dev-materialize-operator-84ff4b4648   1        1         1       12s
   ```


   **cert-manager (Starting in version 0.4.0):**

   Verify the installation and check the status:

   ```shell
   kubectl get all -n cert-manager
   ```
   Wait for the components to be in the `Running` state:
   ```
   NAME                                           READY   STATUS   RESTARTS     AGE
   pod/cert-manager-cainjector-686546c9f7-v9hwp   1/1     Running  0            4m20s
   pod/cert-manager-d6746cf45-cdmb5               1/1     Running  0            4m20s
   pod/cert-manager-webhook-5f79cd6f4b-rcjbq      1/1     Running  0            4m20s
   NAME                              TYPE        CLUSTER-IP      EXTERNAL-IP     PORT(S)            AGE
   service/cert-manager              ClusterIP   172.20.2.136    <none>          9402/TCP           4m20s
   service/cert-manager-cainjector   ClusterIP   172.20.154.137  <none>          9402/TCP           4m20s
   service/cert-manager-webhook      ClusterIP   172.20.63.217   <none>          443/TCP,9402/TCP   4m20s
   NAME                                      READY   UP-TO-DATE  AVAILABLE     AGE
   deployment.apps/cert-manager              1/1     1           1             4m20s
   deployment.apps/cert-manager-cainjector   1/1     1           1             4m20s
   deployment.apps/cert-manager-webhook      1/1     1           1             4m20s
   NAME                                                 DESIRED   CURRENT    READY   AGE
   replicaset.apps/cert-manager-cainjector-686546c9f7   1         1          1       4m20s
   replicaset.apps/cert-manager-d6746cf45               1         1          1       4m20s
   replicaset.apps/cert-manager-webhook-5f79cd6f4b      1         1         1
   4m20s
   ```




   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting) guide.

1. Once the Materialize operator is deployed and running, you can deploy the
   Materialize instances. To deploy Materialize instances, create  a
   `mz_instances.tfvars` file with the [Materialize instance
   configuration](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances).

   For example, the following specifies the configuration for a `demo` instance.

   ```bash
   cat <<EOF > mz_instances.tfvars

   materialize_instances = [
       {
         name           = "demo"
         namespace      = "materialize-environment"
         database_name  = "demo_db"
         cpu_request    = "1"
         memory_request = "2Gi"
         memory_limit   = "2Gi"
         license_key    = ""
       }
   ]
   EOF
   ```

   - **Starting in v0.3.0**, the Materialize on AWS Terraform module also
   deploys, by default, Network Load Balancers (NLBs) for each Materialize
   instance (i.e., the
   [`create_nlb`](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`).  The NLBs, by default, are configured to be
    internal (i.e., the
    [`internal_nlb`](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`). See [`materialize_instances`](
   https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances)
   for the Materialize instance configuration options.

   - **Starting in v0.4.0**, a self-signed `ClusterIssuer` is deployed by
   default. The `ClusterIssuer` is deployed on subsequent after the
   `cert-manager` is running.

   - **Starting in v0.4.6**, you can specify addition configuration options via
     `environmentd_extra_args`.

   > **Tip:** If upgrading from a deployment that was set up using an earlier version of the
>    Terraform modules, additional considerations may apply when using an updated Terraform modules to your existing deployments.
>
>
>    See [Materialize on AWS releases](/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-aws-terraform-module) for notable changes.
>


1. Run `terraform plan` with both `.tfvars` files and review the changes to be
   made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```
   Plan: 17 to add, 1 to change, 0 to destroy.
   ```

1. If you are satisfied with the changes, apply.

   ```bash
   terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   To approve the changes and apply, enter `yes`.

   Upon successful completion, you should see output with a summary similar to
   the following:

   <a name="aws-terrafrom-output"></a>

   ```bash
   Apply complete! Resources: 17 added, 1 changed, 0 destroyed.

   Outputs:

   cluster_certificate_authority_data = <sensitive>
   database_endpoint = "my-demo-dev-db.abcdefg8dsto.us-east-1.rds.amazonaws.com:5432"
   eks_cluster_endpoint = "https://0123456789A00BCD000E11BE12345A01.gr7.us-east-1.eks.amazonaws.com"
   eks_cluster_name = "my-demo-dev-eks"
   materialize_s3_role_arn = "arn:aws:iam::000111222333:role/my-demo-dev-mz-role"
   metadata_backend_url = <sensitive>
   nlb_details = [
     "demo" = {
       "arn" = "arn:aws:elasticloadbalancing:us-east-1:000111222333:loadbalancer/net/my-demo-dev/aeae3d936afebcfe"
       "dns_name" = "my-demo-dev-aeae3d936afebcfe.elb.us-east-1.amazonaws.com"
     }
   ]
   oidc_provider_arn = "arn:aws:iam::000111222333:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/7D14BCA3A7AA896A836782D96A24F958"
   persist_backend_url = "s3://my-demo-dev-storage-f2def2a9/dev:serviceaccount:materialize-environment:12345678-1234-1234-1234-12345678912"
   s3_bucket_name = "my-demo-dev-storage-f2def2a9"
   vpc_id = "vpc-0abc000bed1d111bd"
   ```

   The Network Load Balancer (NLB) details `nlb_details` are available when
   running the Terraform module v0.3.0+.

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be in the `Running` state.

   ```none
   NAME                                             READY   STATUS      RESTARTS      AGE
   pod/create-db-demo-db-6swk7                      0/1     Completed   0             33s
   pod/mzutd2fbabf5-balancerd-6c9755c498-28kcw      1/1     Running     0             11s
   pod/mzutd2fbabf5-cluster-s2-replica-s1-gen-1-0   1/1     Running     0             11s
   pod/mzutd2fbabf5-cluster-u1-replica-u1-gen-1-0   1/1     Running     0             11s
   pod/mzutd2fbabf5-console-57f94b4588-6lg2x        1/1     Running     0             4s
   pod/mzutd2fbabf5-console-57f94b4588-v65lk        1/1     Running     0             4s
   pod/mzutd2fbabf5-environmentd-1-0                1/1     Running     0             16s

   NAME                                               TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                                        AGE
   service/mzutd2fbabf5-balancerd                     ClusterIP   None            <none>        6876/TCP,6875/TCP                              11s
   service/mzutd2fbabf5-cluster-s2-replica-s1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   12s
   service/mzutd2fbabf5-cluster-u1-replica-u1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   12s
   service/mzutd2fbabf5-console                       ClusterIP   None            <none>        8080/TCP                                       4s
   service/mzutd2fbabf5-environmentd                  ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            11s
   service/mzutd2fbabf5-environmentd-1                ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            16s
   service/mzutd2fbabf5-persist-pubsub-1              ClusterIP   None            <none>        6879/TCP                                       16s

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mzutd2fbabf5-balancerd   1/1     1            1           11s
   deployment.apps/mzutd2fbabf5-console     2/2     2            2           4s

   NAME                                                DESIRED   CURRENT   READY      AGE
   replicaset.apps/mzutd2fbabf5-balancerd-6c9755c498   1         1         1          11s
   replicaset.apps/mzutd2fbabf5-console-57f94b4588     2         2         2          4s

   NAME                                                        READY   AGE
   statefulset.apps/mzutd2fbabf5-cluster-s2-replica-s1-gen-1   1/1     12s
   statefulset.apps/mzutd2fbabf5-cluster-u1-replica-u1-gen-1   1/1     11s
   statefulset.apps/mzutd2fbabf5-environmentd-1                1/1     16s

   NAME                          STATUS     COMPLETIONS   DURATION   AGE
   job.batch/create-db-demo-db   Complete   1/1           11s        33s
   ```

   If you run into an error during deployment, refer to the
   [Troubleshooting](/self-managed-deployments/troubleshooting/).

1. Open the Materialize Console in your browser:



   **Via Network Load Balancer:**

   Starting in v0.3.0, for each Materialize instance, Materialize on AWS
   Terraform module also deploys AWS Network Load Balancers (by default,
   internal) with the following listeners, including a listener on port 8080 for
   the Materialize Console:

   | Port | Description |
   | ---- | ------------|
   | 6875 | For SQL connections to the database |
   | 6876 | For HTTP(S) connections to the database |
   | **8080** | **For HTTP(S) connections to Materialize Console** |

   The Network Load Balancer (NLB) details are found in the `nlb_details`  in
   the [Terraform output](#aws-terrafrom-output).

   The example uses a self-signed ClusterIssuer. As such, you may encounter a
   warning with regards to the certificate. In production, run with certificates
   from an official Certificate Authority (CA) rather than self-signed
   certificates.



   **Via port forwarding:**

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
      [https://localhost:8080](https://localhost:8080) (or, if you have not enabled
      TLS, [http://localhost:8080](http://localhost:8080)).

      The example uses a self-signed ClusterIssuer. As such, you may encounter a
      warning with regards to the certificate. In production, run with certificates
      from an official Certificate Authority (CA) rather than self-signed
      certificates.

   [^1]: The port forwarding command uses a while loop to handle a [known
   Kubernetes issue 78446](https://github.com/kubernetes/kubernetes/issues/78446),
   where interrupted long-running requests through a standard port-forward cause
   the port forward to hang. The command automatically restarts the port forwarding
   if an error occurs, ensuring a more stable connection. It detects failures by
   monitoring for "portforward.go" error messages.





   > **Tip:** If you experience long loading screens or unresponsiveness in the Materialize
>    Console, we recommend increasing the size of the `mz_catalog_server` cluster.
>    Refer to the [Troubleshooting Console
>    Unresponsiveness](/self-managed-deployments/troubleshooting/#troubleshooting-console-unresponsiveness)
>    guide.
>
>
>


## Next steps


- From the Console, you can get started with the
[Quickstart](/get-started/quickstart/).

- To start ingesting your own data from an external system like Kafka, MySQL or
  PostgreSQL, see [Ingest data](/ingest-data/).


## Cleanup


To delete the whole sample infrastructure and deployment (including the
Materialize operator and Materialize instances and data), run from the Terraform
directory:

```bash
terraform destroy
```

When prompted to proceed, type `yes` to confirm the deletion.


  > **Tip:** - To delete your S3 bucket, you may need to empty the S3 bucket first. If the
>     `terraform destroy` command is unable to delete the S3 bucket and does not
>     progress beyond "Still destroying...", empty the S3 bucket first and rerun
>     the `terraform destroy` command.
>
>   - Upon successful destroy, you may receive some informational messages with
>     regards to CustomResourceDefinition(CRD). You may safely ignore these
>     messages as your whole deployment has been destroyed, including the CRDs.
>
>


## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)


---

## Install on Azure (Legacy Terraform)



Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; specifically **block** blob storage on Azure; and a license key.


The tutorial deploys Materialize to Azure Kubernetes Service (AKS) with a
PostgreSQL database as the metadata database and Azure premium block blob
storage for blob storage. The tutorial uses [Materialize on Azure Terraform
modules](https://github.com/MaterializeInc/terraform-azurerm-materialize) to:

- Set up the Azure Kubernetes environment
- Call
   [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
   module to deploy Materialize Operator and Materialize instances to that AKS
   cluster

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
>
> - Fork the repo and pin to a specific version; or
>
> - Use the code as a reference when developing your own deployment.
>
>
>
> For simplicity, this tutorial stores various secrets in a file as well as prints
> them to the terminal. In practice, refer to your organization's official
> security and Terraform/infrastructure practices.
>
>
>


## Prerequisites

### Azure subscription

If you do not have an Azure subscription to use for this tutorial, create one.

### Azure CLI

If you don't have Azure CLI installed, [install Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).

### Terraform

If you don't have Terraform installed, [install Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### kubectl

If you do not have `kubectl`, install `kubectl`.

### Python (v3.12+) and pip

If you don't have Python (v3.12 or greater) installed, install it. See
[Python.org](https://www.python.org/downloads/). If `pip` is not included with
your version of Python, install it.

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, install. For details, see to
the [Helm documentation](https://helm.sh/docs/intro/install/).

### jq (Optional)

*Optional*. `jq` is used to parse the AKS cluster name and region from the
Terraform outputs. Alternatively, you can manually specify the name and region.
If you want to use `jq` and do not have `jq` installed, install.

### License key



## A. Authenticate with Azure

1. Open a Terminal window.

1. Authenticate with Azure.

    ```bash
    az login
    ```

   The command opens a browser window to sign in to Azure. Sign in.

1. Select the subscription and tenant to use. After you have signed in, back in
   the terminal, your tenant and subscription information is displayed.

    ```none
    Retrieving tenants and subscriptions for the selection...

    [Tenant and subscription selection]

    No     Subscription name    Subscription ID                       Tenant
    -----  -------------------  ------------------------------------  ----------------
    [1]*   ...                  ...                                   ...

   The default is marked with an *; the default tenant is '<Tenant>' and
   subscription is '<Subscription Name>' (<Subscription ID>).
   ```

   Select the subscription and tenant.

1. Set `ARM_SUBSCRIPTION_ID` to the subscription ID.

    ```bash
    export ARM_SUBSCRIPTION_ID=<subscription-id>
    ```

## B. Set up Azure Kubernetes environment and install Materialize

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
>
> - Fork the repo and pin to a specific version; or
>
> - Use the code as a reference when developing your own deployment.
>
>
>




**Deployed components:**

[Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize) for
deploys a sample infrastructure on Azure with the following components:


| Component | Version |
| --- | --- |
| AKS cluster | All |
| Azure Blob Storage for persistence | All |
| Azure Database for PostgreSQL Flexible Server for metadata storage | All |
| Required networking and security configurations | All |
| Managed identities with proper RBAC permissions | All |
| Materialize Operator | All |
| Materialize instances (Deployed during subsequent runs after the Operator is running) | All |
| <code>cert-manager</code> and a self-signed <code>ClusterIssuer</code>. <code>ClusterIssuer</code> is deployed on subsequent runs after the <code>cert-manager</code> is running. | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-azure-terraform-module" >v0.3.0+</a> |
| Load balancers for each Materialize instance | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-azure-terraform-module" >v0.3.1+</a> |
| OpenEBS and NVMe instance storage to enable spill-to-disk | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-azure-terraform-module" >v0.4.0+</a> |


> **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory,
> which requires minimal user input. For details on the `examples/simple/`
> infrastructure configuration (such as the node instance type, etc.), see the
> [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-azurerm-materialize/blob/main/examples/simple/main.tf).
>
> For more configuration options, you can run the `main.tf` file at the [root of
> the
> repository](https://github.com/MaterializeInc/terraform-azurerm-materialize/)
> instead. When running with the root `main.tf`, see [Azure required
> configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-azure/).
>
>
>



**Releases:**


| Terraform version | Notable changes |
| --- | --- |
| <a href="https://github.com/MaterializeInc/terraform-azurerm-materialize/releases/tag/v0.6.4" >v0.6.4</a> | <ul> <li>Released as part of v26.0.0.</li> <li>Uses <code>terraform-helm-materialize</code> version <code>v0.1.35</code>.</li> </ul>  |





1. Open a Terminal window.



1. Fork the [Materialize's sample Terraform
   repo](https://github.com/MaterializeInc/terraform-azurerm-materialize).

1. Set `MY_ORGANIZATION` to your github organization name, substituting your
   organization's name for `<enter-your-organization>`:

   ```bash
   MY_ORGANIZATION=<enter-your-organization>
   ```


1. Clone your forked repo and checkout the `v0.8.10` tag. For example,

   - If cloning via SSH (substitute `YOUR_ORGANIZATION` with your organization's
     name):

     ```bash
     git clone --depth 1 -b v0.8.10 git@github.com:${MY_ORGANIZATION}/terraform-azurerm-materialize.git
     ```

   - If cloning via HTTPS (substitute `YOUR_ORGANIZATION` with your
     organization's name):

     ```bash
     git clone --depth 1 -b v0.8.10 https://github.com/${MY_ORGANIZATION}/terraform-azurerm-materialize.git
     ```


1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-azurerm-materialize/examples/simple
   ```

   > **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory,
>    which requires minimal user input. For details on the `examples/simple/`
>    infrastructure configuration (such as the node instance type, etc.), see the
>    [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-azurerm-materialize/blob/main/examples/simple/main.tf).
>
>    For more configuration options, you can run the `main.tf` file at the [root of
>    the
>    repository](https://github.com/MaterializeInc/terraform-azurerm-materialize/)
>    instead. When running with the root `main.tf`, see [Azure required
>    configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-azure/).
>
>
>



1. Optional. Create a virtual environment, specifying a path for the new virtual
   environment:

    ```bash
    python3 -m venv <path to the new virtual environment>

    ```

   Activate the virtual environment:
    ```bash
    source <path to the new virtual environment>/bin/activate
    ```

1. Install the required packages.

    ```bash
    pip install -r requirements.txt
    ```

1. Create a `terraform.tfvars` file (you can copy from the
   `terraform.tfvars.example` file) and specify:

   - The prefix for the resources. Prefix has a maximum of 12 characters and
     contains only alphanumeric characters and hyphens; e.g., `mydemo`.

   -  The location for the AKS cluster.

   ```bash
   prefix="enter-prefix"  //  maximum 12 characters, containing only alphanumeric characters and hyphens; e.g. mydemo
   location="eastus2"
   ```

   > **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory,
>    which requires minimal user input. For details on the `examples/simple/`
>    infrastructure configuration (such as the node instance type, etc.), see the
>    [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-azurerm-materialize/blob/main/examples/simple/main.tf).
>
>    For more configuration options, you can run the `main.tf` file at the [root of
>    the
>    repository](https://github.com/MaterializeInc/terraform-azurerm-materialize/)
>    instead. When running with the root `main.tf`, see [Azure required
>    configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-azure/).
>
>
>


1. Initialize the terraform directory.

    ```bash
    terraform init
    ```

1. Use terraform plan to review the changes to be made.

    ```bash
    terraform plan
    ```

1. If you are satisfied with the changes, apply.

    ```bash
    terraform apply
    ```

   To approve the changes and apply, enter `yes`.

   Upon successful completion, various fields and their values are output:

   ```bash
   Apply complete! Resources: 33 added, 0 changed, 0 destroyed.

   Outputs:

   aks_cluster = <sensitive>
   connection_strings = <sensitive>
   kube_config = <sensitive>
   load_balancer_details = {}
   resource_group_name = "mydemo-rg"
   ```

1. Configure `kubectl` to connect to your cluster:

   - `<cluster_name>`. Your cluster name has the form `<your prefix>-aks`; e.g.,
     `mz-simple-aks`.

   - `<resource_group_name>`, as specified in the output.

   ```bash
   az aks get-credentials --resource-group <resource_group_name> --name <cluster_name>
   ```

   Alternatively, you can use the following command to get the cluster name and
   resource group name from the Terraform output:

   ```bash
   az aks get-credentials --resource-group $(terraform output -raw resource_group_name) --name $(terraform output -json aks_cluster | jq -r '.name')
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl cluster-info
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

1. By default, the example Terraform installs the Materialize Operator and,
   starting in v0.3.0, a `cert-manager`. Verify the
   installation and check the status:


   **Materialize Operator:**

   Verify the installation and check the status:

   ```shell
   kubectl get all -n materialize
   ```

   Wait for the components to be in the `Running` state:

   ```none
   NAME                                                              READY       STATUS    RESTARTS   AGE
   pod/materialize-mydemo-materialize-operator-74d8f549d6-lkjjf      1/1         Running   0          36m

   NAME                                                         READY       UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/materialize-mydemo-materialize-operator      1/1         1            1           36m

   NAME                                                                        DESIRED   CURRENT   READY   AGE
   replicaset.apps/materialize-mydemo-materialize-operator-74d8f549d6          1         1         1       36m
   ```



   **cert-manager (Starting in version 0.3.0):**

   Verify the installation and check the status:

   ```shell
   kubectl get all -n cert-manager
   ```
   Wait for the components to be in the `Running` state:
   ```
   NAME                                           READY   STATUS    RESTARTS   AGE
   pod/cert-manager-8576d99cc8-xqxbc              1/1     Running   0          4m22s
   pod/cert-manager-cainjector-664b5878d6-wc4tz   1/1     Running   0          4m22s
   pod/cert-manager-webhook-6ddb7bd6c5-vrm2p      1/1     Running   0          4m22s

   NAME                              TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)            AGE
   service/cert-manager              ClusterIP   10.1.227.230   <none>        9402/TCP           4m22s
   service/cert-manager-cainjector   ClusterIP   10.1.222.156   <none>        9402/TCP           4m22s
   service/cert-manager-webhook      ClusterIP   10.1.84.207    <none>        443/TCP,9402/TCP   4m22s

   NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/cert-manager              1/1     1            1           4m23s
   deployment.apps/cert-manager-cainjector   1/1     1            1           4m23s
   deployment.apps/cert-manager-webhook      1/1     1            1           4m23s

   NAME                                                 DESIRED   CURRENT   READY   AGE
   replicaset.apps/cert-manager-8576d99cc8              1         1         1       4m23s
   replicaset.apps/cert-manager-cainjector-664b5878d6   1         1         1       4m23s
   replicaset.apps/cert-manager-webhook-6ddb7bd6c5      1         1         1       4m23s
   ```




   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Once the Materialize operator is deployed and running, you can deploy the
   Materialize instances. To deploy Materialize instances, create a
   `mz_instances.tfvars` file with the Materialize instance configuration.

   For example, the following specifies the configuration for a `demo` instance.

   ```bash
   cat <<EOF > mz_instances.tfvars

   materialize_instances = [
       {
         name           = "demo"
         namespace      = "materialize-environment"
         database_name  = "demo_db"
         cpu_request    = "1"
         memory_request = "2Gi"
         memory_limit   = "2Gi"
         license_key    = ""
       }
   ]
   EOF
   ```

   - **Starting in v0.3.0**, the Materialize on Azure Terraform module also
     deploys, by default, a self-signed `ClusterIssuer`. The `ClusterIssuer` is
     deployed after the `cert-manager` is deployed and running.

   - **Starting in v0.3.1**, the Materialize on Azure Terraform module also
   deploys, by default, [Load
   balancers](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_materialize_instances)
   for Materialize instances (i.e., the
   [`create_load_balancer`](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`). The load balancers, by default, are configured to
   be internal (i.e., the
   [`internal_load_balancer`](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`).

   - **Starting in v0.4.3**, you can specify addition configuration options via
     `environmentd_extra_args`.

   > **Tip:** If upgrading from a deployment that was set up using an earlier version of the
>    Terraform modules, additional considerations may apply when using an updated
>    Terraform modules to your existing deployments.
>
>
>    See [Materialize on Azure releases](/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-azure-terraform-module) for notable changes.
>


1. Run `terraform plan` with both `.tfvars` files and review the changes to be
   made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```
   Plan: 9 to add, 1 to change, 0 to destroy.
   ```

1. If you are satisfied with the changes, apply.

   ```bash
   terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   To approve the changes and apply, enter `yes`.

   <a name="azure-terraform-output"></a>

   Upon successful completion, you should see output with a summary similar to the following:

   ```bash
   Apply complete! Resources: 9 added, 1 changed, 0 destroyed.

   Outputs:

   aks_cluster = <sensitive>
   connection_strings = <sensitive>
   kube_config = <sensitive>
   load_balancer_details = {
      "demo" = {
         "balancerd_load_balancer_ip" = "192.0.2.10"
         "console_load_balancer_ip" = "192.0.2.254"
      }
   }
   resource_group_name = "mydemo-rg"
   ```

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be ready and in the `Running` state.

   ```none
   NAME                                             READY   STATUS      RESTARTS   AGE
   pod/db-demo-db-l6ss8                             0/1     Completed   0          2m21s
   pod/mz62lr3yltj8-balancerd-6d5dd6d4cf-r9nf4      1/1     Running     0          111s
   pod/mz62lr3yltj8-cluster-s2-replica-s1-gen-1-0   1/1     Running     0          114s
   pod/mz62lr3yltj8-cluster-u1-replica-u1-gen-1-0   1/1     Running     0          114s
   pod/mz62lr3yltj8-console-bfc797745-6nlwv         1/1     Running     0          96s
   pod/mz62lr3yltj8-console-bfc797745-tk9vm         1/1     Running     0          96s
   pod/mz62lr3yltj8-environmentd-1-0                1/1     Running     0          2m4s

   NAME                                               TYPE           CLUSTER-IP     EXTERNAL-IP       PORT(S)                                        AGE
   service/mz62lr3yltj8-balancerd                     ClusterIP      None           <none>            6876/TCP,6875/TCP                              111s
   service/mz62lr3yltj8-balancerd-lb                  LoadBalancer   10.1.201.77    192.0.2.10        6875:30890/TCP,6876:31750/TCP                  2m4s
   service/mz62lr3yltj8-cluster-s2-replica-s1-gen-1   ClusterIP      None           <none>            2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   114s
   service/mz62lr3yltj8-cluster-u1-replica-u1-gen-1   ClusterIP      None           <none>            2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   114s
   service/mz62lr3yltj8-console                       ClusterIP      None           <none>            8080/TCP                                       96s
   service/mz62lr3yltj8-console-lb                    LoadBalancer   10.1.130.212   192.0.2.254       8080:30379/TCP                                 2m4s
   service/mz62lr3yltj8-environmentd                  ClusterIP      None           <none>            6875/TCP,6876/TCP,6877/TCP,6878/TCP            111s
   service/mz62lr3yltj8-environmentd-1                ClusterIP      None           <none>            6875/TCP,6876/TCP,6877/TCP,6878/TCP            2m5s
   service/mz62lr3yltj8-persist-pubsub-1              ClusterIP      None           <none>            6879/TCP                                       2m4s

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mz62lr3yltj8-balancerd   1/1     1            1           111s
   deployment.apps/mz62lr3yltj8-console     2/2     2            2           96s

   NAME                                                DESIRED   CURRENT   READY   AGE
   replicaset.apps/mz62lr3yltj8-balancerd-6d5dd6d4cf   1         1         1       111s
   replicaset.apps/mz62lr3yltj8-console-bfc797745      2         2         2       96s

   NAME                                                        READY   AGE
   statefulset.apps/mz62lr3yltj8-cluster-s2-replica-s1-gen-1   1/1     114s
   statefulset.apps/mz62lr3yltj8-cluster-u1-replica-u1-gen-1   1/1     114s
   statefulset.apps/mz62lr3yltj8-environmentd-1                1/1     2m4s

   NAME                   STATUS     COMPLETIONS   DURATION   AGE
   job.batch/db-demo-db   Complete   1/1           10s        2m21s

   ```

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Open the Materialize Console in your browser:




   **Via Network Load Balancer:**

   Starting in v0.3.1, for each Materialize instance, Materialize on Azure
   Terraform module also deploys load balancers (by default, internal) with the
   following listeners, including a listener on port 8080 for the Materialize
   Console:

   | Port | Description |
   | ---- | ------------|
   | 6875 | For SQL connections to the database |
   | 6876 | For HTTP(S) connections to the database |
   | **8080** | **For HTTP(S) connections to Materialize Console** |

   The load balancer details are found in the `load_balancer_details`  in
   the [Terraform output](#azure-terraform-output).

   The example uses a self-signed ClusterIssuer. As such, you may encounter a
   warning with regards to the certificate. In production, run with certificates
   from an official Certificate Authority (CA) rather than self-signed
   certificates.



   **Via port forwarding:**

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
      [https://localhost:8080](https://localhost:8080) (or, if you have not enabled
      TLS, [http://localhost:8080](http://localhost:8080)).

      The example uses a self-signed ClusterIssuer. As such, you may encounter a
      warning with regards to the certificate. In production, run with certificates
      from an official Certificate Authority (CA) rather than self-signed
      certificates.

   [^1]: The port forwarding command uses a while loop to handle a [known
   Kubernetes issue 78446](https://github.com/kubernetes/kubernetes/issues/78446),
   where interrupted long-running requests through a standard port-forward cause
   the port forward to hang. The command automatically restarts the port forwarding
   if an error occurs, ensuring a more stable connection. It detects failures by
   monitoring for "portforward.go" error messages.





   > **Tip:** If you experience long loading screens or unresponsiveness in the Materialize
>    Console, we recommend increasing the size of the `mz_catalog_server` cluster.
>    Refer to the [Troubleshooting Console
>    Unresponsiveness](/self-managed-deployments/troubleshooting/#troubleshooting-console-unresponsiveness)
>    guide.
>
>
>


## Next steps


- From the Console, you can get started with the
[Quickstart](/get-started/quickstart/).

- To start ingesting your own data from an external system like Kafka, MySQL or
  PostgreSQL, see [Ingest data](/ingest-data/).


## Cleanup


To delete the whole sample infrastructure and deployment (including the
Materialize operator and Materialize instances and data), run from the Terraform
directory:

```bash
terraform destroy
```

When prompted to proceed, type `yes` to confirm the deletion.


  > **Tip:** If the `terraform destroy` command is unable to delete the subnet because it
>   is in use, you can rerun the `terraform destroy` command.
>
>


## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Appendix: Azure deployment guidelines](/installation/install-on-azure/
  appendix-deployment-guidelines)
- [Installation](/installation/)


---

## Install on GCP (Legacy Terraform)


Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; and a license key.


This tutorial deploys Materialize to GCP Google Kubernetes Engine (GKE) cluster
with a Cloud SQL PostgreSQL database as the metadata database and Cloud Storage
bucket for blob storage. Specifically, the tutorial uses [Materialize on Google
Cloud Provider Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) to:

- Set up the GCP environment.

- Call
   [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
   module to deploy Materialize Operator and Materialize instances to the GKE
   cluster.

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
>
> - Fork the repo and pin to a specific version; or
>
> - Use the code as a reference when developing your own deployment.
>
>
>
> For simplicity, this tutorial stores various secrets in a file as well as prints
> them to the terminal. In practice, refer to your organization's official
> security and Terraform/infrastructure practices.
>
>
>


## Prerequisites

### Google cloud provider project

You need a GCP project for which you have a role (such as
`roles/resourcemanager.projectIamAdmin` or `roles/owner`) that includes [
permissions to manage access to the
project](https://cloud.google.com/iam/docs/granting-changing-revoking-access).

### gcloud CLI

If you do not have gcloud CLI, install. For details, see the [Install the gcloud
CLI documentation](https://cloud.google.com/sdk/docs/install).

### Google service account

The tutorial assumes the use of a service account. If you do not have a service
account to use for this tutorial, create a service account. For details, see
[Create service
accounts](https://cloud.google.com/iam/docs/service-accounts-create#creating).

### Terraform

If you do not have Terraform installed, [install
Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### kubectl and plugins

> **Tip:** Using `gcloud` to install `kubectl` will also install the needed plugins.
> Otherwise, you will need to manually install the `gke-gcloud-auth-plugin` for
> `kubectl`.
>
>


- If you do not have `kubectl`, install `kubectl`.  To install, see [Install
  kubectl and configure cluster
  access](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl)
  for details. You will configure `kubectl` to interact with your GKE cluster
  later in the tutorial.

- If you do not have `gke-gcloud-auth-plugin` for `kubectl`, install the
  `gke-gcloud-auth-plugin`. For details, see [Install the
  gke-gcloud-auth-plugin](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#install_plugin).

### Helm 3.2.0+

If you do not have Helm version 3.2.0+ installed, install.  For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### jq (Optional)

*Optional*. `jq` is used to parse the GKE cluster name and region from the
Terraform outputs. Alternatively, you can manually specify the name and region.
If you want to use `jq` and do not have `jq` installed, install.

### License key



## A. Configure GCP project and service account

1. Open a Terminal window.

1. Initialize the gcloud CLI (`gcloud init`) to specify the GCP project you want
   to use. For details, see the [Initializing the gcloud CLI
   documentation](https://cloud.google.com/sdk/docs/initializing#initialize_the).

   > **Tip:** You do not need to configure a default Compute Region and Zone as you will
>    specify the region.
>


1. Enable the following services for your GCP project, if not already enabled:

   ```bash
   gcloud services enable container.googleapis.com        # For creating Kubernetes clusters
   gcloud services enable sqladmin.googleapis.com         # For creating databases
   gcloud services enable cloudresourcemanager.googleapis.com # For managing GCP resources
   gcloud services enable servicenetworking.googleapis.com  # For private network connections
   gcloud services enable iamcredentials.googleapis.com     # For security and authentication
   ```

1. To the service account that will run the Terraform script,
   grant the following IAM roles:

   - `roles/editor`
   - `roles/iam.serviceAccountAdmin`
   - `roles/servicenetworking.networksAdmin`
   - `roles/storage.admin`
   - `roles/container.admin`

   1. Enter your GCP project ID.

      ```bash
      read -s PROJECT_ID
      ```

   1. Find your service account email for your GCP project

      ```bash
      gcloud iam service-accounts list --project $PROJECT_ID
      ```

   1. Enter your service account email.

      ```bash
      read -s SERVICE_ACCOUNT
      ```

   1. Grant the service account the neccessary IAM roles.

      ```bash
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

1. For the service account, authenticate to allow Terraform to
   interact with your GCP project. For details, see [Terraform: Google Cloud
   Provider Configuration
   reference](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#authentication).

   For example, if using [User Application Default
   Credentials](https://cloud.google.com/sdk/gcloud/reference/auth/application-default),
   you can run the following command:

   ```bash
   gcloud auth application-default login
   ```

   > **Tip:** If using `GOOGLE_APPLICATION_CREDENTIALS`, use absolute path to your key file.
>


## B. Set up GCP Kubernetes environment and install Materialize

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
>
> - Fork the repo and pin to a specific version; or
>
> - Use the code as a reference when developing your own deployment.
>
>
>




**Deployed components:**
[Materialize on GCP Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) deploys
a sample infrastructure on GCP (region `us-central1`) with the following
components:


| Component | Version |
| --- | --- |
| Google Kubernetes Engine (GKE) cluster | All |
| Cloud Storage bucket for blob storage | All |
| Cloud SQL PostgreSQL database for metadata storage | All |
| Dedicated VPC | All |
| Service accounts with proper IAM permissions | All |
| Materialize Operator | All |
| Materialize instances (Deployed during subsequent runs after the Operator is running) | All |
| Load balancers for each Materialize instance | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-gcp-terraform-module" >v0.3.0+</a> |
| <code>cert-manager</code> and a self-signed <code>ClusterIssuer</code>. <code>ClusterIssuer</code> is deployed on subsequent runs after the <code>cert-manager</code> is running. | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-gcp-terraform-module" >v0.3.0+</a> |
| OpenEBS and NVMe instance storage to enable spill-to-disk | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-gcp-terraform-module" >v0.4.0+</a> |


> **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory, which
> requires minimal user input. For details on the  `examples/simple/`
> infrastructure configuration (such as the node instance type, etc.), see the
> [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/examples/simple/main.tf).
>
> For more configuration options, you can use the `main.tf` file at the [root of
> the repository](https://github.com/MaterializeInc/terraform-google-materialize/)
> instead. When running with the root `main.tf`, see [GCP required
> configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-gcp/).
>
>


**Releases:**


| Terraform version | Notable changes |
| --- | --- |
| <a href="https://github.com/MaterializeInc/terraform-google-materialize/releases/tag/v0.6.4" >v0.6.4</a> | <ul> <li>Released as part of v26.0.0.</li> <li>Uses <code>terraform-helm-materialize</code> version <code>v0.1.35</code>.</li> </ul>  |







1. Fork the [Materialize's sample Terraform
   repo](https://github.com/MaterializeInc/terraform-google-materialize).

1. Set `MY_ORGANIZATION` to your github organization name, substituting your
   organization's name for `<enter-your-organization>`:

   ```bash
   MY_ORGANIZATION=<enter-your-organization>
   ```

1. Clone your forked repo and checkout the `v0.8.11` tag. For example,

   - If cloning via SSH:

     ```bash
     git clone --depth 1 -b v0.8.11 git@github.com:${MY_ORGANIZATION}/terraform-google-materialize.git
     ```

   - If cloning via HTTPS:

     ```bash
     git clone --depth 1 -b v0.8.11 https://github.com/${MY_ORGANIZATION}/terraform-google-materialize.git
     ```


1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-google-materialize/examples/simple
   ```

   > **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory, which
>    requires minimal user input. For details on the  `examples/simple/`
>    infrastructure configuration (such as the node instance type, etc.), see the
>    [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/examples/simple/main.tf).
>
>    For more configuration options, you can use the `main.tf` file at the [root of
>    the repository](https://github.com/MaterializeInc/terraform-google-materialize/)
>    instead. When running with the root `main.tf`, see [GCP required
>    configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-gcp/).
>
>


1. Create a `terraform.tfvars` file (you can copy from the
   `terraform.tfvars.example` file) and specify the following variables:

   | **Variable** | **Description** |
   |--------------|-----------------|
   | `project_id` | Your GCP project ID. |
   | `prefix`     | A prefix (e.g., `mz-simple`) for your resources. Prefix has a maximum of 15 characters and contains only alphanumeric characters and dashes. |
   | `region`     | The region for the GKE cluster. |

   ```bash
   project_id = "enter-your-gcp-project-id"
   prefix  = "enter-your-prefix" //  Maximum of 15 characters, contain lowercase alphanumeric and hyphens only (e.g., mz-simple)
   region = "us-central1"
   ```

   > **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory, which
>    requires minimal user input. For details on the  `examples/simple/`
>    infrastructure configuration (such as the node instance type, etc.), see the
>    [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/examples/simple/main.tf).
>
>    For more configuration options, you can use the `main.tf` file at the [root of
>    the repository](https://github.com/MaterializeInc/terraform-google-materialize/)
>    instead. When running with the root `main.tf`, see [GCP required
>    configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-gcp/).
>
>
>


1. Initialize the terraform directory.

    ```bash
    terraform init
    ```

1. Run terraform plan and review the changes to be made.

    ```bash
    terraform plan
    ```

1. If you are satisfied with the changes, apply.

    ```bash
    terraform apply
    ```

   To approve the changes and apply, enter `yes`.

   Upon successful completion, various fields and their values are output:

   ```bash
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

1. Configure `kubectl` to connect to your GKE cluster, specifying:

   - `<cluster name>`. Your cluster name has the form `<your prefix>-gke`; e.g.,
     `mz-simple-gke`.

   - `<region>`. By default, the example Terraform module uses the `us-central1`
     region.

   - `<project>`. Your GCP project ID.

   ```bash
   gcloud container clusters get-credentials <cluster-name>  \
    --region <region> \
    --project <project>
   ```

   Alternatively, you can use the following command to get the cluster name and
   region from the Terraform output and the project ID from the environment
   variable set earlier.

   ```bash
   gcloud container clusters get-credentials $(terraform output -json gke_cluster | jq -r .name) \
    --region $(terraform output -json gke_cluster | jq -r .location) --project $PROJECT_ID
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl cluster-info
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

1. By default, the example Terraform installs the Materialize Operator and,
   starting in v0.3.0, a `cert-manager`. Verify the
   installation and check the status:


   **Materialize Operator:**

   Verify the installation and check the status:

   ```shell
   kubectl get all -n materialize
   ```

   Wait for the components to be in the `Running` state:

   ```none
   NAME                                                              READY       STATUS    RESTARTS   AGE
   pod/materialize-mz-simple-materialize-operator-74d8f549d6-lkjjf   1/1         Running   0          36m

   NAME                                                         READY       UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/materialize-mz-simple-materialize-operator   1/1         1            1           36m

   NAME                                                                        DESIRED   CURRENT   READY   AGE
   replicaset.apps/materialize-mz-simple-materialize-operator-74d8f549d6       1         1         1       36m
   ```


   **cert-manager (Starting in version 0.3.0):**

   Verify the installation and check the status:

   ```shell
   kubectl get all -n cert-manager
   ```
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




   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Once the Materialize operator is deployed and running, you can deploy the
   Materialize instances. To deploy Materialize instances, create a
   `mz_instances.tfvars` file with the Materialize instance configuration.

   For example, the following specifies the configuration for a `demo` instance.

   ```bash
   cat <<EOF > mz_instances.tfvars

   materialize_instances = [
       {
         name           = "demo"
         namespace      = "materialize-environment"
         database_name  = "demo_db"
         cpu_request    = "1"
         memory_request = "2Gi"
         memory_limit   = "2Gi"
         license_key    = ""
       }
   ]
   EOF
   ```

   - **Starting in v0.3.0**, the Materialize on GCP Terraform module also
     deploys, by default:

     - [Load balancers](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) for Materialize instances (i.e., the [`create_load_balancer`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) flag defaults to `true`). The load balancers, by default, are configured  to be internal (i.e., the [`internal_load_balancer`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) flag defaults to `true`).

     - A self-signed `ClusterIssuer`. The `ClusterIssuer` is deployed  after the
     `cert-manager` is deployed and running.

   - **Starting in v0.4.3**, you can specify addition configuration options via
     `environmentd_extra_args`.

   > **Tip:** If upgrading from a deployment that was set up using an earlier version of the
>    Terraform modules, additional considerations may apply when using an updated
>    Terraform modules to your existing deployments.
>
>
>    See [Materialize on GCP releases](/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-gcp-terraform-module) for notable changes.
>


1. Run `terraform plan` with both `.tfvars` files and review the changes to be
   made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```
   Plan: 9 to add, 1 to change, 0 to destroy.
   ```

1. If you are satisfied with the changes, apply.

    ```bash
    terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
    ```

   To approve the changes and apply, enter `yes`.

   <a name="gcp-terraform-output"></a>
   Upon successful completion, you should see output with a summary similar to the following:

   ```bash
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

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be in the `Running` state.

   ```none
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
   [Troubleshooting](/installation/troubleshooting/).

1. Open the Materialize Console in your browser:



   **Via Network Load Balancer:**

   Starting in v0.3.0, for each Materialize instance, Materialize on GCP
   Terraform module also deploys load balancers (by default, internal) with the
   following listeners, including a listener on port 8080 for the Materialize
   Console:

   | Port | Description |
   | ---- | ------------|
   | 6875 | For SQL connections to the database |
   | 6876 | For HTTP(S) connections to the database |
   | **8080** | **For HTTP(S) connections to Materialize Console** |

   The load balancer details are found in the `load_balancer_details`  in
   the [Terraform output](#gcp-terraform-output).

   The example uses a self-signed ClusterIssuer. As such, you may encounter a
   warning with regards to the certificate. In production, run with certificates
   from an official Certificate Authority (CA) rather than self-signed
   certificates.



   **Via port forwarding:**

   1. Find your console service name.

      ```shell
      MZ_SVC_CONSOLE=$(kubectl -n materialize-environment get svc \
        -o custom-columns="NAME:.metadata.name" --no-headers | grep console-lb)
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
      [https://localhost:8080](https://localhost:8080) (or, if you have not enabled
      TLS, [http://localhost:8080](http://localhost:8080)).

      The example uses a self-signed ClusterIssuer. As such, you may encounter a
      warning with regards to the certificate. In production, run with certificates
      from an official Certificate Authority (CA) rather than self-signed
      certificates.

   [^1]: The port forwarding command uses a while loop to handle a [known
   Kubernetes issue 78446](https://github.com/kubernetes/kubernetes/issues/78446),
   where interrupted long-running requests through a standard port-forward cause
   the port forward to hang. The command automatically restarts the port forwarding
   if an error occurs, ensuring a more stable connection. It detects failures by
   monitoring for "portforward.go" error messages.






   > **Tip:** If you experience long loading screens or unresponsiveness in the Materialize
>    Console, we recommend increasing the size of the `mz_catalog_server` cluster.
>    Refer to the [Troubleshooting Console
>    Unresponsiveness](/self-managed-deployments/troubleshooting/#troubleshooting-console-unresponsiveness)
>    guide.
>
>
>


## Next steps


- From the Console, you can get started with the
[Quickstart](/get-started/quickstart/).

- To start ingesting your own data from an external system like Kafka, MySQL or
  PostgreSQL, see [Ingest data](/ingest-data/).


## Cleanup


To delete the whole sample infrastructure and deployment (including the
Materialize operator and Materialize instances and data), run from the Terraform
directory:

```bash
terraform destroy
```

When prompted to proceed, type `yes` to confirm the deletion.


## See also

- [Troubleshooting](/self-managed-deployments/troubleshooting/)
- [Materialize Operator Configuration](/installation/configuration/)
