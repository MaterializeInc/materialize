---
title: "Install on AWS"
description: ""
aliases:
  - /self-hosted/install-on-aws/
disable_list: true
menu:
  main:
    parent: "installation"
    identifier: "install-on-aws"
    weight: 20
---

{{% self-managed/materialize-components-sentence %}}

The tutorial deploys Materialize to AWS Elastic Kubernetes Service (EKS) with a
PostgreSQL RDS database as the metadata database and AWS S3 for blob storage.
The tutorial uses [Materialize on AWS Terraform
modules](https://github.com/MaterializeInc/terraform-aws-materialize) to:

- Set up the AWS Kubernetes environment.
- Call
   [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
   module to deploy Materialize Operator and Materialize instances to that EKS
   cluster.

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

{{% self-managed/aws-recommended-instances %}}

See [Operational guidelines](/installation/operational-guidelines/) for more
information.

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

## Set up AWS Kubernetes environment and install Materialize

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< self-managed/tutorial-disclaimer >}}

{{< /warning >}}

Materialize provides [Materialize on AWS Terraform
modules](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/README.md)
for evaluation purposes only. The modules deploy a sample infrastructure on AWS
(region `us-east-1`) with the following components:

- A Kuberneted (EKS) cluster
- A dedicated VPC
- An S3 for blob storage
- An RDS PostgreSQL cluster and database for metadata storage
- Materialize Operator
- Materialize instances (during subsequent runs after the Operator is running)

- **Starting in v0.3.0 of Materialize on AWS Terraform**, AWS Load Balancer
  Controller and AWS Network Load Balancers (NLBs) for each
  Materialize instance. If your deployment is set up using an earlier version of
  the Materialize AWS Terraform module, and you wish to upgrade to v0.3.0
  Terraform modules, see the [Upgrade
  Notes](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/README.md#upgrade-notes).

- **Starting in v0.3.1 of Materialize on AWS Terraform**, OpenEBS and NVMe
  instance storage to [enable
  spill-to-disk](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#enabling-disk-support)
  to support workloads that are larger than can fit into memory.

{{< tip >}}
{{< self-managed/aws-terraform-configs >}}
{{< /tip >}}

1. Open a Terminal window.

1. Configure AWS CLI with your AWS credentials. For details, see the [AWS
   documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

{{% self-managed/versions/step-clone-aws-terraform-repo %}}

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-aws-materialize/examples/simple
   ```

   {{< tip >}}
   {{< self-managed/aws-terraform-configs >}}
   {{< /tip >}}

1. Create a `terraform.tfvars` file (you can copy from the
   `terraform.tfvars.example` file) and specify the following variables:

   {{< tabs >}}
   {{< tab "Without cert-manager" >}}

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

   {{</ tab >}}

   {{< tab "With cert-manager" >}}

   | Variable          | Description |
   |--------------------|-------------|
   | `namespace`       | A namespace (e.g., `my-demo`) that will be used to form part of the prefix for your AWS resources. <br> **Requirements:** <br> - Maximum of 12 characters <br> - Must start with a lowercase letter <br> - Must be lowercase alphanumeric and hyphens only |
   | `environment`     | An environment name (e.g., `dev`, `test`) that will be used to form part of the prefix for your AWS resources. <br> **Requirements:** <br> - Maximum of 8 characters <br> - Must be lowercase alphanumeric only |
   | `install_cert_manager` | A boolean that specifies whether to install cert-manager. Available starting in [Materialize on AWS Terraform version v0.3.2](/installation/appendix-terraforms#materialize-on-aws-terraform-modules). |

   ```bash
   # The namespace and environment variables are used to construct the names of the resources
   # e.g. ${namespace}-${environment}-storage, ${namespace}-${environment}-db etc.

   namespace = "enter-namespace"   // maximum 12 characters, start with a letter, contain lowercase alphanumeric and hyphens only (e.g. my-demo)
   environment = "enter-environment" // maximum 8 characters, lowercase alphanumeric only (e.g., dev, test)

   install_cert_manager = true    // Install cert-manager
   # use_self_signed_cluster_issuer = true // Uncomment, after cert-manager is installed and running, to use self-signed ClusterIssuer
   ```

   {{</ tab >}}
   {{</ tabs >}}
   {{< tip >}}
   {{< self-managed/aws-terraform-configs >}}
   {{< /tip >}}

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

   {{< tabs >}}
   {{< tab "Without cert-manager" >}}

   Upon successful completion, various fields and their values are output:

   ```none
   Apply complete! Resources: 87 added, 0 changed, 0 destroyed.

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
   {{</ tab >}}
   {{< tab "With cert-manager" >}}

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
   {{</ tab >}}
   {{</ tabs >}}

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

1. By default, the example Terraform installs the Materialize Operator.

   {{< tabs >}}
   {{< tab "Without cert-manager" >}}
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

   NAME                                                              DESIRED   CURRENT   READY   AGE
   replicaset.apps/my-demo-dev-materialize-operator-84ff4b4648   1        1         1       12s
   ```
   {{</ tab >}}
   {{< tab "With cert-manager" >}}

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

   NAME                                                              DESIRED   CURRENT   READY   AGE
   replicaset.apps/my-demo-dev-materialize-operator-84ff4b4648   1        1         1       12s
   ```

   If you opted to install `cert-manager`, verify its installation and checkhe
   tatus.

   ```shell
   kubectl get all -n cert-manager
   ```

   Wait for the components to be in the `Running` state:

   ```
   NAME                                           READY   STATUS    RESTARTS   AGE
   pod/cert-manager-cainjector-686546c9f7-v9hwp   1/1     Running   0          4m20s
   pod/cert-manager-d6746cf45-cdmb5               1/1     Running   0          4m20s
   pod/cert-manager-webhook-5f79cd6f4b-rcjbq      1/1     Running   0          4m20s

   NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)            AGE
   service/cert-manager              ClusterIP   172.20.2.136     <none>        9402/TCP           4m20s
   service/cert-manager-cainjector   ClusterIP   172.20.154.137   <none>        9402/TCP           4m20s
   service/cert-manager-webhook      ClusterIP   172.20.63.217    <none>        443/TCP,9402/TCP   4m20s

   NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/cert-manager              1/1     1            1           4m20s
   deployment.apps/cert-manager-cainjector   1/1     1            1           4m20s
   deployment.apps/cert-manager-webhook      1/1     1            1           4m20s

   NAME                                                 DESIRED   CURRENT   READY   AGE
   replicaset.apps/cert-manager-cainjector-686546c9f7   1         1         1       4m20s
   replicaset.apps/cert-manager-d6746cf45               1         1         1       4m20s
   replicaset.apps/cert-manager-webhook-5f79cd6f4b      1         1         1
   4m20s
   ```
   {{</ tab >}}
   {{</ tabs >}}

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting) guide.


1. Optional. If you have installed `cert-manager` successfully, and would like
   to use self-signed
   [ClusterIssuer](https://cert-manager.io/docs/concepts/issuer/):


   1. Uncomment the `use_self_signed_cluster_issuer` variable in your
      `terraform.tfvars` file.

      ```bash
      # The namespace and environment variables are used to construct the names of    the resources
      # e.g. ${namespace}-${environment}-storage, ${namespace}-${environment}-db    etc.

      namespace = "enter-namespace"   // maximum 12 characters, start with a    letter, contain lowercase alphanumeric and hyphens only (e.g. my-demo)
      environment = "enter-environment" // maximum 8 characters, lowercase    alphanumeric only (e.g., dev, test)

      install_cert_manager = true    // Install cert-manager
      use_self_signed_cluster_issuer = true // Uncomment, after cert-manager is    installed and running, to use self-signed ClusterIssuer
      ```

   1. Use terraform plan to review the changes to be made.

      ```bash
      terraform plan
      ```

   1. If you are satisfied with the changes, apply.

      ```bash
      terraform apply
      ```

      To approve the changes and apply, enter yes.

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
       }
   ]
   EOF
   ```

   Starting in v0.3.0, the Materialize on AWS Terraform module also deploys, by
   default, Network Load Balancers (NLBs) for each Materialize instance (i.e.,
   the
   [`create_nlb`](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`).  The NLBs, by default, are configured to be
    internal (i.e., the
    [`internal_nlb`](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`). See [`materialize_instances`](
   https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances)
   for the Materialize instance configuration options.

   {{< tip >}}
   {{% self-managed/aws-terraform-upgrade-notes %}}

   See [Materialize on AWS releases](/installation/appendix-terraforms/#materialize-on-aws-terraform-modules) for notable changes.
   {{</ tip >}}

1. Run `terraform plan` with both `.tfvars` files and review the changes to be
   made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```
   Plan: 14 to add, 0 to change, 0 to destroy.
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
   Apply complete! Resources: 14 added, 0 changed, 0 destroyed.

   Outputs:

   database_endpoint = "my-demo-dev-db.abcdefg8dsto.us-east-1.rds.amazonaws.com:5432"
   eks_cluster_endpoint = "https://0123456789A00BCD000E11BE12345A01.gr7.us-east-1.eks.amazonaws.com"
   eks_cluster_name = "my-demo-dev-eks"
   materialize_s3_role_arn = "arn:aws:iam::000111222333:role/my-demo-dev-mz-role"
   metadata_backend_url = <sensitive>
   nlb_details = [
     {
       "arn" = "arn:aws:elasticloadbalancing:us-east-1:400121260767:loadbalancer/net/demo/aeae3d936afebcfe"
       "dns_name" = "demo-aeae3d936afebcfe.elb.us-east-1.amazonaws.com"
     },
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
   [Troubleshooting](/installation/troubleshooting/).

1. Open the Materialize Console in your browser:

   {{< tabs >}}

   {{< tab  "Via Network Load Balancer" >}}

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

   {{</ tab >}}

   {{< tab "Via port forwarding" >}}

1. Find your console service name.

   ```shell
   MZ_SVC_CONSOLE=$(kubectl -n materialize-environment get svc \
     -o custom-columns="NAME:.metadata.name" --no-headers | grep console)
   echo $MZ_SVC_CONSOLE
   ```

2. Port forward the Materialize Console service to your local machine:[^1]

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

1. Open a browser and navigate to either
   [http://localhost:8080](http://localhost:8080), or, if you have enabled TLS,
   [https://localhost:8080](https://localhost:8080).

[^1]: The port forwarding command uses a while loop to handle a [known
Kubernetes issue 78446](https://github.com/kubernetes/kubernetes/issues/78446),
where interrupted long-running requests through a standard port-forward cause
the port forward to hang. The command automatically restarts the port forwarding
if an error occurs, ensuring a more stable connection. It detects failures by
monitoring for "portforward.go" error messages.

   {{</ tab>}}
   {{</ tabs >}}

   {{< tip >}}

   {{% self-managed/troubleshoot-console-mz_catalog_server_blurb %}}

   {{< /tip >}}

## Next steps

{{% self-managed/next-steps %}}

## Cleanup

{{% self-managed/cleanup-cloud %}}

  {{< tip >}}

  - To delete your S3 bucket, you may need to empty the S3 bucket first. If the
    `terraform destroy` command is unable to delete the S3 bucket and does not
    progress beyond "Still destroying...", empty the S3 bucket first and rerun
    the `terraform destroy` command.

  - Upon successful destroy, you may receive some informational messages with
    regards to CustomResourceDefinition(CRD). You may safely ignore these
    messages as your whole deployment has been destroyed, including the CRDs.

  {{</ tip >}}

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Installation](/installation/)
