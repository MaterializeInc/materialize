---
title: "Install on AWS"
description: ""
aliases:
  - /self-hosted/install-on-aws/
menu:
  main:
    parent: "installation"
---

Self-managed Materialize requires:

{{% self-managed/materialize-components-list %}}

The tutorial deploys Materialize to AWS Elastic Kubernetes Service (EKS) with a
PostgreSQL RDS database as the metadata database and AWS S3 for blob storage.

## Prerequisites

### Terraform

If you don't have Terraform installed, [install
Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### AWS CLI

If you do not have the AWS CLI installed,

- Install the AWS CLI. For details, see the [AWS
  documentation](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).

- Configure with your AWS credentials. For details, see the [AWS
  documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

### kubectl

If you do not have `kubectl`, install. See the [Amazon EKS: install `kubectl`
documentation](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html)
for details.

### Helm 3.2.0+

If you do not have Helm 3.2.0+, install. See the [Helm
documentation](https://helm.sh/docs/intro/install/).

### AWS Kubernetes environment

{{% self-managed/materialize-components-list %}}

When operating in AWS, we recommend:

- Using the `r8g`, `r7g`, and `r6g` families when running without local disk.

- Using the `r7gd` and `r6gd` families of instances (and `r8gd` once available)
  when running with local disk (Recommended for production.  See [Operational guidelines](/installation/operational-guidelines/#locally-attached-nvme-storage-openebs) for more information.)

This tutorial uses Terraform to set up the AWS Kubernetes environment and
install Materialize. See [Set up AWS Kubernetes environment and install
Materialize](#set-up-aws-kubernetes-environment-and-install-materialize) for
details.

## Set up AWS Kubernetes environment and install Materialize

Materialize provides a [sample Terraform
module](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/README.md)
for evaluation purposes only. The module deploys a sample infrastructure on AWS
(region `us-east-1`) with the following components:

- A Kuberneted (EKS) cluster
- A dedicated VPC
- An S3 for blob storage
- An RDS PostgreSQL cluster and database for metadata storage
- Materialize Operator
- Materialize instances (during subsequent runs after the Operator is running)

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

1. Clone or download the [Materialize's sample Terraform
   repo](https://github.com/MaterializeInc/terraform-aws-materialize).

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-aws-materialize/examples/simple
   ```

1. Create a `terraform.tfvars` file and and specify:

   - A namespace that will be used as the prefix for your AWS resources (e.g.,
      `my-materialize`). Namespace has a maximum of 18 characters and must
      be lowercase alphanumeric and hyphens only.

   - A secure password for the RDS PostgreSQL database (to be created).

   ```bash
   namespace = "enter-namespace"   // 18 characters, lowercase alphanumeric and hyphens only (e.g. my-materialize)
   database_password  = "enter-secure-password"
   ```

1. Initialize the terraform directory.

    ```bash
    terraform init
    ```

1. Create a terraform plan and review the changes.

    ```bash
    terraform plan -out my-plan.tfplan
    ```

1. If you are satisfied with the changes, apply the terraform plan.

    ```bash
    terraform apply my-plan.tfplan
    ```

   <a name="terraform-output"></a>
   Upon successful completion, various fields and their values are output:

   ```bash
   Apply complete! Resources: 76 added, 0 changed, 0 destroyed.

   Outputs:

   database_endpoint = "my-materialize-dev-db.abcdefg8dsto.us-east-1.rds.amazonaws.com:5432"
   eks_cluster_endpoint = "https://0123456789A00BCD000E11BE12345A01.gr7.us-east-1.eks.amazonaws.com"
   eks_cluster_name = "my-materialize-dev-eks"
   materialize_s3_role_arn = "arn:aws:iam::000111222333:role/my-materialize-dev-mz-role"
   metadata_backend_url = <sensitive>
   oidc_provider_arn = "arn:aws:iam::000111222333:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/7D14BCA3A7AA896A836782D96A24F958"
   persist_backend_url = "s3://my-materialize-dev-storage-f2def2a9/dev:serviceaccount:materialize-environment:12345678-1234-1234-1234-12345678912"
   s3_bucket_name = "my-materialize-dev-storage-f2def2a9"
   vpc_id = "vpc-0abc000bed1d111bd"
   ```

1. Note your specific values for the following fields:

   - `eks_cluster_name` (Used to configure `kubectl`)

1. Configure `kubectl` to connect to your EKS cluster, replacing:

   - `<your-cluster-name>` with the name of your EKS cluster (specified in
     [Terraform output](#terraform-output))

   - `<your-region>` with the region of your EKS cluster. By default, the
     sample Terraform module uses `us-east-1`.

   ```bash
   aws eks update-kubeconfig --name <your-cluster-name> --region <your-region>
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

1. By default, the example Terraform installs the Materialize Operator. Verify
   the installation and check the status:

    ```shell
    kubectl get all -n materialize
    ```

    Wait for the components to be in the `Running` state:

    ```none
    NAME                                        READY   STATUS    RESTARTS   AGE
    pod/materialize-operator-84ff4b4648-brjhl   1/1     Running   0          12s

    NAME                                   READY   UP-TO-DATE   AVAILABLE   AGE
    deployment.apps/materialize-operator   1/1     1            1           12s

    NAME                                              DESIRED   CURRENT   READY   AGE
    replicaset.apps/materialize-operator-84ff4b4648   1         1         1       12s
    ```

    If you run into an error during deployment, refer to the
    [Troubleshooting](/installation/troubleshooting) guide.

1. <a name="deploy-materialize-instances"></a>

   To deploy Materialize instances, modify the `main.tf` file to include the
   Materialize instance configuration, specifically, uncomment the
   `materialize_instances` block.

   ```bash
   # Once the operator is installed, you can define your Materialize instances here.
   # Uncomment the following block (or provide your own instances) to configure them.
   materialize_instances = [
     {
       name           = "analytics"
       namespace      = "materialize-environment"
       database_name  = "analytics_db"
       cpu_request    = "2"
       memory_request = "4Gi"
       memory_limit   = "4Gi"
     },
     {
       name           = "production"
       namespace      = "materialize-environment"
       database_name  = "production_db"
       cpu_request    = "4"
       memory_request = "8Gi"
       memory_limit   = "8Gi"
     }
   ]
   ```

1. Create a terraform plan and review the changes.

    ```bash
    terraform plan -out my-plan.tfplan
    ```

    The plan should show the changes to be made, with a summary similar to the
    following:

    ```
    Plan: 7 to add, 0 to change, 0 to destroy.

    Saved the plan to: my-plan.tfplan

    To perform exactly these actions, run the following command to apply:
    terraform apply "my-plan.tfplan"
    ```

1. If you are satisfied with the changes, apply the terraform plan.

    ```bash
    terraform apply my-plan.tfplan
    ```


1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be in the `Running` state.

   ```none
   NAME                                             READY   STATUS      RESTARTS      AGE
   pod/create-db-analytics-db-mh2jf                 0/1     Completed   0             108s
   pod/create-db-production-db-8vpj9                0/1     Completed   0             108s
   pod/mznzlk3r3fyl-balancerd-696dc4f949-tdtkv      1/1     Running     0             93s
   pod/mznzlk3r3fyl-cluster-s1-replica-s1-gen-1-0   1/1     Running     0             99s
   pod/mznzlk3r3fyl-cluster-s2-replica-s2-gen-1-0   1/1     Running     0             99s
   pod/mznzlk3r3fyl-cluster-s3-replica-s3-gen-1-0   1/1     Running     0             99s
   pod/mznzlk3r3fyl-cluster-u1-replica-u1-gen-1-0   1/1     Running     0             99s
   pod/mznzlk3r3fyl-console-57c84c99df-5vsdt        1/1     Running     0             86s
   pod/mznzlk3r3fyl-console-57c84c99df-6w5cw        1/1     Running     0             86s
   pod/mznzlk3r3fyl-environmentd-1-0                1/1     Running     0             107s
   pod/mzsylm1f691o-environmentd-1-0                0/1     Pending     0             107s

   NAME                                               TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                                        AGE
   service/mznzlk3r3fyl-balancerd                     ClusterIP   None            <none>        6876/TCP,6875/TCP                              93s
   service/mznzlk3r3fyl-cluster-s1-replica-s1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   99s
   service/mznzlk3r3fyl-cluster-s2-replica-s2-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   99s
   service/mznzlk3r3fyl-cluster-s3-replica-s3-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   99s
   service/mznzlk3r3fyl-cluster-u1-replica-u1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   99s
   service/mznzlk3r3fyl-console                       ClusterIP   None            <none>        8080/TCP                                       86s
   service/mznzlk3r3fyl-environmentd                  ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            94s
   service/mznzlk3r3fyl-environmentd-1                ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            108s
   service/mznzlk3r3fyl-persist-pubsub-1              ClusterIP   None            <none>        6879/TCP                                       108s
   service/mzsylm1f691o-environmentd-1                ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            107s
   service/mzsylm1f691o-persist-pubsub-1              ClusterIP   None            <none>        6879/TCP                                       107s

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mznzlk3r3fyl-balancerd   1/1     1            1           93s
   deployment.apps/mznzlk3r3fyl-console     2/2     2            2           86s

   NAME                                                DESIRED   CURRENT   READY      AGE
   replicaset.apps/mznzlk3r3fyl-balancerd-696dc4f949   1         1         1          93s
   replicaset.apps/mznzlk3r3fyl-console-57c84c99df     2         2         2          86s

   NAME                                                        READY   AGE
   statefulset.apps/mznzlk3r3fyl-cluster-s1-replica-s1-gen-1   1/1     99s
   statefulset.apps/mznzlk3r3fyl-cluster-s2-replica-s2-gen-1   1/1     99s
   statefulset.apps/mznzlk3r3fyl-cluster-s3-replica-s3-gen-1   1/1     99s
   statefulset.apps/mznzlk3r3fyl-cluster-u1-replica-u1-gen-1   1/1     99s
   statefulset.apps/mznzlk3r3fyl-environmentd-1                1/1     108s
   statefulset.apps/mzsylm1f691o-environmentd-1                0/1     107s

   NAME                                STATUS     COMPLETIONS   DURATION   AGE
   job.batch/create-db-analytics-db    Complete   1/1           11s        108s
   job.batch/create-db-production-db   Complete   1/1           11s        108s
   ```

1. Open the Materialize console in your browser:

   1. From the previous `kubectl` output, find the Materialize console service.

      ```none
      NAME                           TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
      service/mznzlk3r3fyl-console   ClusterIP   None         <none>        8080/TCP   86s
      ```

   1. Forward the Materialize console service to your local machine (substitute
      your service name for `mznzlk3r3fyl-console`):

      ```shell
      while true;
      do kubectl port-forward service/mznzlk3r3fyl-console 8080:8080 -n materialize-environment 2>&1 |
      grep -q "portforward.go" && echo "Restarting port forwarding due to an error." || break;
      done;
      ```
      {{< note >}}
      Due to a [known Kubernetes issue](https://github.com/kubernetes/kubernetes/issues/78446),
      interrupted long-running requests through a standard port-forward cause the port forward to hang. The command above
      automatically restarts the port forwarding if an error occurs, ensuring a more stable
      connection. It detects failures by monitoring for "portforward.go" error messages.
      {{< /note >}}

   1. Open a browser and navigate to
      [http://localhost:8080](http://localhost:8080).



## Troubleshooting

If you encounter issues:

1. Check operator logs:
```bash
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

2. Check environment logs:
```bash
kubectl logs -l app.kubernetes.io/name=environmentd -n materialize-environment
```

3. Verify the storage configuration:
```bash
kubectl get sc
kubectl get pv
kubectl get pvc -A
```

See also [Troubleshooting](/self-hosted/troubleshooting).

## Cleanup

To uninstall the Materialize operator:
```bash
helm uninstall materialize-operator -n materialize
```

This will remove the operator but preserve any PVs and data. To completely clean
up:

```bash
kubectl delete namespace materialize
kubectl delete namespace materialize-environment
```

In your Terraform directory, run:

```bash
terraform destroy
```

When prompted, type `yes` to confirm the deletion.

{{< tip>}}
To delete your S3 bucket, you may need to empty the S3 bucket first.  If the
`terraform destroy` command fails because the S3 bucket is not empty, empty the
S3 bucket first and rerun the `terraform destroy` command.
{{</ tip >}}

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Installation](/installation/)
- [Upgrading](/installation/upgrading/)
