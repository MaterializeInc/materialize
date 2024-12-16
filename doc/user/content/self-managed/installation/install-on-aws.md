---
title: "Install on AWS"
description: ""
robots: "noindex, nofollow"
---

Self-managed Materialize requires:

- A Kubernetes (v1.19+) environment.

- PostgreSQL or CockroachDB as a metadata database.

- Blob storage.

The tutorial deploys Materialize to AWS Elastic Kubernetes Service (EKS) with a
PostgreSQL RDS database as the metadata database and AWS S3 for blob storage.

{{< important >}}

For testing purposes only. For testing purposes only. For testing purposes only. ....

{{< /important >}}

## Prerequisites

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, refer to the [Helm
documentation](https://helm.sh/docs/intro/install/).

### AWS CLI

If you do not have the AWS CLI installed,

- Install the AWS CLI. For details, see the [AWS
  documentation](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).

- Configure with your AWS credentials. For details, see the [AWS
  documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

### AWS Kubernetes environment

- A Kubernetes (v1.19+) environment.

- PostgreSQL or CockroachDB as a metadata database.

- Blob storage.

When operating in AWS, we recommend:

- Using the `r8g`, `r7g`, and `r6g` families when running without local disk.

- Using the `r7gd` and `r6gd` families of instances (and `r8gd` once available)
  when running with local disk (Recommended for production.  See [Operational guidelines](/self-managed/operational-guidelines/#locally-attached-nvme-storage-openebs) for more information.)

See [A. Set up AWS Kubernetes environment](#a-set-up-aws-kubernetes-environment)
for details.

## A. Set up AWS Kubernetes environment

{{< tabs  >}}

{{< tab "Terraform" >}}

Materialize provides a [sample Terraform
module](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/README.md)
for evaluation purposes only. The module deploys a sample infrastructure on AWS
with the following components:

- EKS component
- Networking component
- Storage component (S3 for blob storage)
- Database component for metadata storage (PostgreSQL RDS)

{{< warning >}}

The sample Terraform module is for **evaluation purposes only** and not intended
for production use. It is provided to help you get started with Materialize for
evaluation purposes only. Materialize does not support nor recommends this
module for production use. Materialize does not guarantee tests for changes to
the module.

For simplicity, this tutorial stores your RDS Postgres secret in a file. In
practice, refer to your organization's official security and
Terraform/infrastructure practices.

{{< /important >}}

1. If you do not have Terraform installed, [install
   Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

1. Clone or download the [Materialize's sample Terraform
   repo](https://github.com/MaterializeInc/terraform-aws-materialize).

1. Go to the Materialize Terraform repo directory.

   ```bash
   cd terraform-aws-materialize
   ```

1. Copy the `terraform.tfvars.example` file to `terraform.tfvars`.

   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```

1. Edit the `terraform.tfvars` file to set the values for your AWS environment.
   In particular,

   - set your `database_password` to a secure password.
   - set your `node_group_ami_type` to a `"AL2023_ARM_64_STANDARD"`.
   - set your `node_group_instance_types` to a supported instance type for
     ARM64.



   ```bash
   cluster_name      = "my-test-eks"
   environment       = "my-test"
   vpc_name          = "my-test-vpc"

   bucket_name       = "my-test-bucket"
   db_identifier     = "my-test-db"
   database_password = "enter-your-secure-password"

   tags = {
     Environment = "my-test"
     Team        = "my-test-team"
     Project     = "my-test-project"
   }

   node_group_ami_type       = "AL2023_ARM_64_STANDARD"
   node_group_instance_types = ["r6g.medium"]
   node_group_desired_size   = 3
   node_group_min_size       = 2
   node_group_max_size       = 5

   db_instance_class    = "db.t3.large"
   db_allocated_storage = 20
   db_multi_az          = false

   enable_cluster_creator_admin_permissions = true
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
   ...

   Apply complete! Resources: 71 added, 0 changed, 0 destroyed.

   Outputs:
   database_endpoint = "my-test-db.abcdefg8dsto.us-east-1.rds.amazonaws.com:5432"
   eks_cluster_endpoint = "https://0123456789A00BCD000E11BE12345A01.gr7.us-east-1.eks.amazonaws.com"
   materialize_s3_role_arn = "arn:aws:iam::000111222333:role/my-test-materialize-s3-role"
   metadata_backend_url = <sensitive>
   oidc_provider_arn = "arn:aws:iam::000111222333:oidc-provider/oidc.eks. us-east-1.amazonaws.com/id/0123456789A00BCD000E11BE12345A01"
   persist_backend_url = "s3://my-test-bucket/ my-test:serviceaccount:materialize-environment:12345678-1234-1234-1234-1234 56789012"
   s3_bucket_name = "my-test-bucket"
   vpc_id = "vpc-0abc000bed1d111bd"
   ```

1. Note your specific values for the following fields:

   - `materialize_s3_role_arn` (Used during [B. Install the Materialize
     Operator](#b-install-the-materialize-operator))

   - `database_endpoint` (Used during [C. Install
     Materialize](#c-install-materialize))

   - `persist_backend_url` (Used during [C. Install
     Materialize](#c-install-materialize))


1. If you do not have `kubectl` installed,

   - Install `kubectl`. See the [Amazon EKS: install `kubectl`
     documentation](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html).

   - Configure `kubectl` to  connect to your EKS cluster, replacing:

      - `<your-cluster-name>` with the name of your EKS cluster (specified in
        `terraform.tfvars`)

      - `<your-region>` with the region of your EKS cluster

      ```bash
      aws eks update-kubeconfig --name <your-cluster-name> --region <your-region>
      ```

      To verify that you have configured correctly, run the following command:

      ```bash
      kubectl get nodes
      ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).


{{< /tab >}}

{{< /tabs >}}


## B. Install the Materialize Operator

1. Clone/download the [Materialize
   repo](https://github.com/MaterializeInc/materialize).

1. Go to the Materialize repo directory.

   ```bash
   cd materialize
   ```

1. Check out the {{% self-managed/latest_version %}} tag.

1. Create a `my-materialize-values.yaml` configuration file for the Materialize
   operator. Update with:

   - your region,

   - your AWS Account ID, and

   - your `materialize_s3_role_arn`. (Refer to your [Terraform
     output](#terraform-output) for the `materialize_s3_role_arn`.)

      ```yaml
      # my-materialize-values.yaml

      operator:
        cloudProvider:
          type: "aws"
          region: "<your-aws-region>" # e.g. us-west-2
          providers:
            aws:
              enabled: true
              accountID: "<your-aws-account-id>" # e.g. 123456789012
              iam:
                roles:
                  environment: "<your-materialize-s3-role-arn>" # e.g. arn:aws:iam::123456789012:role/materialize-s3-role
      networkPolicies:
        enabled: true
        egress:
          enabled: true
          cidrs: ["0.0.0.0/0"]
        ingress:
          enabled: true
          cidrs: ["0.0.0.0/0"]
        internal:
          enabled: true

      ```

   For production, if you have [opted for locally-attached storage](/self-managed/operational-guidelines/#locally-attached-nvme-storage-openebs),
   include the storage configuration your configuration file.  See the
   [Locally-attached NVMe storage
   (OpenEBS)](/self-managed/operational-guidelines/#locally-attached-nvme-storage-openebs)
   for details.

1. Install the Materialize operator `materialize-operator`, specifying the path
   to your `my-materialize-values.yaml` file:

   ```shell
   helm install materialize-operator misc/helm-charts/operator \
      -f my-materialize-values.yaml  \
      --namespace materialize --create-namespace
   ```

1. Verify the installation and check the status:

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
    [Troubleshooting](/self-hosted/troubleshooting) guide.

## C. Install Materialize

To deploy Materialize:

<a name="deploy-materialize-secrets"></a>

1. {{< warning>}}

   For simplicity, this tutorial stores your RDS Postgres secret in a file. In
   practice, refer to your organization's official security and Terraform/infrastructure
   practices.

   {{< /warning >}}

1. For your backend configuration, create a file
   `materialize-backend-secret.yaml` for your [Kubernetes
   Secret](https://kubernetes.io/docs/concepts/configuration/secret/).

    ```yaml
    apiVersion: v1
    kind: Secret
    metadata:
      name: materialize-backend
      namespace: materialize-environment
    stringData:
      metadata_backend_url: "postgres://db_user:db_password@database_endpoint/db_name?sslmode=require"
      persist_backend_url: "your-persist-backend-url"
    ```

    - For `your-metadata-backend-url`, update with your values:

      - Default `db_user` is `materialize`,
      - `db_password` is the value you specified in the `terraform.tfvars` file.
      - `database_endpoint` is the value from the [Terraform
        output](#terraform-output).
      - Default `db_name` is `materialize`.

      {{< tip >}}
      URL encode your database password.
      {{< /tip >}}


    - For `your-persist-backend-url`, set to the value from the [Terraform
      output](#terraform-output).

1. Create a YAML file `my-materialize.yaml` for your Materialize
   configuration.

   Replace `${var.service_account_name}` with the the desired name for your
   Materialize.

   ```yaml
   apiVersion: materialize.cloud/v1alpha1
   kind: Materialize
   metadata:
     name: "${var.service_account_name}"
     namespace: materialize-environment
   spec:
     environmentdImageRef: materialize/environmentd:latest
     environmentdResourceRequirements:
       limits:
         memory: 16Gi
       requests:
         cpu: "2"
         memory: 16Gi
     balancerdResourceRequirements:
       limits:
         memory: 256Mi
       requests:
         cpu: "100m"
         memory: 256Mi
     backendSecretName: materialize-backend
   ```

1. Create the `materialize-environment` namespace and apply the files to install
   Materialize:

   ```shell
   kubectl create namespace materialize-environment
   kubectl apply -f materialize-backend-secret.yaml
   kubectl apply -f my-materialize.yaml
   ```

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be in the `Running` state.

   ```none
   NAME                                             READY   STATUS    RESTARTS   AGE
   pod/mzm3otrsfcv7-balancerd-59454965d4-jjw4c      1/1     Running   0          37s
   pod/mzm3otrsfcv7-cluster-s1-replica-s1-gen-1-0   1/1     Running   0          43s
   pod/mzm3otrsfcv7-cluster-s2-replica-s2-gen-1-0   1/1     Running   0          43s
   pod/mzm3otrsfcv7-cluster-s3-replica-s3-gen-1-0   1/1     Running   0          43s
   pod/mzm3otrsfcv7-cluster-u1-replica-u1-gen-1-0   1/1     Running   0          42s
   pod/mzm3otrsfcv7-console-68b5cddfbf-xvs97        1/1     Running   0          30s
   pod/mzm3otrsfcv7-console-68b5cddfbf-z5zml        1/1     Running   0          30s
   pod/mzm3otrsfcv7-environmentd-1-0                1/1     Running   0          47s

   NAME                                               TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                                        AGE
   service/mzm3otrsfcv7-balancerd                     ClusterIP   None         <none>        6876/TCP,6875/TCP                              37s
   service/mzm3otrsfcv7-cluster-s1-replica-s1-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   43s
   service/mzm3otrsfcv7-cluster-s2-replica-s2-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   43s
   service/mzm3otrsfcv7-cluster-s3-replica-s3-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   43s
   service/mzm3otrsfcv7-cluster-u1-replica-u1-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   43s
   service/mzm3otrsfcv7-console                       ClusterIP   None         <none>        8080/TCP                                       30s
   service/mzm3otrsfcv7-environmentd                  ClusterIP   None         <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            38s
   service/mzm3otrsfcv7-environmentd-1                ClusterIP   None         <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            47s
   service/mzm3otrsfcv7-persist-pubsub-1              ClusterIP   None         <none>        6879/TCP                                       47s

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mzm3otrsfcv7-balancerd   1/1     1            1           38s
   deployment.apps/mzm3otrsfcv7-console     2/2     2            2           30s

   NAME                                                DESIRED   CURRENT   READY   AGE
   replicaset.apps/mzm3otrsfcv7-balancerd-59454965d4   1         1         1       37s
   replicaset.apps/mzm3otrsfcv7-console-68b5cddfbf     2         2         2       30s

   NAME                                                        READY   AGE
   statefulset.apps/mzm3otrsfcv7-cluster-s1-replica-s1-gen-1   1/1     43s
   statefulset.apps/mzm3otrsfcv7-cluster-s2-replica-s2-gen-1   1/1     43s
   statefulset.apps/mzm3otrsfcv7-cluster-s3-replica-s3-gen-1   1/1     43s
   statefulset.apps/mzm3otrsfcv7-cluster-u1-replica-u1-gen-1   1/1     43s
   statefulset.apps/mzm3otrsfcv7-environmentd-1                1/1     47s
   ```

1. Open the Materialize console in your browser:

   1. From the previous `kubectl` output, find the Materialize console service.

      ```none
      NAME                           TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
      service/mzm3otrsfcv7-console   ClusterIP   None         <none>        8080/TCP   30s
      ```

   1. Forward the Materialize console service to your local machine (substitute
      your service name for `mzm3otrsfcv7-console`):

      ```shell
      while true;
      do kubectl port-forward svc/mzm3otrsfcv7-console 8080:8080 -n materialize-environment 2>&1 |
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

Delete the Materialize environment:
```bash
kubectl delete -f materialize-environment.yaml
```

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

## See also

- [Materialize Kubernetes Operator Helm Chart](/self-managed/)
- [Materialize Operator Configuration](/self-managed/configuration/)
- [Troubleshooting](/self-managed/troubleshooting/)
- [Operational guidelines](/self-managed/operational-guidelines/)
- [Installation](/self-managed/installation/)
- [Upgrading](/self-managed/upgrading/)
