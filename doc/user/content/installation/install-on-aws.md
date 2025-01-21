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

See [A. Set up AWS Kubernetes environment](#a-set-up-aws-kubernetes-environment)
for a sample setup.

## A. Set up AWS Kubernetes environment

{{< tabs  >}}

{{< tab "Terraform" >}}

Materialize provides a [sample Terraform
module](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/README.md)
for evaluation purposes only. The module deploys a sample infrastructure on AWS
(region `us-east-1`) with the following components:

- A Kuberneted (EKS) cluster
- A dedicated VPC
- An S3 for blob storage
- An RDS PostgreSQL cluster and database for metadata storage

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

1. Create a `terraform.tfvars` file and specify a database password.

   ```bash
   database_password = "enter-your-secure-password" # Enter a secure password
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
   Apply complete! Resources: 82 added, 0 changed, 0 destroyed.

   Outputs:
   database_endpoint = "materialize-simple.abcdefg8dsto.us-east-1.rds.amazonaws.com:5432"
   eks_cluster_endpoint = "https://0123456789A00BCD000E11BE12345A01.gr7.us-east-1.eks.amazonaws.com"
   materialize_s3_role_arn = "arn:aws:iam::000111222333:role/dev-materialize-s3-role"
   metadata_backend_url = <sensitive>
   oidc_provider_arn = "arn:aws:iam::000111222333:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/0123456789A00BCD000E11BE12345A01"
   persist_backend_url = "s3://materialize-simple-storage-c2663c2f/dev:serviceaccount:materialize-environment:12345678-1234-1234-1234-123456789012"
   s3_bucket_name = "materialize-simple-storage-c2663c2f"
   vpc_id = "vpc-0abc000bed1d111bd"
   ```

1. Note your specific values for the following fields:

   - `materialize_s3_role_arn` (Used during [B. Install the Materialize
     Operator](#b-install-the-materialize-operator))

   - `persist_backend_url` (Used during [C. Install
     Materialize](#c-install-materialize))

   - `metadata_backend_url` (Used during [C. Install
     Materialize](#c-install-materialize)). You can get the connection strings by running the
     following command:

     ```bash
     terraform output -json metadata_backend_url | jq
     ```

1. Configure `kubectl` to connect to your EKS cluster.

   - By default, the example Terraform module uses `materialize-eks-simple` as
     the name of your EKS cluster.

   - By default, the example Terraform module uses `us-east-1` as the region of
     your EKS cluster.

   ```bash
   aws eks update-kubeconfig --name materialize-eks-simple --region us-east-1
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
   repo](https://github.com/MaterializeInc/materialize). The tutorial uses the
   `lts-v0.130` branch.

   ```sh
   git clone --branch lts-v0.130 https://github.com/MaterializeInc/materialize.git
   ```

1. Go to the Materialize repo directory.

   ```bash
   cd materialize
   ```

1. Create a `my-materialize-operator-values.yaml` configuration file for the
   Materialize operator. Update with:

   - your AWS region (the sample Terraform module uses `us-east-1`).

   - your AWS Account ID, and

   - your `materialize_s3_role_arn`. (Refer to your [Terraform
     output](#terraform-output) for the `materialize_s3_role_arn`.)

      ```yaml
      # my-materialize-operator-values.yaml

      operator:
        cloudProvider:
          type: "aws"
          region: "your-aws-region" # e.g. us-east-1
          providers:
            aws:
              enabled: true
              accountID: "your-aws-account-id" # e.g. 123456789012
              iam:
                roles:
                  environment: "your-materialize-s3-role-arn" # e.g. arn:aws:iam::123456789012:role/materialize-s3-role
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

   For production, if you have [opted for locally-attached storage](/installation/operational-guidelines/#locally-attached-nvme-storage-openebs),
   include the storage configuration in your configuration file.  See the
   [Locally-attached NVMe storage
   (OpenEBS)](/installation/operational-guidelines/#locally-attached-nvme-storage-openebs)
   for details.

1. Install the Materialize operator `materialize-operator`, specifying the path
   to your `my-materialize-operator-values.yaml` file:

   ```shell
   helm install materialize-operator misc/helm-charts/operator \
      -f my-materialize-operator-values.yaml  \
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
    [Troubleshooting](/installation/troubleshooting) guide.

## C. Install Materialize

To deploy Materialize:

<a name="deploy-materialize-secrets"></a>

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
      persist_backend_url: "your-persist-backend-url"
      metadata_backend_url: "your-metadata-backend-url"
    ```

    - For `your-persist-backend-url`, set to the value from the [Terraform
      output](#terraform-output).

    - For `your-metadata-backend-url`, set to the value from the [Terraform
      output](#terraform-output).

      {{< tip >}}
      You may need to URL encode your database password.
      {{< /tip >}}

1. Create a YAML file `my-materialize.yaml` for your Materialize
   configuration.

   ```yaml
   apiVersion: materialize.cloud/v1alpha1
   kind: Materialize
   metadata:
     name: 12345678-1234-1234-1234-123456789012
     namespace: materialize-environment
   spec:
     environmentdImageRef: materialize/environmentd:v0.130.1
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

1. Create the your namespace.

   ```shell
   kubectl create namespace materialize-environment
   ```

1. Apply the files to install Materialize:

   ```shell
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
