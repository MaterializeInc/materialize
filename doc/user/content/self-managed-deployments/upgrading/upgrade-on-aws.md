---
title: "Upgrade on AWS"
description: "Upgrade Materialize on AWS using the Unified Terraform module."
menu:
  main:
    parent: "upgrading"
    weight: 20
---

The following tutorial upgrades your Materialize deployment running on AWS
Elastic Kubernetes Service (EKS). The tutorial assumes you have installed
Materialize on AWS using the instructions on [Install on
AWS](/self-managed-deployments/installation/install-on-aws/).

## Upgrade guidelines

{{< include-md file="shared-content/self-managed/general-rules-for-upgrades.md"
>}}

{{< include-md file="shared-content/self-managed/version-compatibility-upgrade-banner.md" >}}

## Prerequisites

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- [kubectl](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)

## Procedure

{{< important >}}

The following procedure performs a rolling upgrade, where both the old and new Materialize instances are running before the old instances are removed. When performing a rolling upgrade, ensure you have enough resources to support having both the old and new Materialize instances running.

{{</ important >}}

### Step 1: Set up

1. Open a Terminal window.

1. Configure AWS CLI with your AWS credentials. For details, see the [AWS documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

1. Go to the Terraform directory for your Materialize deployment. For example,
   if you deployed from the `aws/examples/simple` directory:

   ```bash
   cd materialize-terraform-self-managed/aws/examples/simple
   ```

1. Configure `kubectl` to connect to your EKS cluster, replacing:

   - `<your-eks-cluster-name>` with the name of your EKS cluster. Your cluster name can be found in the Terraform output or AWS console.

   - `<your-region>` with the region of your EKS cluster.

   ```bash
   aws eks update-kubeconfig --name <your-eks-cluster-name> --region <your-region>
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### Step 2: Upgrading the Materialize Operator

{{< important >}}

When upgrading Materialize, always upgrade the operator first.

{{</ important >}}

The Materialize Kubernetes operator is deployed via Helm and can be updated through standard Helm upgrade commands.


1. Check the current operator version:

   ```bash
   helm list -n materialize
   ```

1. Upgrade the Materialize operator using Helm:

   - If your deployment has not specified custom settings:

     ```bash
     helm upgrade materialize-operator materialize/materialize-operator \
       -n materialize \
       --version <operator-version>
     ```

     Replace `<operator-version>` with the desired operator version.

   - If your deployment has specified custom settings, make sure to include your
     values file:

    ```bash
    helm upgrade materialize-operator materialize/materialize-operator \
      -n materialize \
      --version <operator-version> \
      -f my-values.yaml
    ```

1. Verify that the operator is running:

   ```bash
   kubectl -n materialize get all
   ```

   Verify the operator upgrade by checking its events:

   ```bash
   kubectl -n materialize describe pod -l app.kubernetes.io/name=materialize-operator
   ```

   - The **Containers** section should show the `--helm-chart-version` argument set to the new version.
   - The **Events** section should list that the new version of the orchestratord has been pulled.

## Step 3: Upgrading Materialize Instances

In order to minimize unexpected downtime and avoid connection drops at critical periods for your application, changes are not immediately and automatically rolled out by the Operator. Instead, the upgrade process involves two steps:
- First, staging spec changes to the Materialize custom resource.
- Second, applying the changes via a `requestRollout`.

### Updating the `environmentdImageRef`

To find a compatible version with your currently deployed Materialize operator, check the `appVersion` in the Helm repository:

```bash
helm list -n materialize
```

Using the returned version, we can construct an image ref. We always recommend using the official Materialize image repository `docker.io/materialize/environmentd`.

The following is an example of how to patch the version:

```bash
# For version updates, first update the image reference
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"docker.io/materialize/environmentd:{{< self-managed/versions/get-latest-version >}}\"}}"
```

Replace:
- `<instance-name>` with your Materialize instance name (typically a UUID).
- `<materialize-instance-namespace>` with your instance namespace (typically `materialize-environment`).

### Applying the changes via `requestRollout`

To apply changes and kick off the Materialize instance upgrade, you must update the `requestRollout` field in the Materialize custom resource spec to a new UUID.

Be sure to consult the [Rollout Configurations](/self-managed-deployments/upgrading/#rollout-strategies) to ensure you've selected the correct rollout behavior.

```bash
# Then trigger the rollout with a new UUID
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```

It is possible to combine both operations in a single command if preferred:

```bash
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"docker.io/materialize/environmentd:{{< self-managed/versions/get-latest-version >}}\", \"requestRollout\": \"$(uuidgen)\"}}"
```

### Using YAML Definition

Alternatively, you can update your Materialize custom resource definition directly:

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: <instance-name>
  namespace: materialize-environment
spec:
  environmentdImageRef: docker.io/materialize/environmentd:{{< self-managed/versions/get-latest-version >}} # Update version as needed
  requestRollout: <new-uuid>    # Generate new UUID
  rolloutStrategy: WaitUntilReady # The mechanism to use when rolling out the new version. Can be WaitUntilReady or ImmediatelyPromoteCausingDowntime
  backendSecretName: materialize-backend
```

Apply the updated definition:

```bash
kubectl apply -f materialize.yaml
```

## Verifying the Upgrade

After initiating the rollout, you can monitor the status field of the Materialize custom resource to check on the upgrade.

```bash
# Watch the status of your Materialize environment
kubectl get materialize -n materialize-environment -w

# Check the logs of the operator
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

Verify that the components are running after the upgrade:

```bash
kubectl -n materialize-environment get all
```

Verify upgrade by checking the `balancerd` events:

```bash
kubectl -n materialize-environment describe pod -l app=balancerd
```

The **Events** section should list that the new version of the `balancerd` has been pulled.

Verify the upgrade by checking the `environmentd` events:

```bash
kubectl -n materialize-environment describe pod -l app=environmentd
```

The **Events** section should list that the new version of the `environmentd` has been pulled.

Open the Materialize Console. The Console should display the new version.

## See also

- [Materialize Operator Configuration](/self-managed-deployments/appendix/configuration/)
- [Materialize CRD Field Descriptions](/self-managed-deployments/appendix/materialize-crd-field-descriptions/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)
