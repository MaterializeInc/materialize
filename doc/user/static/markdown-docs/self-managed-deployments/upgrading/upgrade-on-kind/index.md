# Upgrade on kind

Upgrade Materialize running locally on a kind cluster.



To upgrade your Materialize instances, first choose a new operator version and upgrade the Materialize operator. Then, upgrade your Materialize instances to the same version. The following tutorial upgrades your Materialize deployment running locally on a [`kind`](https://kind.sigs.k8s.io/)
cluster.

The tutorial assumes you have installed Materialize on `kind` using the
instructions on [Install locally on
kind](/self-managed-deployments/installation/install-on-local-kind/).

> **Important:** When performing major version upgrades, you can upgrade only one major version
> at a time. For example, upgrades from **v26**.1.0 to **v27**.2.0 is permitted
> but **v26**.1.0 to **v28**.0.0 is not. Skipping major versions or downgrading is
> not supported. To upgrade from v25.2 to v26.0, you must [upgrade first to v25.2.16+](https://materialize.com/docs/self-managed/v25.2/release-notes/#v25216).
>
>



## Prerequisites

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, install. For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl`
documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### License key

Starting in v26.0, Materialize requires a license key. If your existing
deployment does not have a license key configured, contact <a href="https://materialize.com/docs/support/">Materialize support</a>.

## Upgrade

> **Important:** The following procedure performs a rolling upgrade, where both the old and new
> Materialize instances are running before the the old instance are removed.
> When performing a rolling upgrade, ensure you have enough resources to support
> having both the old and new Materialize instances running.
>
>





1. Open a Terminal window.

1. Go to your Materialize working directory.

   ```shell
   cd my-local-mz
   ```

1. Upgrade the Materialize Helm chart.

   ```shell
   helm repo update materialize
   ```

1. Get the sample configuration files for the new version.

   ```shell
   mz_version=v26.8.0

   curl -o upgrade-values.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/operator/values.yaml
   ```

   If you have previously modified the `sample-values.yaml` file for your
   deployment, include the changes into the `upgrade-values.yaml` file.

1. Upgrade the Materialize Operator, specifying the new version and the updated
   configuration file. Include any additional configurations that you specify
   for your deployment.

   ```shell
   helm upgrade my-materialize-operator materialize/materialize-operator \
   --namespace=materialize \
   --version v26.8.0 \
   -f upgrade-values.yaml \
   --set observability.podMetrics.enabled=true
   ```

1. Verify that the operator is running:

   ```bash
   kubectl -n materialize get all
   ```

   Verify the operator upgrade by checking its events:

   ```bash
   kubectl -n materialize describe pod -l app.kubernetes.io/name=materialize-operator
   ```

1. As of v26.0, Self-Managed Materialize requires a license key. If your
deployment has not been configured with a license key:

   a. Contact [Materialize support](https://materialize.com/docs/support/).

   b. Once you have your license key, run the following command to add it to the `materialize-backend` secret:

      ```bash
      kubectl -n materialize-environment patch secret materialize-backend -p '{"stringData":{"license_key":"<your license key goes here>"}}' --type=merge
      ```

1. Create a new `upgrade-materialize.yaml` file, updating the following fields:

   | Field | Description |
   |-------|-------------|
   | `environmentdImageRef` | Update the version to the new version. This should be the same as the operator version: `v26.8.0`. |
   | `requestRollout` or `forceRollout`| Enter a new UUID. Can be generated with `uuidgen`. <br> <ul><li>`requestRollout` triggers a rollout only if changes exist. </li><li>`forceRollout` triggers a rollout even if no changes exist.</li></ul> |


   ```yaml
   apiVersion: materialize.cloud/v1alpha1
   kind: Materialize
   metadata:
     name: 12345678-1234-1234-1234-123456789012
     namespace: materialize-environment
   spec:
     environmentdImageRef: materialize/environmentd:v26.8.0 # Update version
     requestRollout: 22222222-2222-2222-2222-222222222222    # Enter a new UUID
   # forceRollout: 33333333-3333-3333-3333-333333333333    # For forced rollouts
     rolloutStrategy: WaitUntilReady                         # The mechanism to use when rolling out the new version.
     backendSecretName: materialize-backend
   ```

1. Apply the upgrade-materialize.yaml file to your Materialize instance:

   ```shell
   kubectl apply -f upgrade-materialize.yaml
   ```

1. Verify that the components are running after the upgrade:

   ```bash
   kubectl -n materialize-environment get all
   ```

   Verify upgrade by checking the `balancerd` events:

   ```bash
   kubectl -n materialize-environment describe pod -l app=balancerd
   ```

   The **Events** section should list that the new version of the `balancerd`
   have been pulled.

   Verify the upgrade by checking the `environmentd` events:

   ```bash
   kubectl -n materialize-environment describe pod -l app=environmentd
   ```

   The **Events** section should list that the new version of the `environmentd`
   have been pulled.

1. Open the Materialize Console. The Console should display the new version.


## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)
