<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Self-Managed
Deployments](/docs/self-managed-deployments/)

</div>

# Upgrading

Materialize releases new Self-Managed versions per the schedule outlined
in [Release
schedule](/docs/releases/schedule/#self-managed-release-schedule).

## General rules for upgrading

When upgrading:

- **Always** check the [version specific upgrade
  notes](/docs/self-managed-deployments/upgrading/#version-specific-upgrade-notes).

- **Always** upgrade the Materialize Operator **before** upgrading the
  Materialize instances.

<div class="note">

**NOTE:** For major version upgrades, you can **only** upgrade **one**
major version at a time. For example, upgrades from **v26**.1.0 to
**v27**.3.0 is permitted but **v26**.1.0 to **v28**.0.0 is not.

</div>

## Upgrade guides

The following upgrade guides are available as examples:

#### Upgrade using Helm Commands

| Guide | Description |
|----|----|
| [Upgrade on Kind](/docs/self-managed-deployments/upgrading/upgrade-on-kind/) | Uses standard Helm commands to upgrade Materialize on a Kind cluster in Docker. |

#### Upgrade using the new Terraform Modules

<div class="tip">

**💡 Tip:** The Terraform modules are provided as examples. They are not
required for upgrading Materialize.

</div>

| Guide | Description |
|----|----|
| [Upgrade on AWS (Terraform)](/docs/self-managed-deployments/upgrading/upgrade-on-aws/) | Uses the new Terraform module to deploy Materialize to AWS Elastic Kubernetes Service (EKS). |
| [Upgrade on Azure (Terraform)](/docs/self-managed-deployments/upgrading/upgrade-on-azure/) | Uses the new Terraform module to deploy Materialize to Azure Kubernetes Service (AKS). |
| [Upgrade on GCP (Terraform)](/docs/self-managed-deployments/upgrading/upgrade-on-gcp/) | Uses the new Terraform module to deploy Materialize to Google Kubernetes Engine (GKE). |

#### Upgrade using Legacy Terraform Modules

<div class="tip">

**💡 Tip:** The Terraform modules are provided as examples. They are not
required for upgrading Materialize.

</div>

| Guide | Description |
|----|----|
| [Upgrade on AWS (Legacy Terraform)](/docs/self-managed-deployments/upgrading/legacy/upgrade-on-aws-legacy/) | Uses legacy Terraform module to deploy Materialize to AWS Elastic Kubernetes Service (EKS). |
| [Upgrade on Azure (Legacy Terraform)](/docs/self-managed-deployments/upgrading/legacy/upgrade-on-azure-legacy/) | Uses legacy Terraform module to deploy Materialize to Azure Kubernetes Service (AKS). |
| [Upgrade on GCP (Legacy Terraform)](/docs/self-managed-deployments/upgrading/legacy/upgrade-on-gcp-legacy/) | Uses legacy Terraform module to deploy Materialize to Google Kubernetes Engine (GKE). |

## Upgrading the Helm Chart and Materialize Operator

<div class="important">

**! Important:** When upgrading Materialize, always upgrade the Helm
Chart and Materialize Operator first.

</div>

### Update the Helm Chart repository

To update your Materialize Helm Chart repository:

<div class="highlight">

``` chroma
helm repo update materialize
```

</div>

View the available chart versions:

<div class="highlight">

``` chroma
helm search repo materialize/materialize-operator --versions
```

</div>

### Upgrade your Materialize Operator

The Materialize Kubernetes Operator is deployed via Helm and can be
updated through standard `helm upgrade` command:

<div class="highlight">

``` chroma
helm upgrade -n <namespace> <release-name> materialize/materialize-operator \
  --version <new_version> \
  -f <your-custom-values.yml>
```

</div>

| Syntax element | Description |
|----|----|
| `<namespace>` | The namespace where the Operator is running. (e.g., `materialize`) |
| `<release-name>` | The release name. You can use `helm list -n <namespace>` to find your release name. |
| `<new_version>` | The upgrade version. |
| `<your-custom-values.yml>` | The name of your customization file, if using. If you are configuring using `\-\-set key=value` options, include them as well. |

You can use `helm list` to find your release name. For example, if your
Operator is running in the namespace `materialize`, run `helm list`:

<div class="highlight">

``` chroma
helm list -n materialize
```

</div>

Retrieve the name associated with the `materialize-operator` **CHART**;
for example, `my-demo` in the following helm list:

```
NAME       NAMESPACE     REVISION    UPDATED                                 STATUS      CHART                                          APP VERSION
my-demo materialize 1      2025-12-08 11:39:50.185976 -0500 EST deployed    materialize-operator-v26.1.0    v26.1.0
```

Then, to upgrade:

<div class="highlight">

``` chroma
helm upgrade -n materialize my-demo materialize/operator \
  -f my-values.yaml \
  --version v26.6.0
```

</div>

## Upgrading Materialize Instances

**After** you have upgraded your Materialize Operator, upgrade your
Materialize instance(s) to the **APP Version** of the Operator. To find
the version of your currently deployed Materialize Operator:

<div class="highlight">

``` chroma
helm list -n materialize
```

</div>

You will use the returned **App Version** for the updated
`environmentdImageRef` value. Specifically, for your Materialize
instance(s), set `environmentdImageRef` value to use the new version:

```
spec:
  environmentdImageRef: docker.io/materialize/environmentd:<app_version>
```

To minimize unexpected downtime and avoid connection drops at critical
periods for your application, the upgrade process involves two steps:

- First, stage the changes (update the `environmentdImageRef` with the
  new version) to the Materialize custom resource. The Operator watches
  for changes but does not automatically roll out the changes.

- Second, roll out the changes by specifying a new UUID for
  `requestRollout`.

### Stage the Materialize instance version change

To stage the Materialize instances version upgrade, update the
`environmentdImageRef` field in the Materialize custom resource spec to
the compatible version of your currently deployed Materialize Operator.

To stage, but **not** rollout, the Materialize instance version upgrade,
you can use the `kubectl patch` command; for example, if the **App
Version** is v26.6.0:

<div class="highlight">

``` chroma
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"docker.io/materialize/environmentd:v26.6.0\"}}"
```

</div>

<div class="note">

**NOTE:** Until you specify a new `requestRollout`, the Operator watches
for updates but does not roll out the changes.

</div>

### Applying the changes via `requestRollout`

To apply chang Materialize instance upgrade, you must update the
`requestRollout` field in the Materialize custom resource spec to a new
UUID. Be sure to consult the [Rollout
Configurations](#rollout-configuration) to ensure you’ve selected the
correct rollout behavior.

<div class="highlight">

``` chroma
# Then trigger the rollout with a new UUID
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```

</div>

### Staging and applying in a single command

Although separating the staging and rollout of the changes into two
steps can minimize unexpected downtime and avoid connection drops at
critical periods, you can, if preferred, combine both operations in a
single command

<div class="highlight">

``` chroma
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"docker.io/materialize/environmentd:v26.6.0\", \"requestRollout\": \"$(uuidgen)\"}}"
```

</div>

#### Using YAML Definition

Alternatively, you can update your Materialize custom resource
definition directly:

<div class="highlight">

``` chroma
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.6.0 # Update version as needed
  requestRollout: 22222222-2222-2222-2222-222222222222    # Use a new UUID
  forceRollout: 33333333-3333-3333-3333-333333333333      # Optional: for forced rollouts
  inPlaceRollout: false                                   # In Place rollout is deprecated and ignored. Please use rolloutStrategy
  rolloutStrategy: WaitUntilReady                         # The mechanism to use when rolling out the new version. Can be WaitUntilReady or ImmediatelyPromoteCausingDowntime
  backendSecretName: materialize-backend
```

</div>

Apply the updated definition:

<div class="highlight">

``` chroma
kubectl apply -f materialize.yaml
```

</div>

## Rollout Configuration

### `requestRollout`

Specify a new `UUID` value for the `requestRollout` to roll out the
changes to the Materialize instance.

<div class="note">

**NOTE:** `requestRollout` without the `forcedRollout` field only rolls
out if changes exist to the Materialize instance. To roll out even if
there are no changes to the instance, use with `forcedRollouts`.

</div>

<div class="highlight">

``` chroma
# Only rolls out if there are changes
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```

</div>

#### `requestRollout` with `forcedRollouts`

Specify a new `UUID` value for `forcedRollout` to roll out even when
there are no changes to the instance. Use `forcedRollout` with
`requestRollout`.

<div class="highlight">

``` chroma
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\", \"forceRollout\": \"$(uuidgen)\"}}"
```

</div>

### Rollout strategies

Rollout strategies control how Materialize transitions from the current
generation to a new generation during an upgrade.

The behavior of the new version rollout follows your `rolloutStrategy`
setting.

#### *WaitUntilReady* - ***Default***

`WaitUntilReady` creates a new generation of pods and automatically cuts
over to them as soon as they catch up to the old generation and become
`ReadyToPromote`. This strategy temporarily doubles the required
resources to run Materialize.

#### *ImmediatelyPromoteCausingDowntime*

<div class="warning">

**WARNING!** Using the `ImmediatelyPromoteCausingDowntime` rollout flag
will cause downtime.

</div>

`ImmediatelyPromoteCausingDowntime` tears down the prior generation, and
immediately promotes the new generation without waiting for it to
hydrate. This causes downtime until the new generation has hydrated.
However, it does not require additional resources.

#### *ManuallyPromote*

`ManuallyPromote` allows you to choose when to promote the new
generation. This means you can time the promotion for periods when load
is low, minimizing the impact of potential downtime for any clients
connected to Materialize. This strategy temporarily doubles the required
resources to run Materialize.

To minimize downtime, wait until the new generation has fully hydrated
and caught up to the prior generation before promoting. To check
hydration status, inspect the `UpToDate` condition in the Materialize
resource status. When hydration completes, the condition will be
`ReadyToPromote`.

To promote, update the `forcePromote` field to match the
`requestRollout` field in the Materialize spec. If you need to promote
before hydration completes, you can set `forcePromote` immediately, but
clients may experience downtime.

<div class="warning">

**WARNING!** Leaving a new generation unpromoted for over 6 hours may
cause downtime.

</div>

**Do not leave new generations unpromoted indefinitely**. They should
either be promoted or canceled. New generations open a read hold on the
metadata database that prevents compaction. This hold is only released
when the generation is promoted or canceled. If left open too long,
promoting or canceling can trigger a spike in deletion load on the
metadata database, potentially causing downtime. It is not recommended
to leave generations unpromoted for over 6 hours.

#### *inPlaceRollout* - ***Deprecated***

The setting is ignored.

## Verifying the Upgrade

After initiating the rollout, you can monitor the status field of the
Materialize custom resource to check on the upgrade.

<div class="highlight">

``` chroma
# Watch the status of your Materialize environment
kubectl get materialize -n materialize-environment -w

# Check the logs of the operator
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

</div>

## Version Specific Upgrade Notes

### Upgrading to `v26.1` and later versions

- To upgrade to `v26.1` or future versions, you must first upgrade to
  `v26.0`

### Upgrading to `v26.0`

- Upgrading to `v26.0.0` is a major version upgrade. To upgrade to
  `v26.0` from `v25.2.X` or `v25.1`, you must first upgrade to
  `v25.2.16` and then upgrade to `v26.0.0`.

- For upgrades, the `inPlaceRollout` setting has been deprecated and
  will be ignored. Instead, use the new setting `rolloutStrategy` to
  specify either:

  - `WaitUntilReady` (*Default*)
  - `ImmediatelyPromoteCausingDowntime`

  For more information, see
  [`rolloutStrategy`](/docs/self-managed-deployments/upgrading/#rollout-strategies).

- New requirements were introduced for [license
  keys](/docs/releases/#license-key). To upgrade, you will first need to
  add a license key to the `backendSecret` used in the spec for your
  Materialize resource.

  See [License key](/docs/releases/#license-key) for details on getting
  your license key.

- Swap is now enabled by default. Swap reduces the memory required to
  operate Materialize and improves cost efficiency. Upgrading to `v26.0`
  requires some preparation to ensure Kubernetes nodes are labeled and
  configured correctly. As such:

  - If you are using the Materialize-provided Terraforms, upgrade to
    version `v0.6.1` of the Terraform.

  - If you are **not** using a Materialize-provided Terraform, refer to
    [Prepare for swap and upgrade to
    v26.0](/docs/self-managed-deployments/appendix/upgrade-to-swap/).

### Upgrading between minor versions less than `v26`

- Prior to `v26`, you must upgrade at most one minor version at a time.
  For example, upgrading from `v25.1.5` to `v25.2.16` is permitted.

## See also

- [Materialize Operator
  Configuration](/docs/self-managed-deployments/operator-configuration/)

- [Materialize CRD Field
  Descriptions](/docs/self-managed-deployments/materialize-crd-field-descriptions/)

- [Troubleshooting](/docs/self-managed-deployments/troubleshooting/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/self-managed-deployments/upgrading/_index.md"
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

© 2026 Materialize Inc.

</div>
