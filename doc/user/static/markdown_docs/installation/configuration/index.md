<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [Install/Upgrade
(Self-Managed)](/docs/self-managed/v25.2/installation/)

</div>

# Materialize Operator Configuration

## Configure the Materialize operator

To configure the Materialize operator, you can:

- Use a configuration YAML file (e.g., `values.yaml`) that specifies the
  configuration values and then install the chart with the `-f` flag:

  <div class="highlight">

  ``` chroma
  # Assumes you have added the Materialize operator Helm chart repository
  helm install my-materialize-operator materialize/materialize-operator \
     -f /path/to/your/config/values.yaml
  ```

  </div>

- Specify each parameter using the `--set key=value[,key=value]`
  argument to `helm install`. For example:

  <div class="highlight">

  ``` chroma
  # Assumes you have added the Materialize operator Helm chart repository
  helm install my-materialize-operator materialize/materialize-operator  \
    --set observability.podMetrics.enabled=true
  ```

  </div>

| Parameter | Default |
|----|----|
| [`balancerd.enabled`](#balancerdenabled) | `true` |
| [`balancerd.nodeSelector`](#balancerdnodeselector) | `{}` |
| [`clusterd.nodeSelector`](#clusterdnodeselector) | `{}` |
| [`console.enabled`](#consoleenabled) | `true` |
| [`console.imageTagMapOverride`](#consoleimagetagmapoverride) | `{}` |
| [`console.nodeSelector`](#consolenodeselector) | `{}` |
| [`environmentd.nodeSelector`](#environmentdnodeselector) | `{}` |
| [`networkPolicies.egress`](#networkpoliciesegress) | `{"cidrs":["0.0.0.0/0"],"enabled":false}` |
| [`networkPolicies.enabled`](#networkpoliciesenabled) | `false` |
| [`networkPolicies.ingress`](#networkpoliciesingress) | `{"cidrs":["0.0.0.0/0"],"enabled":false}` |
| [`networkPolicies.internal`](#networkpoliciesinternal) | `{"enabled":false}` |
| [`observability.enabled`](#observabilityenabled) | `true` |
| [`observability.podMetrics.enabled`](#observabilitypodmetricsenabled) | `false` |
| [`observability.prometheus.scrapeAnnotations.enabled`](#observabilityprometheusscrapeannotationsenabled) | `true` |
| [`operator.args.enableInternalStatementLogging`](#operatorargsenableinternalstatementlogging) | `true` |
| [`operator.args.startupLogFilter`](#operatorargsstartuplogfilter) | `INFO,mz_orchestratord=TRACE` |
| [`operator.cloudProvider.providers.aws.accountID`](#operatorcloudproviderprovidersawsaccountid) |  |
| [`operator.cloudProvider.providers.aws.enabled`](#operatorcloudproviderprovidersawsenabled) | `false` |
| [`operator.cloudProvider.providers.aws.iam.roles.connection`](#operatorcloudproviderprovidersawsiamrolesconnection) |  |
| [`operator.cloudProvider.providers.aws.iam.roles.environment`](#operatorcloudproviderprovidersawsiamrolesenvironment) |  |
| [`operator.cloudProvider.providers.gcp`](#operatorcloudproviderprovidersgcp) | `{"enabled":false}` |
| [`operator.cloudProvider.region`](#operatorcloudproviderregion) | `kind` |
| [`operator.cloudProvider.type`](#operatorcloudprovidertype) | `local` |
| [`operator.clusters.defaultReplicationFactor.analytics`](#operatorclustersdefaultreplicationfactoranalytics) | `0` |
| [`operator.clusters.defaultReplicationFactor.probe`](#operatorclustersdefaultreplicationfactorprobe) | `0` |
| [`operator.clusters.defaultReplicationFactor.support`](#operatorclustersdefaultreplicationfactorsupport) | `0` |
| [`operator.clusters.defaultReplicationFactor.system`](#operatorclustersdefaultreplicationfactorsystem) | `0` |
| [`operator.clusters.defaultSizes.analytics`](#operatorclustersdefaultsizesanalytics) | `25cc` |
| [`operator.clusters.defaultSizes.catalogServer`](#operatorclustersdefaultsizescatalogserver) | `25cc` |
| [`operator.clusters.defaultSizes.default`](#operatorclustersdefaultsizesdefault) | `25cc` |
| [`operator.clusters.defaultSizes.probe`](#operatorclustersdefaultsizesprobe) | `mz_probe` |
| [`operator.clusters.defaultSizes.support`](#operatorclustersdefaultsizessupport) | `25cc` |
| [`operator.clusters.defaultSizes.system`](#operatorclustersdefaultsizessystem) | `25cc` |
| [`operator.image.pullPolicy`](#operatorimagepullpolicy) | `IfNotPresent` |
| [`operator.image.repository`](#operatorimagerepository) | `materialize/orchestratord` |
| [`operator.image.tag`](#operatorimagetag) | `v0.138.0` |
| [`operator.nodeSelector`](#operatornodeselector) | `{}` |
| [`operator.resources.limits`](#operatorresourceslimits) | `{"memory":"512Mi"}` |
| [`operator.resources.requests`](#operatorresourcesrequests) | `{"cpu":"100m","memory":"512Mi"}` |
| [`operator.secretsController`](#operatorsecretscontroller) | `kubernetes` |
| [`rbac.create`](#rbaccreate) | `true` |
| [`schedulerName`](#schedulername) | `nil` |
| [`serviceAccount.create`](#serviceaccountcreate) | `true` |
| [`serviceAccount.name`](#serviceaccountname) | `orchestratord` |
| [`storage.storageClass.allowVolumeExpansion`](#storagestorageclassallowvolumeexpansion) | `false` |
| [`storage.storageClass.create`](#storagestorageclasscreate) | `false` |
| [`storage.storageClass.name`](#storagestorageclassname) |  |
| [`storage.storageClass.parameters`](#storagestorageclassparameters) | `{"fsType":"ext4","storage":"lvm","volgroup":"instance-store-vg"}` |
| [`storage.storageClass.provisioner`](#storagestorageclassprovisioner) |  |
| [`storage.storageClass.reclaimPolicy`](#storagestorageclassreclaimpolicy) | `Delete` |
| [`storage.storageClass.volumeBindingMode`](#storagestorageclassvolumebindingmode) | `WaitForFirstConsumer` |
| [`telemetry.enabled`](#telemetryenabled) | `true` |
| [`telemetry.segmentApiKey`](#telemetrysegmentapikey) | `hMWi3sZ17KFMjn2sPWo9UJGpOQqiba4A` |
| [`telemetry.segmentClientSide`](#telemetrysegmentclientside) | `true` |
| [`tls.defaultCertificateSpecs`](#tlsdefaultcertificatespecs) | `{}` |

## Parameters

### `balancerd` parameters

#### balancerd.enabled

**Default**: `true`

Flag to indicate whether to create balancerd pods for the environments

#### balancerd.nodeSelector

**Default**: `{}`

Node selector to use for balancerd pods spawned by the operator

### `clusterd` parameters

#### clusterd.nodeSelector

**Default**: `{}`

Node selector to use for clusterd pods spawned by the operator

### `console` parameters

#### console.enabled

**Default**: `true`

Flag to indicate whether to create console pods for the environments

#### console.imageTagMapOverride

**Default**: `{}`

Override the mapping of environmentd versions to console versions

#### console.nodeSelector

**Default**: `{}`

Node selector to use for console pods spawned by the operator

### `environmentd` parameters

#### environmentd.nodeSelector

**Default**: `{}`

Node selector to use for environmentd pods spawned by the operator

### `networkPolicies` parameters

#### networkPolicies.egress

**Default**: `{“cidrs”:[“0.0.0.0/0”],“enabled”:false}`

egress from Materialize pods to sources and sinks

#### networkPolicies.enabled

**Default**: `false`

Whether to enable network policies for securing communication between
pods

#### networkPolicies.ingress

**Default**: `{“cidrs”:[“0.0.0.0/0”],“enabled”:false}`

Whether to enable ingress to the SQL and HTTP interfaces on environmentd
or balancerd

#### networkPolicies.internal

**Default**: `{“enabled”:false}`

Whether to enable internal communication between Materialize pods

### `observability` parameters

#### observability.enabled

**Default**: `true`

Whether to enable observability features

#### observability.podMetrics.enabled

**Default**: `false`

Whether to enable the pod metrics scraper which populates the
Environment Overview Monitoring tab in the web console (requires
metrics-server to be installed)

#### observability.prometheus.scrapeAnnotations.enabled

**Default**: `true`

Whether to annotate pods with common keys used for prometheus scraping.

### `operator` parameters

#### operator.args.enableInternalStatementLogging

**Default**: `true`

#### operator.args.startupLogFilter

**Default**: `INFO,mz_orchestratord=TRACE`

Log filtering settings for startup logs

#### operator.cloudProvider.providers.aws.accountID

**Default**:

When using AWS, accountID is required

#### operator.cloudProvider.providers.aws.enabled

**Default**: `false`

#### operator.cloudProvider.providers.aws.iam.roles.connection

**Default**:

ARN for CREATE CONNECTION feature

#### operator.cloudProvider.providers.aws.iam.roles.environment

**Default**:

ARN of the IAM role for environmentd

#### operator.cloudProvider.providers.gcp

**Default**: `{“enabled”:false}`

GCP Configuration (placeholder for future use)

#### operator.cloudProvider.region

**Default**: `kind`

Common cloud provider settings

#### operator.cloudProvider.type

**Default**: `local`

Specifies cloud provider. Valid values are ‘aws’, ‘gcp’, ‘azure’,
‘generic’, or ’local’

#### operator.clusters.defaultReplicationFactor.analytics

**Default**: `0`

#### operator.clusters.defaultReplicationFactor.probe

**Default**: `0`

#### operator.clusters.defaultReplicationFactor.support

**Default**: `0`

#### operator.clusters.defaultReplicationFactor.system

**Default**: `0`

#### operator.clusters.defaultSizes.analytics

**Default**: `25cc`

#### operator.clusters.defaultSizes.catalogServer

**Default**: `25cc`

#### operator.clusters.defaultSizes.default

**Default**: `25cc`

#### operator.clusters.defaultSizes.probe

**Default**: `mz_probe`

#### operator.clusters.defaultSizes.support

**Default**: `25cc`

#### operator.clusters.defaultSizes.system

**Default**: `25cc`

#### operator.image.pullPolicy

**Default**: `IfNotPresent`

Policy for pulling the image: “IfNotPresent” avoids unnecessary
re-pulling of images

#### operator.image.repository

**Default**: `materialize/orchestratord`

The Docker repository for the operator image

#### operator.image.tag

**Default**: `v0.138.0`

The tag/version of the operator image to be used

#### operator.nodeSelector

**Default**: `{}`

#### operator.resources.limits

**Default**: `{“memory”:“512Mi”}`

Resource limits for the operator’s CPU and memory

#### operator.resources.requests

**Default**: `{“cpu”:“100m”,“memory”:“512Mi”}`

Resources requested by the operator for CPU and memory

#### operator.secretsController

**Default**: `kubernetes`

Which secrets controller to use for storing secrets. Valid values are
‘kubernetes’ and ‘aws-secrets-manager’. Setting ‘aws-secrets-manager’
requires a configured AWS cloud provider and IAM role for the
environment with Secrets Manager permissions.

### `rbac` parameters

#### rbac.create

**Default**: `true`

Whether to create necessary RBAC roles and bindings

### `schedulerName` parameters

#### schedulerName

**Default**: `nil`

Optionally use a non-default kubernetes scheduler.

### `serviceAccount` parameters

#### serviceAccount.create

**Default**: `true`

Whether to create a new service account for the operator

#### serviceAccount.name

**Default**: `orchestratord`

The name of the service account to be created

### `storage` parameters

#### storage.storageClass.allowVolumeExpansion

**Default**: `false`

#### storage.storageClass.create

**Default**: `false`

Set to false to use an existing StorageClass instead. Refer to the
[Kubernetes StorageClass
documentation](https://kubernetes.io/docs/concepts/storage/storage-classes/)

#### storage.storageClass.name

**Default**:

Name of the StorageClass to create/use: eg
“openebs-lvm-instance-store-ext4”

#### storage.storageClass.parameters

**Default**:
`{“fsType”:“ext4”,“storage”:“lvm”,“volgroup”:“instance-store-vg”}`

Parameters for the CSI driver

#### storage.storageClass.provisioner

**Default**:

CSI driver to use, eg “local.csi.openebs.io”

#### storage.storageClass.reclaimPolicy

**Default**: `Delete`

#### storage.storageClass.volumeBindingMode

**Default**: `WaitForFirstConsumer`

### `telemetry` parameters

#### telemetry.enabled

**Default**: `true`

#### telemetry.segmentApiKey

**Default**: `hMWi3sZ17KFMjn2sPWo9UJGpOQqiba4A`

#### telemetry.segmentClientSide

**Default**: `true`

### `tls` parameters

#### tls.defaultCertificateSpecs

**Default**: `{}`

## See also

- [Installation](/docs/self-managed/v25.2/installation/)
- [Troubleshooting](/docs/self-managed/v25.2/installation/troubleshooting/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/installation/configuration.md"
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
