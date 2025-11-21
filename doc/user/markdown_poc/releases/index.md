<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

# Releases

## Self-Managed v26.0.0

*Released: 2025-11-18*

### Swap support

Starting in v26.0.0, Self-Managed Materialize enables swap by default.
Swap allows for infrequently accessed data to be moved from memory to
disk. Enabling swap reduces the memory required to operate Materialize
and improves cost efficiency.

To facilitate upgrades from v25.2, Self-Managed Materialize added new
labels to the node selectors for `clusterd` pods:

- To upgrade using Materialize-provided Terraforms, upgrade your
  Terraform version to `v0.6.1`:

  - [AWS Terraform v0.6.1 Upgrade
    Notes](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#v061).
  - [GCP Terraform v0.6.1 Upgrade
    Notes](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#v061).
  - [Azure Terraform v0.6.1 Upgrade
    Notes](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#v061).

- To upgrade if **not** using a Materialize-provided Terraforms, you
  must prepare your nodes by adding the required labels. For detailed
  instructions, see [Prepare for swap and upgrade to
  v26.0](/docs/installation/upgrade-to-swap/).

### SASL/SCRAM-SHA-256 support

Starting in v26.0.0, Self-Managed Materialize supports
SASL/SCRAM-SHA-256 authentication for PostgreSQL wire protocol
connections. For more information, see
[Authentication](/docs/security/self-managed/authentication/).

When SASL authentication is enabled:

- **PostgreSQL connections** (e.g., `psql`, client libraries,
  [connection poolers](/docs/integrations/connection-pooling/)) use
  SCRAM-SHA-256 authentication
- **HTTP/Web Console connections** use standard password authentication

This hybrid approach provides maximum security for SQL connections while
maintaining compatibility with web-based tools.

### License Key

Starting in v26.0.0, Self-Managed Materialize requires a license key.

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>License key type</th>
<th>Deployment type</th>
<th>Action</th>
</tr>
</thead>
<tbody>
<tr>
<td>Community</td>
<td>New deployments</td>
<td><p>To get a license key:</p>
<ul>
<li>If you have a Cloud account, visit the <a
href="https://console.materialize.com/license/"><strong>License</strong>
page in the Materialize Console</a>.</li>
<li>If you do not have a Cloud account, visit <a
href="https://materialize.com/self-managed/community-license/">https://materialize.com/self-managed/community-license/</a>.</li>
</ul></td>
</tr>
<tr>
<td>Community</td>
<td>Existing deployments</td>
<td>Contact <a href="https://materialize.com/docs/support/">Materialize
support</a>.</td>
</tr>
<tr>
<td>Enterprise</td>
<td>New deployments</td>
<td>Visit <a
href="https://materialize.com/self-managed/enterprise-license/">https://materialize.com/self-managed/enterprise-license/</a>
to purchase an Enterprise license.</td>
</tr>
<tr>
<td>Enterprise</td>
<td>Existing deployments</td>
<td>Contact <a href="https://materialize.com/docs/support/">Materialize
support</a>.</td>
</tr>
</tbody>
</table>

For new deployments, you configure your license key in the Kubernetes
Secret resource during the installation process. For details, see the
[installation guides](/docs/installation/). For existing deployments,
you can configure your license key via:

<div class="highlight">

``` chroma
kubectl -n materialize-environment patch secret materialize-backend -p '{"stringData":{"license_key":"<your license key goes here>"}}' --type=merge
```

</div>

### PostgreSQL: Source versioning

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

For PostgreSQL sources, starting in v26.0.0, Materialize introduces new
syntax for [`CREATE SOURCE`](/docs/sql/create-source/postgres-v2/) and
[`CREATE TABLE`](/docs/sql/create-table/) to allow better handle DDL
changes to the upstream PostgreSQL tables.

<div class="note">

**NOTE:**

- This feature is currently supported for PostgreSQL sources, with
  additional source types coming soon.

- Changing column types is currently unsupported.

</div>

To create a source from an external PostgreSQL:

<div class="highlight">

``` chroma
CREATE SOURCE [IF NOT EXISTS] <source_name>
[IN CLUSTER <cluster_name>]
FROM POSTGRES CONNECTION <connection_name> (PUBLICATION '<publication_name>')
;
```

</div>

To create a read-only table from a [source](/docs/sql/create-source/)
connected (via native connector) to an external PostgreSQL:

<div class="highlight">

``` chroma
CREATE TABLE [IF NOT EXISTS] <table_name> FROM SOURCE <source_name> (REFERENCE <upstream_table>)
[WITH (
    TEXT COLUMNS (<column_name> [, ...])
  | EXCLUDE COLUMNS (<column_name> [, ...])
  [, ...]
)]
;
```

</div>

For more information, see:

- [Guide: Handling upstream schema changes with zero
  downtime](/docs/ingest-data/postgres/source-versioning/)
- [`CREATE SOURCE`](/docs/sql/create-source/postgres-v2/)
- [`CREATE TABLE`](/docs/sql/create-table/)

### Deprecation

The `inPlaceRollout` setting has been deprecated and will be ignored.
Instead, use the new setting `rolloutStrategy` to specify either:

- `WaitUntilReady` (*Default*)
- `ImmediatelyPromoteCausingDowntime`

For more information, see
[`rolloutStrategy`](/docs/installation/#rollout-strategies).

### Terraform helpers

Corresponding to the v26.0.0 release, the following versions of the
sample Terraform modules have been released:

| Sample Module | Description |
|----|----|
| [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize) | A sample Terraform module for installing the Materialize Helm chart into a Kubernetes cluster. |
| [Materialize on AWS](https://github.com/MaterializeInc/terraform-aws-materialize) | A sample Terraform module for deploying Materialize on AWS Cloud Platform with all required infrastructure components. See [Install on AWS](/docs/installation/install-on-aws/) for an example usage. |
| [Materialize on Azure](https://github.com/MaterializeInc/terraform-azurerm-materialize) | A sample Terraform module for deploying Materialize on Azure with all required infrastructure components. See [Install on Azure](/docs/installation/install-on-azure/) for an example usage. |
| [Materialize on Google Cloud Platform (GCP)](https://github.com/MaterializeInc/terraform-google-materialize) | A sample Terraform module for deploying Materialize on Google Cloud Platform (GCP) with all required infrastructure components. See [Install on GCP](/docs/installation/install-on-gcp/) for an example usage. |

<div class="code-tabs">

<div class="tab-content">

<div id="tab-materialize-on-aws" class="tab-pane"
title="Materialize on AWS">

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
href="https://github.com/MaterializeInc/terraform-aws-materialize/releases/tag/v0.6.4">v0.6.4</a></td>
<td><ul>
<li>Released as part of v26.0.0.</li>
<li>Uses <code>terraform-helm-materialize</code> version
<code>v0.1.35</code>.</li>
</ul></td>
</tr>
</tbody>
</table>

If upgrading from a deployment that was set up using an earlier version
of the Terraform modules, additional considerations may apply when using
an updated Terraform modules to your existing deployments.

Click on the Terraform version link to go to the release-specific
Upgrade Notes.

</div>

<div id="tab-materialize-on-azure" class="tab-pane"
title="Materialize on Azure">

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
href="https://github.com/MaterializeInc/terraform-azurerm-materialize/releases/tag/v0.6.4">v0.6.4</a></td>
<td><ul>
<li>Released as part of v26.0.0.</li>
<li>Uses <code>terraform-helm-materialize</code> version
<code>v0.1.35</code>.</li>
</ul></td>
</tr>
</tbody>
</table>

If upgrading from a deployment that was set up using an earlier version
of the Terraform modules, additional considerations may apply when using
an updated Terraform modules to your existing deployments.

See also Upgrade Notes for release specific notes.

</div>

<div id="tab-materialize-on-gcp" class="tab-pane"
title="Materialize on GCP">

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
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/tag/v0.6.4">v0.6.4</a></td>
<td><ul>
<li>Released as part of v26.0.0.</li>
<li>Uses <code>terraform-helm-materialize</code> version
<code>v0.1.35</code>.</li>
</ul></td>
</tr>
</tbody>
</table>

If upgrading from a deployment that was set up using an earlier version
of the Terraform modules, additional considerations may apply when using
an updated Terraform modules to your existing deployments.

See also Upgrade Notes for release specific notes.

</div>

<div id="tab-terraform-helm-materialize" class="tab-pane"
title="terraform-helm-materialize">

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>terraform-helm-materialize</th>
<th>Notes</th>
<th>Release date</th>
</tr>
</thead>
<tbody>
<tr>
<td>v0.1.35</td>
<td><ul>
<li>Released as part of v26.0.0.</li>
<li>Uses as default Materialize Operator version:
<code>v26.0.0</code></li>
</ul></td>
<td>2025-11-18</td>
</tr>
</tbody>
</table>

</div>

</div>

</div>

#### Upgrade notes for v26.0.0

- Upgrading to `v26.0.0` is a major version upgrade. To upgrade to
  `v26.0` from `v25.2.X` or `v25.1`, you must first upgrade to
  `v25.2.16` and then upgrade to `v26.0.0`.

- For upgrades, the `inPlaceRollout` setting has been deprecated and
  will be ignored. Instead, use the new setting `rolloutStrategy` to
  specify either:

  - `WaitUntilReady` (*Default*)
  - `ImmediatelyPromoteCausingDowntime`

  For more information, see
  [`rolloutStrategy`](/docs/installation/#rollout-strategies).

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
    v26.0](/docs/installation/upgrade-to-swap/).

See also [General notes for
upgrades](/docs/installation/#general-notes-for-upgrades).

## Self-managed release versions

| Materialize Operator | orchestratord version | environmentd version | Release date | Notes |
|----|----|----|----|----|
| v26.0.0 | v26.0.0 | v26.0.0 | 2025-11-18 | See [v26.0.0 release notes](/docs/releases/#self-managed-v2600) |

## See also

- [Cloud Upgrade Schedule](/docs/releases/cloud-upgrade-schedule/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/releases/_index.md"
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
