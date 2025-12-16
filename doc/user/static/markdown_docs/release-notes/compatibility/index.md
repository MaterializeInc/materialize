<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [v25.2 Release
notes](/docs/self-managed/v25.2/release-notes/)

</div>

# Release Compatibility

### Materialize Operator

For self-managed deployments, Materialize provides the Materialize
Operator. The Materialize Operator manages Materialize environments
within your Kubernetes infrastructure.

Materialize provides the Materialize Operator Helm Chart, a [Helm
Chart](https://helm.sh/docs/topics/charts/) for installing and upgrading
the Materialize Operator. The [chart
repository](https://helm.sh/docs/topics/chart_repository/) is:
`https://materializeinc.github.io/materialize`.

#### Releases and Compatibility

| Materialize Operator | orchestratord version | environmentd version | Release date | Notes |
|----|----|----|----|----|
| v25.2.16 | v0.163.1 | v0.147.20 | 2025-11-21 | Fixes potential issue with where environmentd crash during an upgrade can fence out environmentd. |
| v25.2.15 | v0.164.1 | v0.147.20 | 2025-11-11 | DO NOT USE |
| v25.2.12 | v0.161.0 | v0.147.18 | 2025-10-23 | Fix DDL lock being required when sequencing `ALTER SINK` |
| v25.2.11 | v0.159.1 | v0.147.17 | 2025-10-14 | Fixes issue where licenceKeyChecks were enabled by default |
| v25.2.10 | v0.159.1 | v0.147.17 | 2025-10-9 | Adds support for GCP using `COPY TO s3`, resolves console bug involving sources in object explorer, fixes 0dt cluster reconfiguration issue. |
| v25.2.9 | v0.159.0 | v0.147.17 | 2025-10-8 | Please use 25.2.10 |
| v25.2.5 | v0.154.0. | v0.147.6 | 2025-08-21 | Fixes license key issues, adds broader service account support. |
| v25.2.4 | v0.153.0 | v0.147.4 | 2025-07-15 | DO NOT USE |
| v25.2.3 | v0.147.5 | v0.147.4 | 2025-07-15 |  |
| v25.2.0 | v0.147.0 | v0.147.0 | 2025-06-24 | Support for password authentication. |
| v25.1.12 | v0.144.0 | v0.130.13 | 2025-05-15 |  |
| v25.1.11 | v0.143.0 | v0.130.12 | 2025-05-15 |  |
| v25.1.10 | v0.142.1 | v0.130.11 | 2025-04-24 |  |

### Terraform helpers

To help you get started, Materialize also provides some template
Terraforms.

<div class="important">

**! Important:**

These modules are intended for evaluation/demonstration purposes and for
serving as a template when building your own production deployment. The
modules should not be directly relied upon for production deployments:
**future releases of the modules will contain breaking changes.**
Instead, to use as a starting point for your own production deployment,
either:

- Fork the repo and pin to a specific version; or

- Use the code as a reference when developing your own deployment.

</div>

| Sample Module | Description |
|----|----|
| [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize) | A sample Terraform module for installing the Materialize Helm chart into a Kubernetes cluster. |
| [Materialize on AWS](https://github.com/MaterializeInc/terraform-aws-materialize) | A sample Terraform module for deploying Materialize on AWS Cloud Platform with all required infrastructure components. See [Install on AWS](/docs/self-managed/v25.2/installation/install-on-aws/) for an example usage. |
| [Materialize on Azure](https://github.com/MaterializeInc/terraform-azurerm-materialize) | A sample Terraform module for deploying Materialize on Azure with all required infrastructure components. See [Install on Azure](/docs/self-managed/v25.2/installation/install-on-azure/) for an example usage. |
| [Materialize on Google Cloud Platform (GCP)](https://github.com/MaterializeInc/terraform-google-materialize) | A sample Terraform module for deploying Materialize on Google Cloud Platform (GCP) with all required infrastructure components. See [Install on GCP](/docs/self-managed/v25.2/installation/install-on-gcp/) for an example usage. |

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
href="https://github.com/MaterializeInc/terraform-aws-materialize/releases/">v0.5.5</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.26.</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-aws-materialize/releases/">v0.5.4</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.25.</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-aws-materialize/releases/">v0.4.9</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.19.</li>
<li>Bumps Materialize release to <a
href="/docs/self-managed/v25.2/release-notes">self-managed 25.2</a></li>
<li>Adds support for password authentication and enabling RBAC</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-aws-materialize/releases/">v0.4.6</a></td>
<td><ul>
<li><p>Adds support for passing in additional Materialize instance
configuration options via <a
href="https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances"><code>environmentd_extra_args</code></a></p>
<p>To use, set the instance’s <code>environmentd_extra_env</code> to an
array of strings; for example:</p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>materialize_instances = [
  {
    ...
    environmentd_extra_args = [
      &quot;--system-parameter-default=&lt;param&gt;=&lt;value&gt;&quot;,
      &quot;--bootstrap-builtin-catalog-server-cluster-replica-size=50cc&quot;
    ]
  }
]</code></pre>
</div></li>
<li><p>Uses <code>terraform-helm-materialize</code> v0.1.15.</p></li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-aws-materialize/releases/tag/v0.4.5">v0.4.5</a></td>
<td><ul>
<li>Defaults to using Materialize Operator v25.1.12 (via
<code>terraform-helm-materialize</code> v0.1.14).</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-aws-materialize/releases/tag/v0.4.4">v0.4.4</a></td>
<td><ul>
<li>Defaults to using Materialize Operator v25.1.11 (via
<code>terraform-helm-materialize</code> v0.1.13).</li>
</ul></td>
</tr>
</tbody>
</table>

If upgrading from a deployment that was set up using an earlier version
of the Terraform modules, additional considerations may apply when using
an updated Terraform modules to your existing deployments.

See also [Upgrade
Notes](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#upgrade-notes)
for release-specific upgrade notes.

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
href="https://github.com/MaterializeInc/terraform-azurerm-materialize/releases/">v0.5.5</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.26.</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-azurerm-materialize/releases/">v0.5.4</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.25.</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-azurerm-materialize/releases/">v0.4.6</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.19.</li>
<li>Bumps Materialize release to <a
href="/docs/self-managed/v25.2/release-notes">self-managed 25.2</a></li>
<li>Adds support for password authentication and enabling RBAC</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-azurerm-materialize/releases/">v0.4.3</a></td>
<td><ul>
<li><p>Adds support for passing in additional Materialize instance
configuration options via <a
href="https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_materialize_instances"><code>environmentd_extra_args</code></a></p>
<p>To use, set the instance’s <code>environmentd_extra_env</code> to an
array of strings; for example:</p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>materialize_instances = [
  {
    ...
    environmentd_extra_args = [
      &quot;--system-parameter-default=&lt;param&gt;=&lt;value&gt;&quot;,
      &quot;--bootstrap-builtin-catalog-server-cluster-replica-size=50cc&quot;
    ]
  }
]</code></pre>
</div></li>
<li><p>Uses <code>terraform-helm-materialize</code> v0.1.15.</p></li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-azurerm-materialize/releases/tag/v0.4.1">v0.4.1</a></td>
<td><ul>
<li>Defaults to using Materialize Operator v25.1.11 (via
<code>terraform-helm-materialize</code> v0.1.13).</li>
</ul></td>
</tr>
</tbody>
</table>

If upgrading from a deployment that was set up using an earlier version
of the Terraform modules, additional considerations may apply when using
an updated Terraform modules to your existing deployments.

See also [Upgrade
Notes](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#upgrade-notes)
for release specific notes.

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
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/">v0.5.5</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.26.</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/">v0.5.4</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.25.</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/">v0.4.6</a></td>
<td><ul>
<li>Uses <code>terraform-helm-materialize</code> v0.1.19.</li>
<li>Bumps Materialize release to <a
href="/docs/self-managed/v25.2/release-notes">self-managed 25.2</a></li>
<li>Adds support for password authentication and enabling RBAC</li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/">v0.4.3</a></td>
<td><ul>
<li><p>Adds support for passing in additional Materialize instance
configuration options via <a
href="https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances"><code>environmentd_extra_args</code></a></p>
<p>To use, set the instance’s <code>environmentd_extra_env</code> to an
array of strings; for example:</p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>materialize_instances = [
  {
    ...
    environmentd_extra_args = [
      &quot;--system-parameter-default=&lt;param&gt;=&lt;value&gt;&quot;,
      &quot;--bootstrap-builtin-catalog-server-cluster-replica-size=50cc&quot;
    ]
  }
]</code></pre>
</div></li>
<li><p>Uses <code>terraform-helm-materialize</code> v0.1.15.</p></li>
</ul></td>
</tr>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/tag/v0.4.1">v0.4.1</a></td>
<td><ul>
<li>Defaults to using Materialize Operator v25.1.11 (via
<code>terraform-helm-materialize</code> v0.1.13).</li>
</ul></td>
</tr>
</tbody>
</table>

If upgrading from a deployment that was set up using an earlier version
of the Terraform modules, additional considerations may apply when using
an updated Terraform modules to your existing deployments.

See also [Upgrade
Notes](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#upgrade-notes)
for release specific notes.

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
<td>v0.1.26</td>
<td><ul>
<li>Uses as default Materialize Operator version:
<code>v0.159.1</code></li>
<li>Uses as default environmentd version: <code>v0.147.17</code></li>
</ul></td>
<td>2025-10-14</td>
</tr>
<tr>
<td>v0.1.25</td>
<td><ul>
<li>Uses as default Materialize Operator version:
<code>v0.159.1</code></li>
<li>Uses as default environmentd version: <code>v0.147.17</code></li>
</ul></td>
<td>2025-9-10</td>
</tr>
<tr>
<td>v0.1.16</td>
<td><ul>
<li>Adds support for passing in additional <code>environmentd</code>
configuration options.</li>
<li>Adds support for password authentication.</li>
<li>Uses as default Materialize Operator version:
<code>v25.2.0</code></li>
<li>Uses as default environmentd version: <code>v0.147.0</code></li>
</ul></td>
<td>2025-05-15</td>
</tr>
<tr>
<td>v0.1.15</td>
<td><ul>
<li>Adds support for passing in additional <code>environmentd</code>
configuration options.</li>
<li>Uses as default Materialize Operator version:
<code>v25.1.12</code></li>
<li>Uses as default environmentd version: <code>v0.130.13</code></li>
</ul></td>
<td>2025-05-15</td>
</tr>
<tr>
<td>v0.1.14</td>
<td><ul>
<li>Uses as default Materialize Operator version:
<code>v25.1.12</code></li>
<li>Uses as default environmentd version: <code>v0.130.13</code></li>
</ul></td>
<td>2025-05-15</td>
</tr>
<tr>
<td>v0.1.13</td>
<td><ul>
<li>Uses as default Materialize Operator version:
<code>v25.1.11</code></li>
<li>Uses as default environmentd version: <code>v0.130.12</code></li>
</ul></td>
<td>2025-05-15</td>
</tr>
<tr>
<td>v0.1.12</td>
<td><ul>
<li>Uses as default Materialize Operator version:
<code>v25.1.7</code></li>
<li>Uses as default environmentd version: <code>v0.130.8</code></li>
</ul></td>
<td>2025-04-08</td>
</tr>
</tbody>
</table>

</div>

</div>

</div>

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/release-notes/compatibility.md"
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
