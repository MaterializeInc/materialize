---
title: "Release notes"
description: "Release notes for Self-managed Materialize"
menu:
  main:
    weight: 50
    name: "Release notes"
    identifier: "release-notes"
---

## v25.1

{{% self-managed/self-managed-editions-table %}}

### Materialize Operator

{{% self-managed/versions/self-managed-products %}}

#### Releases and Compatibility

{{< yaml-table data="self_managed/self_managed_operator_compatibility" >}}

### Terraform helpers

To help you get started, Materialize also provides some template Terraforms.

{{< important >}}
These modules are intended for evaluation/demonstration purposes and for serving
as a template when building your own production deployment. The modules should
not be directly relied upon for production deployments: **future releases of the
modules will contain breaking changes.** Instead, to use as a starting point for
your own production deployment, either:

- Fork the repo and pin to a specific version; or

- Use the code as a reference when developing your own deployment.

{{</ important >}}

{{< yaml-table data="self_managed/terraform_list" >}}

### Known Limitations

| Item                                    | Status      |
|-----------------------------------------|-------------|
| **IAM** <br>Built-in authorization mechanisms. | In progress |
| **License Compliance** <br>License key support to make it easier to comply with license terms. | In progress |
| **Spill to disk** <br> Provide Terraform modules to set up  locally-attached NVMe storage to support workloads that are larger than can fit into memory. | <ul><li>AWS: Available in Materialize on AWS Terraform v0.3.1+</li><li>GCP: In progress</li><li>Azure: In progress</li><ul> |
| **Ingress from outside cluster** <br> Provide Terraform modules to set up ingress from outside the Kubernetes cluster hosting self-managed Materialize. | <ul><li>AWS: Available in Materialize on AWS Terraform v0.3.0+</li><li>GCP: In progress</li><li>Azure: In progress</li><ul> |
| **AWS Connections** <br> AWS connections require backing cluster that hosts Materialize to be AWS EKS.  | |
| **EKS/Azure Connections** | |
| **Temporal Filtering** <br> Memory optimizations for filtering time-series data are not yet implemented. | |

## Self-managed versioning and lifecycle

Self-managed Materialize uses a calendar versioning (calver) scheme of the form
`vYY.R.PP` where:

- `YY` indicates the year.
- `R` indicates major release.
- `PP` indicates the patch number.

For Self-managed Materialize, Materialize supports the latest 2 major releases.
