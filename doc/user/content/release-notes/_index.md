---
title: "Release notes"
description: "Release notes for Self-managed Materialize"
menu:
  main:
    weight: 50
    name: "Release notes"
    identifier: "release-notes"
---

## **Upcoming**

{{% self-managed/self-managed-details %}}

### Materialize Operator Helm Chart

As part of the Self-managed Materialize, Materialize provides:

{{% self-managed/versions/self-managed-products %}}

## Self-managed versioning and lifecycle

Self-managed Materialize uses a calendar versioning (calver) scheme of the form
`vYY.R.PP` where:

- `YY` indicates the year.
- `R` indicates major release.
- `PP` indicates the patch number.

For Self-managed Materialize, Materialize supports the latest 2 major releases.

## Known Limitations

| Item                                    | Status      |
|-----------------------------------------|-------------|
| **IAM** <br>Built-in authorization mechanisms. | In progress |
| **License Compliance** <br>License key support to make it easier to comply with license terms. | In progress |
| **Spill to disk** <br> Cloud feature that enables Materialize to support workloads that are larger than can fit into memory. | In progress |
| **Ingress from outside cluster** <br> Provide Terraform modules to set up ingress from outside the Kubernetes cluster hosting self-managed Materialize. | In progress |
| **Temporal Filtering** <br> Memory optimizations for filtering time-series data are not yet implemented. | |
