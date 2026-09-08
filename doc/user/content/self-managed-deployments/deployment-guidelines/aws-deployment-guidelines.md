---
title: "AWS deployment guidelines"
description: "General guidelines when deploying Self-Managed Materialize on AWS."
disable_list: true
menu:
  main:
    parent: "deployment-guidelines"
    identifier: "aws-deployment-guidelines"
    weight: 10
aliases:
  - /installation/install-on-aws/appendix-deployment-guidelines/
---

{{% self-managed/materialize-components-sentence %}}

## Recommended instance types

As a general guideline, we recommend:

- ARM-based CPU
- A 1:8 ratio of vCPU to GiB memory.
- At least a 2:1 ratio of GiB local instance storage to GiB memory when using swap.

{{% self-managed/aws-recommended-instances %}}

## Locally-attached NVMe storage

Configuring swap on nodes to use locally-attached NVMe storage allows
Materialize to spill to disk when operating on datasets larger than main memory.
This setup can provide significant cost savings and provides a more graceful
degradation rather than OOMing. Network-attached storage (like EBS volumes) can
significantly degrade performance and is not supported.

### Swap support

The Materialize [Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws/examples/simple) supports configuring swap out of the box.

## Recommended metadata database sizing

{{< include-md file="content/headless/self-managed-deployments/metadata-database-sizing.md" >}}

### RDS instance types

For the RDS PostgreSQL metadata database, we recommend:

- **Graviton (ARM)** memory-optimized instances (the `r6g` / `r7g` families).
- **Multi-AZ** for production.
- **gp3** storage.

| Deployment size | Instance | vCPU / memory | Storage | Provisioned IOPS | Continuously-active objects (~60% CPU) |
|---|---|---|---|---|---|
| Entry / small production | `db.r6g.large` | 2 / 16 GiB | 200 GiB | 3,000 (baseline) | ~4,500 |
| Recommended default | `db.r6g.2xlarge` | 8 / 64 GiB | 400 GiB | 6,000 | ~18,000 |

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## Upgrading guideline

{{% include-headless "/headless/self-managed-deployments/general-rules-for-upgrades" %}}

## Karpenter node expiry

We recommend setting `expire_after` to `Never` on the Materialize nodepool
since node expiry is not a voluntary disruption. With any other value,
Karpenter removes nodes that reach their configured lifetime even if they run
pods annotated with `karpenter.sh/do-not-disrupt`. This can cause downtime
unless you gracefully roll the nodes first. The [Materialize Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed)
default `expire_after` to `Never`.

## Karpenter termination grace period

We recommend leaving `termination_grace_period` unset on nodepools that run
Materialize workloads. When this value is set, Karpenter terminates nodes after
the configured grace period following any change to the nodepool
configuration, even if they run pods annotated with
`karpenter.sh/do-not-disrupt`.

Before v6.0.0, the modules set `termination_grace_period` to `300s`. If you are
using a version earlier than v6.0.0, upgrade to v6.0.0 using the [v6.0.0
upgrade
notes](https://github.com/MaterializeInc/materialize-terraform-self-managed/blob/v6.0.0/README.md#v600).
Starting in v6.0.0, the Materialize Terraform modules leave
`termination_grace_period` unset by default.

## Node pool resizing

{{% include-headless "/headless/self-managed-deployments/resize-node-pool" %}}
