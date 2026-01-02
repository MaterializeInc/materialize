---
title: Install/Upgrade (Self-Managed)
description: Installation and upgrade guides for Self-Managed Materialize
doc_type: howto
product_area: Deployment
audience: operator
status: stable
complexity: intermediate
keywords:
  - installation
  - self-managed
  - kubernetes
  - helm
  - terraform
  - upgrade
canonical_url: https://materialize.com/docs/installation/
---

# Install/Upgrade (Self-Managed)

## Purpose
This section contains installation and upgrade guides for Self-Managed Materialize. Self-Managed Materialize runs on Kubernetes clusters locally or on cloud providers.

If you're deploying Materialize in your own infrastructure, start here.

## When to use
- Installing Materialize on AWS, Azure, or GCP
- Setting up a local development environment
- Upgrading an existing installation
- Configuring the Materialize Operator

## Prerequisites

Self-Managed Materialize requires:
- Kubernetes 1.25+
- Helm 3.x
- Object storage (S3, Azure Blob, or GCS)
- PostgreSQL-compatible metadata storage
- A valid license key (starting v26.0)

## Installation Guides

### Cloud Providers
- [Install on AWS (via Terraform)](install-on-aws/index.md)
- [Install on Azure (via Terraform)](install-on-azure/index.md)
- [Install on GCP (via Terraform)](install-on-gcp/index.md)

### Local Development
- [Install locally on kind (via Helm)](install-on-local-kind/index.md)

## Upgrade Guides

- [Upgrade on AWS](install-on-aws/upgrade-on-aws/index.md)
- [Upgrade on Azure](install-on-azure/upgrade-on-azure/index.md)
- [Upgrade on GCP](install-on-gcp/upgrade-on-gcp/index.md)
- [Upgrade on local kind](install-on-local-kind/upgrade-on-local-kind/index.md)
- [Prepare for swap and upgrade to v26.0](upgrade-to-swap/index.md)

## Configuration

- [Materialize Operator Configuration](configuration/index.md) — Configure the Kubernetes operator
- [Release Versions](release-versions/index.md) — Available versions

## Reference

### Appendices
- [Cluster Sizes](appendix-cluster-sizes/index.md) — Compute resource sizing
- [Terraform Configurations](appendix-terraforms/index.md) — Terraform module reference
- [Materialize CRD Field Descriptions](appendix-materialize-crd-field-descriptions/index.md) — Custom resource definitions

### Cloud-Specific Configuration
- [AWS Configuration](install-on-aws/appendix-aws-configuration/index.md)
- [AWS Deployment Guidelines](install-on-aws/appendix-deployment-guidelines/index.md)
- [Azure Configuration](install-on-azure/appendix-azure-configuration/index.md)
- [Azure Deployment Guidelines](install-on-azure/appendix-deployment-guidelines/index.md)
- [GCP Configuration](install-on-gcp/appendix-gcp-configuration/index.md)
- [GCP Deployment Guidelines](install-on-gcp/appendix-deployment-guidelines/index.md)

## Troubleshooting

- [Installation FAQ](faq/index.md)
- [Troubleshooting](troubleshooting/index.md)
- [Operational Guidelines](operational-guidelines/index.md)

## Key Takeaways

- Self-Managed Materialize runs on Kubernetes with the Materialize Operator
- Use Terraform for cloud deployments, Helm for local development
- A license key is required starting in v26.0
- Always check upgrade notes before upgrading to a new version
