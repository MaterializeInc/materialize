+++
title = "Bring Your Own Cloud (BYOC)"
description = "Run Materialize in your own cloud account, managed by Materialize."
disable_list = true

[menu.main]
parent = "byoc"
name = "Overview"
weight = 5
+++

Draft, work in progress. Shared for feedback on the BYOC MVP design; flows and details will change.

Bring Your Own Cloud (BYOC) runs a full Materialize environment inside your own cloud account, while Materialize manages provisioning, upgrades, and operations from its control plane. Your data never leaves your account.

BYOC is a good fit if you need data residency, network isolation, direct control of the underlying infrastructure for your compliance requirements, or direct cloud cost visibility. GCP is in development first, with AWS to follow; Azure is on the roadmap.

## Guides

- [BYOC on GCP](/byoc/byoc-gcp-draft/)
- [BYOC on AWS](/byoc/byoc-aws-draft/)
