+++
title = "Bring Your Own Cloud (BYOC)"
description = "Run Materialize in your own cloud account, managed by Materialize."

[menu.main]
parent = "byoc"
name = "Overview"
weight = 5
+++

Draft, work in progress. Shared for feedback on the BYOC MVP design; flows and details will change.

Bring Your Own Cloud (BYOC) runs a full Materialize environment inside your own cloud account, while Materialize manages provisioning, upgrades, and operations from its control plane. Your data never leaves your account.

BYOC is a good fit if you need data residency, network isolation, compliance (SOC 2 / HIPAA), or direct cloud cost visibility. AWS is available first; GCP and Azure are on the roadmap.

## Guides

- [BYOC on AWS](/byoc/byoc-aws-draft/)
