---
audience: developer
canonical_url: https://materialize.com/docs/installation/faq/
complexity: intermediate
description: Frequently asked questions about self-managed installations.
doc_type: troubleshooting
keywords:
- 'FAQ: Self-managed installation'
product_area: Deployment
status: stable
title: 'FAQ: Self-managed installation'
---

# FAQ: Self-managed installation

## Purpose
Frequently asked questions about self-managed installations.

This page provides detailed documentation for this topic.


Frequently asked questions about self-managed installations.



## How long do license keys last?

Community edition license keys are valid for one year. Enterprise license
keys will vary based on the terms of your contract.

## How do I get a license key?

<!-- Dynamic table: self_managed/license_key - see original docs -->

## How do I add a license key to an existing installation?

The license key should be configured in the Kubernetes Secret resource
created during the installation process. To configure a license key in an
existing installation, run:

```bash
kubectl -n materialize-environment patch secret materialize-backend -p '{"stringData":{"license_key":"<your license key goes here>"}}' --type=merge
```

