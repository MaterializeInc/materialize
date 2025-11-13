---
title: "FAQ: Self-managed installation"
description: "Frequently asked questions about self-managed installations."
aliases:
  - /self-hosted/faq/
menu:
  main:
    parent: "installation"
    weight: 92
---

## How long do license keys last?

Community edition license keys are valid for one year. Enterprise license
keys will vary based on the terms of your contract.

## How do I get a community license key?

To request a license key for Materialize Community Edition, visit https://materialize.com/get-started/.

## How do I get an enterprise license key?

To purchase a Materialize Self-Managed Enterprise License, contact us
[here](https://materialize.com/self-managed).

## How do I add a license key to an existing installation?

The license key should be configured in the Kubernetes Secret resource
created during the installation process. To configure a license key in an
existing installation, run:

```bash
kubectl -n materialize-environment patch secret materialize-backend -p '{"stringData":{"license_key":"<your license key goes here>"}}' --type=merge
```
