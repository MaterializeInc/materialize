---
audience: developer
canonical_url: https://materialize.com/docs/installation/install-on-local-kind/upgrade-on-local-kind/
complexity: intermediate
description: Upgrade Materialize running locally on a kind cluster.
doc_type: reference
keywords:
- v27
- v26
- Upgrade on kind
- v28
- 'Important:'
product_area: Deployment
status: stable
title: Upgrade on kind
---

# Upgrade on kind

## Purpose
Upgrade Materialize running locally on a kind cluster.

If you need to understand the syntax and options for this command, you're in the right place.


Upgrade Materialize running locally on a kind cluster.


To upgrade your Materialize instances, first choose a new operator version and upgrade the Materialize operator. Then, upgrade your Materialize instances to the same version. The following tutorial upgrades your Materialize deployment running locally on a [`kind`](https://kind.sigs.k8s.io/)
cluster.

The tutorial assumes you have installed Materialize on `kind` using the
instructions on [Install locally on kind](/installation/install-on-local-kind/).

> **Important:** 

When performing major version upgrades, you can upgrade only one major version
at a time. For example, upgrades from **v26**.1.0 to **v27**.2.0 is permitted
but **v26**.1.0 to **v28**.0.0 is not. Skipping major versions or downgrading is
not supported. To upgrade from v25.2 to v26.0, you must [upgrade first to v25.2.16+](../self-managed/v25.2/release-notes/#v25216).


## Prerequisites

This section covers prerequisites.

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, install. For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl`
documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### License key

Starting in v26.0, Materialize requires a license key. If your existing
deployment does not have a license key configured, contact [Materialize support](../support/).


## Upgrade

> **Important:** 

The following procedure performs a rolling upgrade, where both the old and new
Materialize instances are running before the the old instance are removed.
When performing a rolling upgrade, ensure you have enough resources to support
having both the old and new Materialize instances running.


<!-- Unresolved shortcode: {{% self-managed/versions/upgrade/upgrade-steps-lo... -->

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Installation](/installation/)