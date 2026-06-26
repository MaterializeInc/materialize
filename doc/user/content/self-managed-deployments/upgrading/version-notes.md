---
title: "Version-specific upgrade notes"
description: "Version-specific notes for upgrading Self-Managed Materialize."
menu:
  main:
    parent: "upgrading"
    weight: 5
---

Review the notes for your target version before upgrading. For the general
upgrade procedure, see the [upgrade
guides](/self-managed-deployments/upgrading/#upgrade-guides).

## Upgrading to `v26.30` and later versions

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.30.md" >}}

## Upgrading to `v26.1` and later versions

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.1.md" >}}

## Upgrading to `v26.0`

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.0.md" >}}

## Upgrading between minor versions less than `v26`

- Prior to `v26`, you must upgrade at most one minor version at a time. For
  example, upgrading from `v25.1.5` to `v25.2.16` is permitted.
