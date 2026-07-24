---
title: Sources
description: Learn about sources in Materialize.
menu:
  main:
    parent: concepts
    weight: 10
    identifier: 'concepts-sources'
aliases:
  - /get-started/key-concepts/#sources
  - /self-managed/v25.2/concepts/sources/
---

## Overview

{{% include-headless "/headless/source-definition" %}}

### Connectors

Materialize bundles native connectors for the following external systems:

{{< include-md file="shared-content/multilink-box-native-connectors.md" >}}

## Sources and clusters

Sources require compute resources in Materialize, and so need to be associated
with a [cluster](/concepts/clusters/). If possible, dedicate a cluster just for
sources.

See also [Operational guidelines](/manage/operational-guidelines/).

## Related pages

- [`CREATE SOURCE`](/sql/create-source)
