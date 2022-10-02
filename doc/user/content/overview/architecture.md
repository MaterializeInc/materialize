---
title: "Architecture overview"
description: "Understand the different components that make Materialize a horizontally scalable, highly available distributed system."
draft: true
menu:
  main:
    parent: overview
    weight: 10
---

Materialize is a horizontally scalable, highly available distributed system. It separates storage and compute into different layers, allowing data ingestion, computation, and query serving to be **performed and scaled independently**.

{{<
    figure src="/images/architecture_overview.jpg"
    alt="Materialize architecture overview"
    width="700"
>}}

## Logical components

The architecture is broken into three logical components:

### Storage

### Compute

### Adapter

## Learn more

- [Key Concepts](/overview/key-concepts/) to better understand Materialize's API
- [Get started](/get-started) to try out Materialize
