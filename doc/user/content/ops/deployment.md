---
title: "Deployment"
description: "Find details about running your Materialize instances"
menu:
  main:
    parent: operations
---

_This page is a work in progress and will have more detail in the coming months.
If you have specific questions, feel free to [file a GitHub
issue](https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md)._

## Memory

Materialize stores the majority of its state in-memory, and works best when the streamed data
can be reduced in some way. For example, if you know that only a subset of your rows and columns
are relevant for your queries, it helps to avoid materializing sources or views until you've
expressed this to the system (we can avoid stashing that data, which can in some cases dramatically
reduce the memory footprint).

This is particularly important in Linux and in Docker, where swap may not be automatically
setup for you.

To minimize the chances that Materialize runs out of memory in a production environment,
we recommend you make additional memory available to Materialize via a SSD-backed
swap file or swap partition.
