---
title: "Appendix: Alternative cluster architectures"
description: "If the recommended 3-tier architecture is infeasible, can use a 2-cluster or a 1-cluster pattern."
menu:
  main:
    parent: "operational-guidelines"
    weight: 5
aliases:
  - /manage/appendix-alternative-cluster-architectures/
---

If the [recommended three-tier
architecture](/clusters/operational-guidelines/#three-tier-architecture)
is infeasible or unnecessary due to low volume or a **non**-production setup, a
two-tier or a one-tier architecture may suffice.

## Two-tier architecture

{{% include-from-yaml data="best_practices_details" name="architecture-two-tier" %}}

## One-tier architecture

{{% include-from-yaml data="best_practices_details" name="architecture-one-tier" %}}
