---
title: "Self-managed Materialize"
description: ""
aliases:
  - /self-hosted/
robots: "noindex, nofollow"
---

With self-managed Materialize, you can deploy and operate Materialize in your
Kubernetes environment. For self-managed Materialize, Materialize offers:

- A Kubernetes Operator that manages your Materialize running in your Kubernetes
  environment.

- Materialize packaged as a containerized application that can be deployed in a
  Kubernetes cluster.

## Recommended instance types

Materialize has been tested to work on instances with the following properties:

- ARM-based CPU
- 1:8 ratio of vCPU to GiB memory
- 1:16 ratio of vCPU to GiB local instance storage (if enabling spill-to-disk)

When operating in AWS, we recommend:

- Using the `r7gd` and `r6gd` families of instances (and `r8gd` once available)
  when running with local disk

- Using the `r8g`, `r7g`, and `r6g` families when running without local disk

See also the [operational guidelines](/self-managed/operational-guidelines/).


## Installation

For instructions on installing Materialize on your Kubernetes cluster, see:

- [Install locally on kind](/self-managed/installation/install-on-kind/)
- [Install locally on minikube](/self-managed/installation/install-on-minikube/)
- [Install on AWS](/self-managed/installation/install-on-aws/)
- [Install on GCP](/self-managed/installation/install-on-gcp/)

## Related pages

<!-- Temporary:
Hugo will add links to the pages in the same folder.
Since we're hiding this section from the left-hand nav, adding the links here.
-->
