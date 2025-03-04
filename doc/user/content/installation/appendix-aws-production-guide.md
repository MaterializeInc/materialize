---
title: "Production guide"
description: "Production guide for Self-Managed Materialize AWS deployment."
draft: true

---

The sample Materialize on AWS Terraform module are provided as a starting point
for evaluation. The Terraform module:

- Does not use encryption for object storage.
- Does not use TLS for traffic.
- Does not implement RBAC (Role-Based Access Control).
- Does not include explicit OS version upgrade logic.
- Does not include Kubernetes upgrade logic.
- Relies on port forwarding for access.


<!-- Don't worry about formatting, grammar, etc. Can be bulletpoints or whatever you find easiest to draft; we can do a handover once we have some content -->
