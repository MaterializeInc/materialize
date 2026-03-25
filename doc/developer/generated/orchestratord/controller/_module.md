---
source: src/orchestratord/src/controller.rs
revision: 82d92a7fad
---

# mz-orchestratord::controller

Module declaration for the three Kubernetes controllers managed by orchestratord: `materialize`, `balancer`, and `console`.
Each submodule provides a `Config` struct, reconciler logic, and the Kubernetes resource management for its respective custom resource.
