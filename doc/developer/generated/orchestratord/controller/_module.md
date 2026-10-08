---
source: src/orchestratord/src/controller.rs
revision: bb5c454adc
---

# mz-orchestratord::controller

Module declaration for the three Kubernetes controllers managed by orchestratord: `materialize`, `balancer`, and `console`.
Each submodule provides a `Config` struct, reconciler logic, and the Kubernetes resource management for its respective custom resource.
