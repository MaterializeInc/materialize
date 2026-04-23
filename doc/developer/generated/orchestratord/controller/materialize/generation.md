---
source: src/orchestratord/src/controller/materialize/generation.rs
revision: f495cf372e
---

# mz-orchestratord::controller::materialize::generation

Manages version-specific generation logic for deploying `environmentd`.
Contains version constants and thresholds (e.g., `V140_DEV0`, `V143`, `V144`, `V147_DEV0`, `V153`, `V154_DEV0`, `V161`, `V26_1_0`) that gate feature flags and configuration changes based on the running Materialize version.
Builds the `StatefulSet`, `Service`, and `ConfigMap` resources for a specific generation of an `environmentd` deployment, encoding version-gated arguments and resource templates.
MCP routes (agents and developer) are enabled by default on both public and internal listeners.
Key types include `DeploymentStatus` (simplified external deployment state), `Resources` (the generated Kubernetes resources for a generation), `ConnectionInfo`, and `LoginCredentials`.
Uses the `ListenersConfig` types from `mz-server-core` to generate listener configuration for the deployed environmentd.
Service creation distinguishes between headless and VIP-bearing services via a `headless` parameter on `create_base_service_object`: the public environment service is headless (`cluster_ip: "None"`), while generation-specific services omit `cluster_ip` so Kubernetes assigns a VIP.
