---
source: src/orchestratord/src/controller/materialize/generation.rs
revision: 59b1f165b2
---

# mz-orchestratord::controller::materialize::generation

Manages version-specific generation logic for deploying `environmentd`.
Contains version constants and thresholds (e.g., `V140_DEV0`, `V143`, `V144`, `V147_DEV0`, `V153`, `V154_DEV0`, `V161`, `V26_1_0`, `PER_ROUTE_GROUP_ROLES_VERSION`) that gate feature flags and configuration changes based on the running Materialize version.
`PER_ROUTE_GROUP_ROLES_VERSION` (v26.32.0) is the version at which HTTP listeners moved `allowed_roles` from the listener level to per route group; `orchestratord` serializes `v0_147_0::ListenersConfig` for older binaries and `v26_32_0::ListenersConfig` for binaries at or above that version, wrapping both in the `VersionedListenersConfig` enum from `mz-server-core`.
Builds the `StatefulSet`, `Service`, and `ConfigMap` resources for a specific generation of an `environmentd` deployment, encoding version-gated arguments and resource templates.
MCP routes (agents and developer) are enabled by default on both public and internal listeners.
Key types include `DeploymentStatus` (simplified external deployment state), `Resources` (the generated Kubernetes resources for a generation), and `LoginCredentials`.
Uses the `VersionedListenersConfig` type from `mz-server-core` to generate listener configuration for the deployed environmentd, selecting between the legacy and current schemas based on the target version.
Service creation distinguishes between headless and VIP-bearing services via a `headless` parameter on `create_base_service_object`: the public environment service is headless (`cluster_ip: "None"`), while generation-specific services omit `cluster_ip` so Kubernetes assigns a VIP.
