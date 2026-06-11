# cloud-resources::crd

Kubernetes Custom Resource Definitions for Materialize cloud infrastructure.

## Subtree (≈ 1,188 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `materialize.rs` | 823 | `Materialize` CRD (`v1alpha1`): spec/status types, rollout strategies, TLS config |
| `balancer.rs` | 181 | `Balancer` CRD: load-balancer spec/status |
| `console.rs` | 173 | `Console` CRD: web console spec/status |
| `vpc_endpoint.rs` | 201 | `VpcEndpoint` CRD (`v1`): AWS VPC endpoint spec/status |
| `generated.rs` | 10 | Re-exports cert-manager generated types |
| `crd.rs` (parent) | 179 | `ManagedResource` trait, `register_versioned_crds`, `VersionedCrd` |

## Purpose

Defines the Kubernetes operator surface for Materialize cloud deployments.
All CRDs live under group `materialize.cloud`. Each CRD is derived via
`#[derive(CustomResource)]` (kube-rs) from a `*Spec`/`*Status` pair.
`register_versioned_crds` applies CRDs via server-side apply with retry and
waits for establishment.

## Key types

- `Materialize` / `MaterializeSpec` / `MaterializeStatus` — primary deployment
  object; reconciled by `mz-orchestratord`; encodes rollout strategy, image,
  resource limits, TLS, generation tracking.
- `MaterializeRolloutStrategy` — `WaitUntilReady` (default), `ManuallyPromote`,
  `ImmediatelyPromoteCausingDowntime`.
- `VpcEndpoint` / `VpcEndpointSpec` / `VpcEndpointStatus` — reconciled into an
  AWS VPC endpoint by the environment controller.
- `Balancer`, `Console` — auxiliary cloud-only resource types.
- `ManagedResource` trait — standard `owner_references` / label injection for
  sub-resources.

## Bubbled findings for crate CONTEXT.md

- `materialize.rs` dominates at 69% of crd LOC; `Materialize` CRD is the
  primary orchestration object for the entire cloud deployment lifecycle.
- All CRDs are feature-gate-free except `vpc_endpoint`, which is conditionally
  compiled under the `vpc-endpoints` feature.
- Generated cert-manager types live in `generated/` and are a thin vendored
  surface; no direct authorship.
