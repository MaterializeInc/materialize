# mz-cloud-resources

Kubernetes CRD definitions and trait abstractions for cloud-only resources
(AWS PrivateLink / VPC endpoints, and the Materialize/Balancer/Console operator
objects). Nothing here has an equivalent in local deployments.

## Subtree (≈ 6,470 LOC total)

| Path | LOC | What it owns |
|---|---|---|
| `src/crd/` | 5,761 | All Kubernetes CRDs — see [`src/crd/CONTEXT.md`](src/crd/CONTEXT.md) |
| `src/vpc_endpoint.rs` | 151 | `CloudResourceController`/`CloudResourceReader` traits + helpers |
| `src/crd.rs` | 179 | `ManagedResource`, `register_versioned_crds`, `VersionedCrd` |
| `src/lib.rs` | 20 | Feature-gated re-exports |

## Package identity

Crate name: `mz-cloud-resources`. Feature flag `vpc-endpoints` gates
`vpc_endpoint` module and its re-exports; the `crd` module is always compiled.
Key deps: `kube`, `k8s-openapi`, `schemars`, `mz-ore`, `mz-server-core`.
`mz-repr` is an optional dep (required only when `vpc-endpoints` is active).

## Purpose

Provides two orthogonal capabilities:

1. **CRD schema definitions** — the `crd` module publishes Kubernetes CRD types
   (`Materialize`, `Balancer`, `Console`, `VpcEndpoint`) used by `mz-orchestratord`
   to register and reconcile cloud resources.
2. **VPC endpoint control traits** — `CloudResourceController` / `CloudResourceReader`
   define the async interface for creating, deleting, listing, and watching
   `VpcEndpoint` objects; implementations live in `mz-controller`.

## Key interfaces (exported)

- `CloudResourceController` — `ensure_vpc_endpoint`, `delete_vpc_endpoint`,
  `list_vpc_endpoints`, `watch_vpc_endpoints`, `reader`.
- `CloudResourceReader` — `read(id)`.
- `VpcEndpointConfig`, `VpcEndpointEvent` — config input / event output types.
- `AwsExternalIdPrefix` — security-typed wrapper enforcing operator-only construction.
- `vpc_endpoint_name`, `vpc_endpoint_host`, `id_from_vpc_endpoint_name` — naming
  contract shared with the cloud infrastructure VpcEndpointController.
- `register_versioned_crds` — idempotent CRD registration with retry.

## Downstream consumers

`mz-orchestratord`, `mz-controller`, `mz-adapter`, `mz-environmentd`.

## Architecture notes

- **Feature-gate seam**: the `crd` module (Kubernetes types) compiles everywhere;
  the `vpc-endpoints` feature gates the async control trait. This allows schema
  tooling to import the crate without pulling in the full async Kubernetes stack.
- **Thin trait, external impl**: `CloudResourceController` has no implementation
  here — it is fulfilled by `mz-controller`. The trait + naming helpers form the
  stable contract between the adapter layer and the cloud operator.
- **`AwsExternalIdPrefix`** constructor name (`new_from_cli_argument_or_environment_variable`)
  is deliberately unwieldy to prevent accidental user-controlled construction.

## Bubbled findings for src/CONTEXT.md

- `mz-cloud-resources` is the sole crate defining the Kubernetes CRD schema for
  Materialize cloud deployments; it is the operator/orchestrator boundary.
- The `vpc-endpoints` feature flag is the primary cloud-vs-local deployment seam
  in this crate; downstream callers must activate it to use control traits.
- `AwsExternalIdPrefix` is a narrow but security-critical type; its forced-verbose
  constructor is an intentional design to prevent privilege-escalation bugs.
