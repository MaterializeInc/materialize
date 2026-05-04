# orchestratord (mz-orchestratord)

Kubernetes operator (controller) binary that manages Materialize regions in
cloud deployments. Watches `Materialize`, `Balancer`, and `Console` custom
resources and reconciles them to the desired Kubernetes state (StatefulSets,
Deployments, Services, RBAC, NetworkPolicies, cert-manager certificates).

## Module surface (LOC ≈ 5,052)

| Module | LOC | Purpose |
|---|---|---|
| `controller/materialize/generation.rs` | 1,468 | Generation-versioned K8s resource specs per Materialize minor version (V140…V161+); upgrade path logic |
| `controller/materialize.rs` | 836 | `Materialize` CRD reconciler — Config struct, reconcile entry point, status conditions |
| `controller/console.rs` | 636 | `Console` CRD reconciler — Deployment, Service, NetworkPolicy |
| `controller/balancer.rs` | 598 | `Balancer` CRD reconciler — StatefulSet, Service, RBAC, cert |
| `bin/orchestratord.rs` | 549 | Binary entry point — CLI args, kube client init, controller wiring, metrics server |
| `controller/materialize/global.rs` | 439 | Global (cross-region) state reconciliation for Materialize resources |
| `k8s.rs` | 142 | `apply_resource`, `delete_resource`, `get_resource` — idempotent kube API helpers |
| `tls.rs` | 133 | `DefaultCertificateSpecs`, `issuer_ref_defined` — cert-manager integration |
| `metrics.rs` | 129 | `Metrics` — Prometheus counters/histograms for reconcile loops |
| `lib.rs` | 110 | `Error` enum, `parse_image_tag`, `matching_image_from_environmentd_image_ref` |
| `controller.rs` | 12 | Module declarations for `balancer`, `console`, `materialize` |

## Package identity

Crate name: `mz-orchestratord`. Binary: `mz-orchestratord`.
This is a cloud-infrastructure crate, not a query-processing crate.

## Key interfaces (exported)

- **`controller::materialize`** — reconciler for the `Materialize` CRD;
  owns the generation-versioned upgrade path (`generation/` sub-module with
  per-version structs `V140`, `V143`, `V144`, `V147`, `V161`, etc.).
- **`controller::balancer`** — reconciler for the `Balancer` CRD.
- **`controller::console`** — reconciler for the `Console` CRD.
- **`k8s::{apply_resource, delete_resource, get_resource}`** — thin wrappers
  over `kube::Api` for idempotent apply/delete.
- **`tls::{DefaultCertificateSpecs, issuer_ref_defined}`** — cert-manager
  certificate spec construction.
- **`metrics::Metrics`** — Prometheus metrics for controller reconcile loops.
- **`parse_image_tag` / `matching_image_from_environmentd_image_ref`** —
  OCI image reference utilities; extract tags correctly across
  `host:port/repo:tag` and `@sha256:` digest forms.
- **`Error`** — top-level error enum wrapping `anyhow`, `kube`, `reqwest`.

## Dependencies

`kube`, `k8s-openapi`, `mz-cloud-resources`, `mz-cloud-provider`,
`mz-license-keys`, `mz-orchestrator-kubernetes`, `mz-orchestrator-tracing`,
`reqwest`, `semver`.

## Bubbled findings for src/CONTEXT.md

- **Generation-versioned upgrade path is the dominant complexity** (1,468 LOC
  in `generation.rs`): per-minor-version structs encode the exact K8s resource
  shape expected at each Materialize release. This is a deliberate Depth
  trade-off — brittleness lives here so the rest of the controller stays clean.
- **`controller/materialize/global.rs`** (439 LOC) manages cross-region global
  state; the boundary between per-region and global reconciliation is a Seam
  worth documenting explicitly as the cloud topology grows.
- **No trait abstraction over the three CRD controllers** — `materialize`,
  `balancer`, and `console` reconcilers share no `Reconciler` trait; they are
  wired independently in `bin/orchestratord.rs`. Adding a fourth CRD requires
  touching the binary directly.
