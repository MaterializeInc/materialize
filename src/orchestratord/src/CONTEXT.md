# orchestratord::src

See [`../CONTEXT.md`](../CONTEXT.md) for crate-level overview.

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

## Key interfaces

- **`controller::materialize::reconcile`** — main reconcile function for
  `Materialize` CRDs; dispatches to generation-specific resource builders.
- **`generation::V161`** (latest at time of review) — the current canonical
  generation; earlier `V*` structs are retained for upgrade migration.
- **`k8s::apply_resource`** — server-side apply wrapper; used by all three
  CRD reconcilers.
- **`Error`** — unified error type; `kube::Error` and `reqwest::Error`
  conversions allow the reconcile loop to return a single error kind.

## Cross-references

- `mz-cloud-resources` — CRD types (`Materialize`, `Balancer`, `Console`)
- `mz-cloud-provider` — `CloudProvider` enum
- `mz-license-keys` — license validation in `controller/materialize.rs`
- `mz-orchestrator-kubernetes` — `KubernetesImagePullPolicy`
- `kube` / `k8s-openapi` — Kubernetes API client and generated types
- Generated docs: `doc/developer/generated/orchestratord/`
