# MaterializeDebug CRD: in-cluster flight recorder + thin mz-debug client

## Context

Today `mz-debug self-managed` collects diagnostics from outside the cluster: it
needs the customer's kubeconfig, a local `kubectl` binary, port-forwards to
every pod, and only captures data from the moment someone runs it, which is
often after the interesting state is gone. This plan moves collection inside
the cluster: a new `MaterializeDebug` CRD whose controller (in orchestratord)
runs a collector Deployment that continuously snapshots the same diagnostics
mz-debug gathers today (k8s resources + describe, pod logs, heap/CPU profiles,
Prometheus metrics, ~95 system catalog relations) into a ring buffer. The
mz-debug CLI's self-managed mode is reduced to retrieval.

Ownership chain (Kubernetes GC does teardown):

```
Materialize CR ──ownerRef──> MaterializeDebug CR ──ownerRef──> Deployment, Service,
                                                               ServiceAccount, RoleBinding
                                                  ──finalizer─> ClusterRoleBinding,
                                                                foreign-ns RoleBindings
```

## Decisions (user-confirmed)

- **Continuous flight recorder**: long-lived CR, periodic snapshots into a ring
  buffer; CLI can also trigger a fresh snapshot at download time.
- **CLI `self-managed` mode replaced entirely** (no legacy fallback; clear
  error against old operators). Emulator mode unchanged.
- **`kubectl describe` reimplemented natively in Rust** in the collector.
- **emptyDir buffer**, served over HTTP. Pod restart loses history.
- **Defaults: interval 30m, retain 12 snapshots, buffer cap 2Gi.**
- **DaemonSets dumped per-target-namespace only** (was cluster-wide), keeping
  the collector's cluster-scoped grant minimal.

## Verified constraints that shape the design

- A cluster-scoped object cannot have an ownerReference to a namespaced object,
  so the ClusterRoleBinding (and RoleBindings in `additionalNamespaces`) are
  cleaned by a **finalizer** on the debug controller, not GC.
- RBAC escalation prevention: orchestratord can only create Roles containing
  permissions it already holds. So all collector permissions live in two
  **chart-provided ClusterRoles**; the controller creates only bindings, using
  the existing `bind` + `resourceNames` pattern
  (`misc/helm-charts/operator/templates/clusterrole.yaml:70-76`, precedent
  `environmentd`).
- Listener topology (`create_v0_147_0_listeners_config`,
  `src/orchestratord/src/controller/materialize/generation.rs:1305-1437`):
  with `Password|Sasl|Oidc` the internal SQL/HTTP listeners are removed;
  profiling/internal routes move to the external listener restricted to
  `NormalAndInternal` roles; an unauthenticated metrics-only listener stays on
  the internal HTTP port. The collector must therefore authenticate as
  `mz_system` using the backend secret key `external_login_password_mz_system`
  (env wiring precedent generation.rs:1086-1097), mounted via `secretKeyRef`
  (no secret-read RBAC). With `None` auth it uses the internal SQL/HTTP ports,
  no creds. mz-debug's existing `get_port_labels`
  (`src/mz-debug/src/internal_http_dumper.rs:129-162`) already encodes this
  split and is reused.
- TLS: the internal cert's SANs cover only the environmentd *service* DNS
  names (`global.rs:432-435`), so the collector addresses environmentd by
  service FQDN and builds its reqwest client with
  `danger_accept_invalid_certs(true)` (same as orchestratord's leader-status
  polling, generation.rs:~284). Clusterd internal-http has no TLS/auth; pod-IP
  addressing is fine. Existing https→http fallback covers no-TLS.
- NetworkPolicies lock pods labeled `materialize.cloud/mz-resource-id` to
  same-label traffic (`global.rs:100-211`). Like Balancer
  (materialize.rs:829), MaterializeDebug carries `spec.resourceId` set to the
  instance's resource id so collector pods match
  `allow-all-within-environment`. `kubectl port-forward` ingress originates
  from the kubelet and bypasses NetworkPolicy in mainstream CNIs.
- `materialize/mz-debug:<tag>` is published for every release (mzbuild
  `publish`/`mainline` default true, `misc/python/materialize/mzbuild.py:942,944`;
  tagging in `ci/deploy/docker.py:45-59`), so image derivation via
  `matching_image_from_environmentd_image_ref(.., "mz-debug", None)`
  (`src/orchestratord/src/lib.rs:46`) works. Old tags lack the collector
  subcommand → gate on `mz.meets_minimum_version(DEBUG_COLLECTOR_MIN_VERSION)`
  (pick the version at merge time; precedent `PER_ROUTE_GROUP_ROLES_VERSION`).

## 1. CRD: `src/cloud-resources/src/crd/materialize_debug.rs` (new)

Modeled on `balancer.rs`/`console.rs`; `pub mod materialize_debug;` in `crd.rs`.
Namespaced, group `materialize.cloud`, `v1alpha1`, kind `MaterializeDebug`,
plural `materializedebugs`, shortname `mzdbg`, status subresource,
printcolumns: Materialize (`.spec.materializeName`), Ready condition, ImageRef
(priority 1).

Spec (camelCase):
- `materializeName: String` — the Materialize CR in the same namespace.
- `collectorImageRef: Option<String>` — override; default derived from the
  instance's environmentd image.
- `snapshotInterval` — humantime string newtype, default `"30m"` (copy the
  `RolloutRequestTimeout` pattern in materialize.rs).
- `retainedSnapshots: Option<u32>` — default 12.
- `bufferSizeLimit: Option<Quantity>` — default `2Gi`; oldest snapshots evicted
  first; emptyDir sizeLimit set ~10% above.
- `collect: DebugCollectionConfig` — `k8s`, `systemCatalog`, `heapProfiles`,
  `prometheusMetrics` (all default true), `cpuProfiles` (**default false** for
  periodic snapshots: each capture disables memory profiling on the target and
  adds sampling load; on-demand snapshots default it to true, preserving
  today's CLI behavior), `cpuProfileDurationSeconds` (default 10).
- `additionalNamespaces: Option<Vec<String>>`.
- `resourceRequirements`, `podAnnotations`, `podLabels`.
- `resourceId: Option<String>` — set by the operator to the instance's resource
  id for network-policy/label matching.

Status: `resource_id: String`, `conditions: Vec<Condition>` (Ready).

Helpers mirroring Balancer: `deployment_name()`/`service_name()` =
`mz{rid}-debug-collector`, `service_account_name()`,
`cluster_role_binding_name()` = `mz{rid}-debug-collector-{namespace}`
(cluster-unique), `status()`. `impl ManagedResource` with `default_labels()` =
organization-name (= `spec.materializeName`), organization-namespace,
mz-resource-id; `app_name()` = `Some("debug-collector")`.

Register: one `VersionedCrd { stored_version: "v1alpha1", conversion: None }`
entry in `register_crds` (`src/orchestratord/src/k8s.rs:160-186`).

## 2. Controller: `src/orchestratord/src/controller/materialize_debug.rs` (new)

`pub mod materialize_debug;` in `controller.rs`. `impl k8s_controller::Context`,
`FINALIZER_NAME = Some("orchestratord.materialize.cloud/materialize-debug")`.
Config carries security-context/image-pull/scheduler settings, the four
environmentd port numbers, collector HTTP port (8080), and
`default_certificate_specs` (to compute TLS-ness via `issuer_ref_defined`).

Reconcile:
1. Status bootstrap: if `status.is_none()`, `replace_status` with default and
   return (idiom balancer.rs:564-579).
2. Resolve `Api::<Materialize>::namespaced(ns).get(&spec.materialize_name)`:
   - missing → `Ready=False reason=MaterializeNotFound`, no children,
     `Action::requeue(60s)`;
   - `status.resource_id` absent → requeue;
   - version below `DEBUG_COLLECTOR_MIN_VERSION` →
     `Ready=False reason=UnsupportedVersion`;
   - **patch ownerReference → Materialize onto the debug CR if missing** (SSA
     with `FIELD_MANAGER`; same shape as `crd.rs:113-122`). Covers manually
     created CRs; auto-created ones already have it.
3. Derive: image (`spec.collectorImageRef` or
   `matching_image_from_environmentd_image_ref`), auth mode
   (`mz.spec.authenticator_kind`), TLS, envd service FQDN, backend secret name.
4. Apply children via `apply_resource` (SSA), metadata via
   `debug.managed_resource_meta(...)`:
   - ServiceAccount.
   - RoleBinding (CR namespace) → chart ClusterRole
     `materialize-debug-collector`; plus one per `additionalNamespaces` entry
     (foreign ns → no ownerRef possible → finalizer-tracked, name includes
     owning namespace).
   - ClusterRoleBinding → chart ClusterRole
     `materialize-debug-collector-cluster` (no ownerRef; finalizer-cleaned;
     carries `default_labels()`).
   - Deployment: replicas 1, **strategy Recreate** (two collectors would
     double profile load and split the buffer), balancerd security-context
     pattern, emptyDir volume mounted at `/var/lib/mz-debug`, container args:
     `collector --k8s-namespace --mz-instance-name --listen-addr=0.0.0.0:8080
     --snapshot-dir --snapshot-interval --retained-snapshots
     --buffer-size-limit-bytes --auth-mode=<none|password> <4 port flags>
     <category flags> [--additional-k8s-namespace ...]`; password mode adds
     `MZ_USERNAME=mz_system` + `MZ_PASSWORD` from
     `secretKeyRef{backend_secret, external_login_password_mz_system}`;
     readiness `GET /api/readyz`; ports `http` 8080.
   - Service: ClusterIP, port `http` 8080, selector on the deployment's
     `materialize.cloud/name` label (balancer pattern).
5. Status sync: copy `sync_deployment_status` from balancer.rs:79-143 (Ready
   from Deployment Available; `replace_status` with no-op guard).
6. `Action::requeue(300s)` — picks up drift in the referenced Materialize
   (auth kind, image, secret name), since we can't `.owns()` it.

`cleanup()` (finalizer): `delete_resource` the ClusterRoleBinding and
foreign-namespace RoleBindings; everything else GC's via ownerReferences.

Wiring in `src/orchestratord/src/bin/orchestratord.rs`: new
`make_materialize_debug_controller` closure,
`.owns(Deployment/Service/ServiceAccount/RoleBinding)` with
`labels("materialize.cloud/mz-resource-id").timeout(29)`; `join4` → `join5`
(lines ~871-887).

**Auto-creation** (materialize.rs, after the Console block at :891): if
`config.create_debug_collectors && mz.meets_minimum_version(...)`, apply a
`MaterializeDebug { metadata: mz.managed_resource_meta(mz.name_unchecked()),
spec: { materialize_name, resource_id: Some(status.resource_id), ..Default } }`
(ownerRef → Materialize for free); else `delete_resource` by instance name —
exact Balancer/Console pattern (materialize.rs:807-894). Gating: new clap flag
`--create-debug-collectors` (default **off**), helm value
`debugCollector.enabled: false` emitting the flag in
`templates/deployment.yaml`; test configs (test/orchestratord, cloudtest) turn
it **on** per the flag-default convention. No new MaterializeSpec field (v1 +
conversion webhook makes that disproportionately expensive; manual CRs give
per-instance opt-in already since the debug controller runs unconditionally).

## 3. RBAC (`misc/helm-charts/operator/`)

Operator ClusterRole (`templates/clusterrole.yaml`):
- materialize.cloud rule: add `materializedebugs`, `materializedebugs/status`.
- new rule: `rbac.authorization.k8s.io`/`clusterrolebindings`: full CRUD+watch.
- extend the `clusterroles`+`bind` rule's `resourceNames` with
  `materialize-debug-collector`, `materialize-debug-collector-cluster`.

New template `debugcollector-clusterroles.yaml` (gated `.Values.rbac.create`),
verbs `get,list,watch` throughout, **no secrets anywhere**:
- `materialize-debug-collector` (namespaced kinds, granted per-ns via
  RoleBinding): `""` pods, **pods/log**, services, configmaps, pvcs,
  serviceaccounts, events, endpoints; `apps` deployments, statefulsets,
  replicasets, daemonsets; `networking.k8s.io` networkpolicies;
  `rbac.authorization.k8s.io` roles, rolebindings; `materialize.cloud`
  materializes, materializedebugs; `cert-manager.io` certificates.
- `materialize-debug-collector-cluster` (via ClusterRoleBinding): `""` nodes,
  persistentvolumes; `storage.k8s.io` storageclasses;
  `admissionregistration.k8s.io` mutating/validatingwebhookconfigurations;
  `apiextensions.k8s.io` customresourcedefinitions.

Update `tests/clusterrole_test.yaml`; add tests for the new template.

## 4. Collector (`src/mz-debug/`)

New modules: `collector/{mod,http,store,snapshot,targets}.rs`, `describe.rs`.
`DebugModeArgs` gains a hidden `Collector(CollectorArgs)` subcommand.

- **Refactor pivot**: extract `DumpConfig { base_path, category toggles }` from
  today's `Context` (main.rs:204); re-point `K8sDumper`, `HttpDumpClient`,
  `SystemCatalogDumper` at it (mechanical; emulator constructs one from its
  Context). Each snapshot gets a fresh `DumpConfig` with base_path =
  snapshot workdir.
- **targets.rs**: reuse the kube-API service/pod discovery already in
  `kubectl_port_forwarder.rs:171-359` (`find_environmentd_service`,
  `find_cluster_services`, `find_service_pods`) to produce direct authorities:
  envd = `{service_fqdn}:{port}` chosen by `get_port_labels`; clusterd =
  `{pod_ip}:{internal_http_port}` per pod. Delete the per-pod
  `spawn_pod_port_forward` machinery (`internal_http_dumper.rs:600-613` + call
  sites); rewrite `dump_self_managed_http_resources` →
  `dump_in_cluster_http_resources(config, targets, auth_mode, client)`.
- **SQL**: `create_mz_connection_url(envd_fqdn, port, creds)`; password mode →
  external `sql` port as `mz_system`; None → internal SQL port, no creds. Fresh
  `SystemCatalogDumper` per snapshot (resilient to envd restarts).
- **store.rs `SnapshotStore`**: `<dir>/<id>.zip` + `<id>.meta.json`
  (`{id, kind: periodic|on_demand, started_at, completed_at, size_bytes,
  categories}`); id = `<RFC3339>-<kind>`. Build in `<dir>/tmp/<id>/`, zip with
  existing `zip_debug_folder` (utils.rs:53), atomic rename in; wipe `tmp/` on
  startup. Retention after each completion: delete oldest until count ≤ N and
  bytes ≤ cap.
- **snapshot loop**: one task owns execution (no concurrent snapshots — CPU
  profiles must not overlap): `tokio::select!` over interval tick
  (`MissedTickBehavior::Skip`) and an mpsc of on-demand requests (category
  overrides from the CLI; on-demand defaults `cpu_profiles=true`); coalesce
  duplicate pending requests. Per-snapshot zip layout identical to today's
  bundle tree so downstream habits keep working.
- **Log strategy**: periodic snapshots use
  `LogParams { since_seconds: interval + 60, timestamps: true }` (bounded size,
  history reconstructable by concatenating retained snapshots); on-demand
  snapshots keep full current+previous logs as today (the zip handed to
  support must stand alone). Full logs every periodic snapshot would multiply
  pod-log volume by retention count and dominate the buffer.
- **http.rs** (axum, new workspace-dep usage in mz-debug/Cargo.toml):
  `GET /api/readyz`; `GET /api/snapshots` (metadata list);
  `GET /api/snapshots/latest`; `GET /api/snapshots/{id}` (zip stream);
  `POST /api/snapshots` (category-override body → `{id}`, coalescing).
- **describe.rs**: trait `DescribeResource { fn describe(&self, events: &[Event]) -> String }`
  with shared Name/Namespace/Labels/Annotations header + Events section
  (filtered in-memory from the per-namespace Event list by involvedObject —
  no extra API calls). Hand-written bodies for the high-value kinds: Pod,
  Deployment/StatefulSet/ReplicaSet, Service, Node, PVC/PV. All other kinds:
  header + events only (their YAML sits next to describe.txt). Output: one
  `describe.txt` per `<plural>/<ns>/`, objects separated by `Name:` headers.
  Replaces the kubectl shell-out (`k8s_dumper.rs:152-219`); `k8s_context`
  field removed. DaemonSets switch to per-target-namespace listing.

## 5. CLI rework (self-managed mode)

New `SelfManagedDebugModeArgs`: keep `--k8s-namespace`, `--mz-instance-name`,
`--k8s-context`; add `--all-snapshots`, `--no-fresh-snapshot`,
`--snapshot-timeout-seconds` (default 600), `--debug-name` (when the CR name
differs from the instance name). Global `--dump-*` flags are forwarded in the
`POST /api/snapshots` body (warn-ignored with `--no-fresh-snapshot`).
`--additional-k8s-namespace` errors with a pointer to
`spec.additionalNamespaces`; `--mz-username/--mz-password/--mz-connection-url`
stay for emulator, warn-ignored here.

Flow: kubeconfig client → get `MaterializeDebug` (distinct error messages for
"CRD not installed: operator predates in-cluster debug collection, upgrade or
use mz-debug <old version>" vs "no MaterializeDebug found: enable
`debugCollector.enabled: true` or create one, see docs") → warn if not Ready
(retained snapshots may still exist) → `KubectlPortForwarder` to the collector
Service:8080 (module kept for exactly this) → unless `--no-fresh-snapshot`,
POST + poll until complete or timeout (timeout → warn, proceed with latest) →
download latest (default) or all, saved as
`mz_debug_<instance>_<snapshot_id>.zip`. No local re-zipping.

Code disposition: dumpers become collector/emulator-only;
`create_pg_wire_port_forwarder` deleted; emulator mode untouched.

## 6. Versioning

- `DEBUG_COLLECTOR_MIN_VERSION` = first release shipping the collector
  subcommand (set at merge).
- mz-debug crate 0.5.0 → 0.6.0; S3 tarball release process unchanged.
- Skew: new CLI + old operator → clear error; old CLI + new cluster → still
  works (nothing server-side removed direct collection from old binaries).

## 7. Testing

- Rust unit: CRD schema/defaults round-trip in cloud-resources (pattern:
  materialize.rs bottom-of-file tests); describe.rs golden tests over
  constructed k8s-openapi objects; SnapshotStore retention/size-cap/crash
  recovery (tempdir); clap arg tests.
- Helm unittest: clusterrole_test.yaml additions, new
  debugcollector_clusterroles_test.yaml, deployment_test.yaml case for
  `--create-debug-collectors`.
- e2e test/orchestratord/mzcompose.py: enable `debugCollector.enabled` in test
  values; un-stub `run_mz_debug()` (line 85 — the download-only flow should
  also fix the port-forward hang that got it stubbed); assert the zip contains
  `materializes/`, `logs/`, `prom_metrics/`, `system_catalog/`.
- e2e test/cloudtest/test_mz_debug_tool.py rewrite: create/wait
  MaterializeDebug → run CLI → assert zip; profile test triggers on-demand
  snapshot with `--dump-cpu-profiles=true --cpu-profile-duration-seconds=1`
  and asserts per-pod `profiles/*.{cpuprof,memprof}.pprof.gz`.
- test/mz-debug/mzcompose.py (emulator) untouched.

## 8. Docs

- Rewrite `doc/user/content/integrations/mz-debug/self-managed.md`
  (architecture, operator prerequisite + `debugCollector.enabled`, manual CR
  example, download flows); regenerate
  `doc/user/data/mz-debug/self_managed_options.yml`.
- Extend `src/cloud-resources/src/bin/crd_writer.rs` with a kind selector and
  `bin/bump-version:74-76` to also emit
  `materialize_debug_crd_descriptions_v1alpha1.json`; operator-docs page for
  the CRD.
- Release-note snippet for the operator version introducing this.

## 9. Sequencing (each step leaves `cargo check` green; riskiest first)

1. **Collector core in mz-debug** (riskiest: in-cluster connectivity/auth):
   DumpConfig extraction, targets.rs, `collector` subcommand with loop +
   SnapshotStore + HTTP API; delete per-pod port-forward path. Smoke-test in
   kind by deploying the mzbuild image by hand before any operator work, to
   de-risk the TLS/password matrix in isolation.
2. **Native describe** (describe.rs + k8s_dumper swap). Independent.
3. **CRD type** + register_crds entry + unit tests.
4. **Debug controller** + orchestratord wiring (join5) + helm RBAC + helm
   unittests.
5. **Materialize-controller auto-create** + `--create-debug-collectors` flag +
   helm value (off) + test-config enablement + version gate.
6. **CLI rework** (depends on 1's HTTP API).
7. **e2e tests** (orchestratord un-stub, cloudtest rewrite).
8. **Docs + crd_writer/bump-version + crate version bump.**

## Verification

- Per step: `bin/fmt`, `cargo check` (repo convention), targeted
  `cargo test -p mz-cloud-resources` / `-p mz-debug` / `-p mz-orchestratord`.
- Helm: `helm unittest misc/helm-charts/operator`.
- End-to-end: `cd test/orchestratord && bin/mzcompose run default` (kind):
  MaterializeDebug auto-created with ownerRef → Materialize; collector
  Deployment/Service/SA/RoleBinding carry ownerRef → MaterializeDebug;
  snapshots accumulate; `mz-debug self-managed` downloads a fresh zip;
  `kubectl delete materialize <name>` cascades away the debug CR and children
  (finalizer removes the ClusterRoleBinding).
- Auth matrix: repeat with `authenticatorKind: Password` (cloudtest covers the
  kind Password path with the `external_login_password_mz_system` secret).

## Deferred (noted, not in scope)

- Collector writing `lastSnapshotTime` etc. to its own CR status (would need
  status-write RBAC for the pod; cheap to add later).
- Cluster-wide DaemonSet dumping (per-namespace now; add to the -cluster role
  later if support asks).
- PVC-backed buffer surviving pod replacement.
