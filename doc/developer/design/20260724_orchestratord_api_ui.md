# orchestratord status API and UI

- Associated issues: none yet

## The Problem

orchestratord manages Materialize, Balancer, and Console custom resources,
plus the Kubernetes objects generated from them (StatefulSets, Deployments,
Services, ConfigMaps, Certificates, NetworkPolicies). Today the only ways to
see what the operator is doing are `kubectl` spelunking, operator logs, and
Prometheus metrics. Answering basic questions like "what environments exist,
what version are they running, is an upgrade in flight, is it stuck waiting
for promotion" requires knowing the CRD status conventions (the `UpToDate`
condition and its reasons) and the per-generation resource naming scheme.

Managing system parameter defaults is similarly opaque: they live in a
ConfigMap referenced by `spec.systemParameterConfigmapName`, with a JSON
document under a magic `system-params.json` key.

## Success Criteria

- An operator of a self-managed Materialize installation can see, in one
  place: all Materialize environments, their running and desired versions,
  rollout state (including waiting-for-approval and ready-to-promote states),
  and the health of the generated workloads (environmentd StatefulSets and
  pods, balancerd and console Deployments).
- Common rollout actions are available without hand-editing YAML: request a
  rollout, force a rollout with no spec changes, promote a pending
  `ManuallyPromote` rollout.
- The system parameter ConfigMap can be viewed and edited with JSON
  validation.
- orchestratord is not modified. The console is a separate deployable that can
  be installed, upgraded, or left out without touching the component that
  manages databases, and it runs with strictly fewer permissions than the
  operator, including no access to Secrets.
- The UI has no build step and no external dependencies at runtime.

## Out of Scope

- Authentication and authorization. The server binds a ClusterIP-only port.
  Anyone with network access to the pod (or `kubectl port-forward` rights)
  has full access. Auth can be layered on later without changing the API
  surface.
- Editing arbitrary spec fields of the Materialize CR. The API exposes a
  small set of intentional mutations, not a generic YAML editor. `kubectl`
  remains the tool for everything else.
- Any data-plane visibility (SQL-level objects, cluster replica status inside
  Materialize). This is a control-plane view only.
- Multi-cluster. The API serves the cluster the operator runs in.

## Solution Proposal

A separate binary, `src/orchestratord-console`, deployed on its own and
optional. orchestratord is not modified at all.

Everything the console does goes through the Kubernetes API, so it shares no
state with the operator. That makes the separation nearly free, and it buys
three things worth more than the one image it costs.

The first is permissions. The console never reads Secrets. Run inside the
operator, its unauthenticated HTTP surface would nonetheless be backed by the
operator's service account, which can read every Secret in the cluster and
mutate everything it manages. As its own deployment it gets a service account
scoped to what it actually uses, with no Secret access at all.

The second is blast radius. The operator manages databases. The console is a
convenience. Keeping them in separate processes means the console's memory,
restarts, and release cadence do not ride along with reconciliation.

The third is that it can be installed, upgraded, or left out entirely without
touching the operator, which is the point of it being optional.

The cost is a second image, a second chart, and RBAC that is written out
rather than inherited. The console also encodes some knowledge of the
operator's behavior, most notably which spec fields are hashed into the
environmentd StatefulSet and therefore need a rollout. Without a compile-time
link that can drift, so it is kept advisory: the controller remains the source
of truth and still reports `WaitingForApproval`, meaning drift produces a
misleading hint rather than a broken rollout.

The binary serves:

- `GET /` — the UI, a single self-contained HTML file embedded via
  `include_str!`.
- `GET /api/...` — the JSON API below.

### API

All responses are JSON. Errors use a uniform `{ "error": "message" }` body
with an appropriate status code. Kube API errors pass through their status
codes (404, 409, etc.) where meaningful.

Read endpoints:

- `GET /api/health` — 200 if the server is up and can reach the Kubernetes
  API (a cheap `Api::<Materialize>::all().list(limit=1)` probe).
- `GET /api/info` — operator build version, cloud provider, region, helm
  chart version, and relevant feature flags (`create_balancers`,
  `create_console`). The operator half is read back off the orchestratord
  Deployment, by parsing the flags it was started with and its image tag,
  rather than being configured here. Best effort: when that Deployment cannot
  be read, those fields are null and the console still reports its own
  version.
- `GET /api/materializes` — all Materialize CRs across namespaces. Each item
  is a summary: namespace, name, resource id, environment id, spec image ref,
  running image ref (`status.lastCompletedRolloutEnvironmentdImageRef`),
  active generation, rollout strategy, the `UpToDate` condition
  (status/reason/message/lastTransitionTime), whether a rollout is requested
  but incomplete, and creation timestamp.
- `GET /api/materializes/:namespace/:name` — full detail:
  - the summary fields above, plus spec fields relevant to operations
    (backend secret name, authenticator kind, system parameter ConfigMap
    name, replica counts, rollout request timeout, requestRollout /
    forcePromote / forceRollout values),
  - full status including all conditions,
  - owned workloads, discovered via the
    `materialize.cloud/mz-resource-id=<resource_id>` label selector:
    environmentd StatefulSets (per generation, with ready/total replicas and
    image), balancerd and console Deployments, Services, and pods (name,
    phase, ready, restart count, image, start time, node),
  - recent Kubernetes events for the Materialize resource and its
    StatefulSets/Deployments (best effort, capped at 50, sorted newest
    first).
- `GET /api/balancers` and `GET /api/consoles` — the Balancer and Console CRs
  with name, namespace, image ref, replicas, and conditions.

Mutations (all narrow, intentional operations on the v1alpha1 stored
version):

- `POST /api/materializes/:namespace/:name/rollout` — body
  `{ "forceRollout": bool, "forcePromote": bool }` (both default false).
  Sets `spec.requestRollout` to a fresh UUID. With `forceRollout`, also sets
  `spec.forceRollout` to that UUID (rollout proceeds even with no resource
  changes). With `forcePromote`, also sets `spec.forcePromote` to that UUID
  (skips waiting for cluster catchup). Returns the new rollout UUID.
- `POST /api/materializes/:namespace/:name/promote` — promotes a pending
  rollout by setting `spec.forcePromote = spec.requestRollout`. Intended for
  the `ManuallyPromote` strategy once the status reason is `ReadyToPromote`,
  but valid whenever a rollout is in progress. 409 if no rollout is pending.
- `PUT /api/materializes/:namespace/:name/config` — the editable slice of the
  spec as one endpoint: `environmentdImageRef`, `rolloutStrategy`,
  `rolloutRequestTimeout`, `balancerdReplicas`, `consoleReplicas`,
  `enableRbac`, and per-component `resources`. Every field is optional and only
  the ones present are changed, so the UI can save a single row without
  restating the rest. A component under `resources` given as `null` has its
  override cleared. `requestRollout` bumps the rollout in the same write, and
  is ignored when nothing changed so a no-op save cannot start one.

  The proposed image is checked against the same upgrade window the controller
  enforces, by running the check against a candidate copy of the resource
  rather than reimplementing the rule, so an illegal jump is rejected with 400
  instead of landing in the spec and surfacing later as `FailedDeploy`. The
  rollout timeout is parsed for the same reason: the CRD silently falls back to
  its default on an unparseable value, which would otherwise accept a setting
  that never applies.

  The response lists which fields changed and whether a rollout is needed.
  Only `environmentdImageRef`, `enableRbac`, and environmentd's own resources
  are part of the environmentd StatefulSet, and therefore hashed into the
  Materialize status. Rollout strategy, the rollout timeout, replica counts,
  and balancerd and console resources reach their targets without a new
  generation, since they flow to the Balancer and Console resources that their
  own controllers reconcile.

- `GET /api/materializes/:namespace/:name/system-params` — returns the parsed
  JSON object from the `system-params.json` key of the ConfigMap named by
  `spec.systemParameterConfigmapName`. Returns `{ "configmapName": null,
  "params": null }` if the spec field is unset, and `"params": null` if the
  ConfigMap or key is missing.
- `PUT /api/materializes/:namespace/:name/system-params` — body is a JSON
  object of system parameters. Writes it (pretty-printed) to the
  `system-params.json` key, creating the ConfigMap if it does not exist. If
  `spec.systemParameterConfigmapName` is unset, first patches the spec to
  point at `<materialize-name>-system-params` and then creates that
  ConfigMap. Non-object bodies are rejected with 400.

  NOTE: the operator mounts this ConfigMap into environmentd and passes
  `--config-sync-file-path` with a one second `--config-sync-loop-interval`.
  environmentd re-reads the file on every tick of that loop and applies
  changes through `ALTER SYSTEM`, so edits reach a running environment
  without a rollout. End to end latency is dominated by how fast the kubelet
  refreshes the mounted file, not by the loop.

  To avoid waiting out the kubelet's refresh period, the write also stamps a
  `materialize.cloud/system-params-refreshed-at` annotation onto the
  environmentd pods, which triggers a pod sync and re-projects the volume
  immediately. This annotates the live pods, not the pod template: the
  template is part of the StatefulSet, which is hashed into the Materialize
  status, so touching it would create a new generation and roll the
  environment out on every save. The nudge is best effort, since the write
  has already landed by then.

  Whether an edit actually reaches the environment is reported as `syncState`,
  read from what is deployed rather than inferred from a version. The serving
  generation's StatefulSet either carries `--config-sync-file-path` or it does
  not, which is the ground truth for `live`. When the spec names a ConfigMap
  but that StatefulSet has no mount, the environment is either mid-change or
  running an environmentd too old to ever get one, and those are told apart by
  the controller's own verdict: if it has finished reconciling with nothing
  outstanding, the omission was deliberate, so the state is `unsupported`
  rather than `pendingRollout`. That verdict has to describe the current spec,
  since wiring the ConfigMap bumps the resource's generation and the previous
  `Applied` condition lingers until the controller catches up. Comparing the
  condition's `observedGeneration` against `metadata.generation` is what keeps
  a brand new mount from being read as a deliberate omission.

  Deriving this from the cluster rather than from a version constant is what
  lets the console live outside orchestratord without duplicating the
  operator's knowledge of which environmentd releases support the sync loop.
  `rolloutRequired` is reported separately, and is true only when the write
  had to add `systemParameterConfigmapName` to the spec, since that is what
  puts the mount into the StatefulSet.

Both mutations that touch the spec use `Api::replace` on a freshly-`get`ed
object so a conflicting concurrent edit fails with 409 rather than being
clobbered.

### UI

One HTML file (`src/orchestratord/src/api/ui.html`) with inline CSS and
vanilla JavaScript. No framework, no bundler, no external fonts or CDNs.

- Overview: table of environments with namespace/name, running → desired
  image (highlighted when they differ), rollout state badge derived from the
  `UpToDate` condition (`Applied`, `Applying`, `ReadyToPromote`, `Promoting`,
  `WaitingForApproval`, `FailedDeploy`, `RolloutTimeout`), active generation,
  and age. Auto-refreshes every 5 seconds via `fetch`.
- Detail view (client-side routing on `location.hash`), in order: the rollout
  rail and condition message, the configuration card, then what is actually
  running (environmentd StatefulSets per generation, balancerd and console
  Deployments, pods with phase and restart counts, services), the system
  parameter editor, and recent events. Configuration and runtime state are
  kept as separate sections because they answer different questions: what the
  environment is meant to be, and what it currently is.
- Configuration lives in one card rather than being spread across sections.
  Each row shows the current value, and the editable ones carry a pencil that
  swaps that row into an inline editor with save and cancel: a text field for
  the image, a select for the rollout strategy and for RBAC, quantity fields
  for each component's requests and limits (empty meaning inherit the operator
  default), and plain fields for the timeout and replica counts. Rows whose
  change only lands through a rollout say so in the editor. Rows the operator
  owns, or that must not change on a live environment, render without a
  pencil.

- Other actions on the detail view: "Request rollout" (with force-rollout and
  force-promote checkboxes), "Promote" (shown when a rollout is pending), and
  a system parameters editor (textarea with client-side JSON validation, save
  via PUT). Destructive or disruptive actions confirm first, and the API error
  is shown verbatim on failure, with the edited row left open so the entered
  value is not lost.

- Because the page polls, an edit in progress must never be overwritten by a
  refresh. Every editable field records the value it was rendered with in a
  `data-initial` attribute, and the refresh is skipped while any field is
  focused or diverges from that value. Saving marks the field current again,
  which releases the hold.

### Screenshots

The environment list, answering "what is running, is it healthy, is an upgrade
in flight" at a glance:

![environment list](./static/orchestratord_api_ui/overview.png)

The configuration card, with the editable rows carrying a pencil:

![configuration card](./static/orchestratord_api_ui/configuration.png)

What is actually running, kept separate from the configuration above it:

![workloads](./static/orchestratord_api_ui/workloads.png)

Editing a row in place. The rollout strategy is a select, since it is a closed
set and one of its values causes downtime:

![editing the rollout strategy](./static/orchestratord_api_ui/rollout-strategy-edit.png)

A rollout in flight, showing the controller's own stages:

![rollout in progress](./static/orchestratord_api_ui/rollout-rail.png)

### Helm chart

A chart of its own, `misc/helm-charts/orchestratord-console`, installed
separately from the operator. The operator's chart is untouched, so an existing
install is unaffected by this whole change and the console is opt-in by not
installing it.

The chart is a Deployment, a ClusterIP Service, a ServiceAccount, and a
ClusterRole. The role is written to be visibly narrower than the operator's:
read on the custom resources and the workloads it displays, `update` and
`patch` on Materialize for the rollout and spec endpoints, `patch` on pods for
the ConfigMap refresh, and read plus `create`/`update` on ConfigMaps for system
parameters. There is no Secret access, which is the difference that matters
given the console has no authentication.

The Service is ClusterIP with no ingress and no note suggesting one, for the
same reason.

## Minimal Viable Prototype

The implementation above is itself close to minimal. The MVP cut, if needed:
read-only endpoints plus the overview UI, no mutations. The mutation
endpoints are small and well-isolated, so they ship in the same change here.

## Alternatives

- Embedding the API and UI in orchestratord. This is where the design started,
  on the grounds that the operator already holds a kube client with the
  necessary RBAC. Rejected once the permissions argument became clear: the
  console needs strictly fewer rights than the operator and none at all to
  Secrets, and embedding it hands an unauthenticated surface the operator's
  full access. Being able to leave it out of an install is also worth more than
  the image it saves.
- Splitting only the UI, leaving the API in orchestratord. Rejected: the API is
  the bulk of the code, so the operator keeps carrying it, and the result is a
  deployable that serves static files plus either CORS or a proxy hop.
- A separate repository. Deferred rather than rejected. `mz-cloud-resources` is
  not published and pulls in 387 transitive crates, so an outside repository
  would need a git dependency on the monorepo, vendored types, or untyped
  objects. Vendoring would duplicate the upgrade-window rules this deliberately
  reuses. The enabling step, if it is ever wanted, is extracting the CRD types
  into a small publishable crate: they do not touch `vpc_endpoint` (the only
  reason `mz-repr` is in the tree) and their sole non-public dependency is one
  enum.
- Serve the UI from the existing metrics or profiling ports. Rejected: those
  have established scrape/debug semantics, and mixing a mutating API into
  the metrics port would surprise anyone who has exposed it for Prometheus.
- Build the UI with a framework (React/Svelte) and a bundler. Rejected for
  now: adds a JS toolchain to the build for a small dashboard. A single
  static file keeps the Docker image and CI untouched. If the UI grows, this
  decision is cheap to revisit since the API is the stable boundary.
- Expose generic CR editing (PATCH with arbitrary JSON). Rejected: with no
  auth, the API should expose the smallest useful set of mutations, each with
  clear semantics, rather than a general write path.

## Open questions

- Should the API refuse mutations when `--disable-authentication` is not set
  once auth exists? (Placeholder for the future auth design; today there is
  no auth by construction.)
- Whether to surface pod resource usage (the operator can already read
  `metrics.k8s.io` when `--collect-pod-metrics` is set). Left out to keep the
  first cut small.
