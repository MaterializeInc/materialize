# Antithesis harness

Runs self-managed Materialize, deployed by the real orchestratord on
Kubernetes, under [Antithesis](https://antithesis.com). The target is the class
of bugs that only appear at a specific restart point, under a concurrent-writer
race, or in a timing window: 0dt rollouts, persist leases and finalization,
controller and catalog state rebuilt at boot, and source ingestion across
restarts.

Research, the property catalog, and the reasoning behind every check live in
`antithesis/scratchbook/` at the repository root. Start with
`property-catalog.md`.

## Layout

| Path | What |
|---|---|
| `manifests/` | Kubernetes manifest templates and the orchestratord Helm values |
| `workload/` | mzbuild image `antithesis-workload`: the Python workload, its entrypoint, and the test template under `test/main/` |
| `config/manifests/` | Rendered manifests (gitignored: they embed the license key) |
| `misc/python/materialize/antithesis/` | Workload library and drivers (importable outside Antithesis) |
| `misc/python/materialize/orchestratord/` | Kubernetes client for an orchestratord-managed environment |

The Materialize CR is not in the manifests. orchestratord registers its CRD at
startup, so the workload's setup step (`materialize.antithesis.setup`) creates
the CR, waits for the first generation to serve SQL, and emits
`setup_complete`.

## Instrumentation

The Antithesis build flavor (`bin/mzimage ... --antithesis`, or
`CI_ANTITHESIS=1`) builds with sancov coverage flags and the
`mz-ore/antithesis` feature, which links the Antithesis SDK and reports:

- every process panic as the failed property `Materialize process panicked`,
  except panics marked with `mz_ore::antithesis::expect_panic()`;
- every soft assertion that logs instead of panicking as
  `Materialize soft assertion failed`;
- assertions placed with `mz_ore::antithesis_{always,sometimes,reachable,unreachable}!`.

Without the feature all of these compile to nothing.

## Memory and resource kills

The Antithesis VM has about 10 GiB, and every pod shares it, including two
environmentd generations and two sets of clusterd replicas during a rollout.
environmentd is limited to 2 GiB (`environmentdResourceRequirements` in the
CR), clusterd to 1 GiB per container (the `clusterd-memory` LimitRange, since
replica sizes carry no memory limit), orchestratord to 512 MiB. The
dependencies and the workload have no limit.

A kill for memory exits like a crash, so tell them apart before reading a
dead pod as a Materialize bug:

- A panic reports `Materialize process panicked` from inside the process. An
  OOM kill or an eviction is a SIGKILL (137) with no panic message. The
  platform's `No unexpected crashes` and `No unexpected container exits` skip
  137, and `anytime_pod_restarts` counts it as an explained exit and leaves
  the verdict to the resource kill check below.
- `anytime_resource_kills` and `finally_resource_kills` read every pod, the
  eviction and OOM events, and the node conditions. `No Materialize container
  is OOM-killed at its own memory limit` means environmentd, clusterd or
  orchestratord outgrew its own limit while the node had memory: a
  Materialize finding, with the limit in the details. `No pod is evicted and
  no process is OOM-killed for lack of node memory` and `The node never
  reports memory pressure` mean the harness does not fit the VM: fix the
  sizing, not Materialize. Restarts and failures in the same timeline after
  one of these are suspect.
- The platform's `Peak memory usage` fails above 95% of the VM's memory. Its
  examples list the largest processes, which shows whose growth to look at.

## Running locally

```
bin/pyactivate -m materialize.antithesis.local up --build-dir <git checkout>
bin/pyactivate -m materialize.antithesis.local down
```

`up` builds the Antithesis flavor of environmentd, clusterd, orchestratord,
and the workload; loads them into a kind cluster; renders and applies the
manifests; and waits for `setup_complete`. mzbuild needs a git work tree, so
from a jj secondary workspace pass a git worktree checked out at the same
commit. The license key comes from `MZ_CI_LICENSE_KEY` or
`~/.config/materialize/antithesis-license-key`.

`--upgrade-from v26.45.0` starts the environment on that release's published
images instead, for the upgrade scenario below. The published images work on
kind; only Antithesis needs them rebuilt.

## Cross-version upgrades

Rendered with `--upgrade-from-environmentd-image` and
`--upgrade-from-clusterd-image`, the environment starts on an older release and
the rollouts driver's `upgrade` action rolls out the image under test while the
other drivers keep running. The action's weight is never zero, so every timeline
can leave the old release, and once the spec names the new image every other
action keeps it. Some upgrades start under `ManuallyPromote` and are cancelled
before promotion, after which the old release must still serve.

The observer checks that the version serving SQL never goes backwards, and
convergence checks that an upgraded environment serves a newer version than the
one it started on.

The base release is the newest release of the previous minor version
(`images.upgrade_base_version`). The published release images seed AWS-LC from
CPU jitter, which aborts on Antithesis's deterministic CPU, so CI rebuilds
environmentd and clusterd from the release tag without jitter entropy and pushes
them as `<release>--antithesis.upgrade-base`. They have no coverage
instrumentation. Every image tag starts with its version
(`images.image_tag`), because orchestratord reads it to gate environmentd
arguments and to refuse rollbacks.

## Submitting to Antithesis

CI builds the images on PRs labeled `ci-antithesis` (see `ci/README.md`) and
pushes them to the Antithesis registry. The step's `antithesis-images.json`
artifact lists the references, including the upgrade base images. Render the
manifests against them, then validate and launch:

```
bin/pyactivate -m materialize.antithesis.render --output test/antithesis/config/manifests \
    --environmentd-image <environmentd> --clusterd-image <clusterd> \
    --orchestratord-image <orchestratord> --workload-image <antithesis-workload> \
    [--upgrade-from-environmentd-image <upgrade-base-environmentd> \
     --upgrade-from-clusterd-image <upgrade-base-clusterd>]
snouty validate test/antithesis/config
snouty launch --webhook basic_k8s_test --config test/antithesis/config --duration <minutes> ...
```

snouty builds the config image (`FROM scratch` with the rendered manifests under
`/manifests`) and pushes it to `ANTITHESIS_REPOSITORY`. environmentd will not
start under Kubernetes without a license key, and Antithesis runs air-gapped,
so the key is baked into the rendered `materialize-backend` Secret. The config
image must therefore only ever go to the tenant's private Antithesis registry,
never to GHCR.
