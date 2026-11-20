# Antithesis harness

Runs self-managed Materialize, deployed by the real orchestratord on
Kubernetes, under [Antithesis](https://antithesis.com). The target is the class
of bugs that only appear at a specific restart point, under a concurrent-writer
race, or in a timing window: 0dt rollouts, persist leases and finalization,
controller and catalog state rebuilt at boot, and source ingestion across
restarts.

The workload is one process in the fault-excluded `driver` pod: a
[hegel](https://hegel.dev) state machine whose rules send SQL and Kubernetes
requests and check every outcome against in-memory models. Its one test
command, `first_start_workload`, starts the workload. An eventually rule stops
faults, runs the post-fault checks and ends the timeline. The design, and a
comparison with the Test Composer workload it replaced, are in
[the design doc](../../doc/developer/design/20261120_antithesis_driver.md).

## What the driver checks

| Component | Guarantees |
|---|---|
| Client history (`history.py`) | Strict serializable, serializable and strong session serializable appends and reads of several keys, through tables, an index, materialized views and a join, checked for G0 to G2 anomalies with real-time edges by `serializability.py`, and for stale, lost, phantom and aborted reads |
| Contended counters (`counters.py`) | Read-then-write `UPDATE` and `INSERT ... SELECT` stay within the bounds of increments acknowledged and attempted |
| Atomic commits (`atomic.py`) | No read sees part of a multi-table write transaction, or a rolled-back one |
| IVM (`views.py`) | Twelve view kinds, each as an indexed view and a materialized view, equal a Python reference (`ivm_model.py`) on single reads and at every closed timestamp of a `SUBSCRIBE` |
| Disruptions (`disrupt.py`) | Replica changes, deletions of environmentd, clusterd and orchestratord pods, and 0dt rollouts, some manually promoted, with kills in a chosen phase. `activeGeneration` never decreases |
| After faults stop (`health.py`, each component's `eventually`) | The CR settles, SQL serves, replicas come online, every pending statement finishes, every component's final read succeeds and passes its checks, no pod crash-loops |

A guarantee is an `always`, which fails when a run never evaluates it, and a
situation worth reaching is a `sometimes` or `reachable`, which fails when a
run never reaches it. Expect the latter on situations that need faults in
short runs. The assertion messages are those of the legacy workload's checks,
so their properties keep their history. hegel reports the run's own verdict
as `::materialize_driver passes properties`, which fails if the state machine
raised.

Not ported yet, in the order the design doc gives: the rollout status claims,
exit evidence, Postgres and Kafka sources, Kafka sinks, read-only isolation,
and the smaller families listed there.

## Running locally

```
bin/pyactivate -m pip install -r misc/python/materialize/antithesis/requirements.txt
bin/pyactivate -m pytest -q misc/python/materialize/antithesis misc/python/materialize/antithesis_driver
bin/pyactivate -m materialize.antithesis_driver.local --output /tmp/driver --duration 300 standalone
bin/pyactivate -m materialize.antithesis_driver.local --output /tmp/driver --duration 300 kind
```

`standalone` runs the driver on this machine against a fresh
`materialize/materialized` container. `kind` deploys these manifests into a
kind cluster with a release's published environmentd, clusterd and
orchestratord images and a locally built driver image, and needs `kind`,
`kubectl`, `helm` and the license key (`MZ_CI_LICENSE_KEY` or
`~/.config/materialize/antithesis-license-key`). Either runs the workload for
`--duration` seconds, then the eventually, and lists the assertions a run
would fail, including those never reached. Nothing injects faults locally.
A kind run keeps a second copy of every image in the node, so check free disk
space first and `kind delete cluster --name mz-antithesis-driver` afterwards.

## Layout

| Path | What |
|---|---|
| `manifests/` | Kubernetes manifest templates and the orchestratord Helm values |
| `driver/` | mzbuild image `antithesis-driver`: the driver and its `first_start_workload` command |
| `config/manifests/` | Rendered manifests (gitignored: they embed the license key) |
| `misc/python/materialize/antithesis_driver/` | The driver's state machine, components and local loop |
| `misc/python/materialize/antithesis/` | Rendering, images, SQL error classification, and the pure models the checks use |
| `misc/python/materialize/orchestratord/` | Kubernetes client for an orchestratord-managed environment |

The Materialize CR is not in the manifests. orchestratord registers its CRD at
startup, so the driver creates the CR, waits for the first generation to serve
SQL, and then reports `setup_complete`.

## Instrumentation

The Antithesis build flavor (`bin/mzimage ... --antithesis`, or
`CI_ANTITHESIS=1`) builds with sancov coverage flags and the
`mz-ore/antithesis` feature, which links the Antithesis SDK and reports:

- every process panic as the failed property `Materialize process panicked`,
  except panics marked with `mz_ore::antithesis::expect_panic()`;
- every `halt!` as a per-site `process halted at <file>:<line>` event, plus a
  verdict from `mz_ore::antithesis::classify_halt`: a structured observation
  for messages on the reviewed `DESIGNED_HALTS` list, a dedicated failed
  property for halts of filed bugs, and `Materialize process
  halted for an unexpected reason` for everything else;
- every soft assertion that logs instead of panicking as
  `Materialize soft assertion failed`;
- optional-hit safety assertions placed with `mz_ore::antithesis_always!` and
  `mz_ore::antithesis_unreachable!`, and coverage guidance from
  `mz_ore::antithesis_{sometimes,reachable}!`, which marks rare branches for
  exploration without becoming a property.

Without the feature all of these compile to nothing. The driver's own Python
code is cataloged and coverage-instrumented from `/opt/antithesis/catalog/` in
its image.

## Memory

The Antithesis VM has about 10 GiB, and every pod shares it, including two
environmentd generations and two sets of clusterd replicas during a rollout.
environmentd is limited to 2 GiB (`environmentdResourceRequirements` in the
CR), clusterd to 1 GiB per container (the `clusterd-memory` LimitRange, since
replica sizes carry no memory limit), orchestratord to 512 MiB. The
dependencies and the driver have no limit.

A kill for memory exits like a crash. A panic reports `Materialize process
panicked` from inside the process. An OOM kill or an eviction is a SIGKILL
(137) with no panic message, which the platform's `No unexpected crashes` and
`No unexpected container exits` skip. The platform's `Peak memory usage` fails
above 95% of the VM's memory, and its examples list the largest processes.

## Cross-version upgrades

Rendered with `--upgrade-from-environmentd-image` and
`--upgrade-from-clusterd-image`, the environment starts on an older release,
and half the driver's rollouts move it to the image under test. Once the spec
names the new image, later rollouts keep it.

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
pushes them to the Antithesis registry. The step requires `MZ_CI_LICENSE_KEY`
to render and publish the config image against those exact references.
The step's `antithesis-images.json` artifact lists the images, upgrade base
images, and a `config` reference pinned to its pushed digest. Download the
artifact and launch without a local registry login, excluding the driver from
faults:

```
snouty launch --launcher basic_k8s_test \
    --config-image "$(jq -r '.config' antithesis-images.json)" \
    --param "antithesis.images=$(jq -r 'del(.config, ."upgrade-base-version") | [.[]] | join(";")' antithesis-images.json)" \
    --fault-tolerance high_availability --fault-exclusions '^driver$' \
    --max-compute-hours <hours> ...
```

To render and validate the manifests locally:

```
bin/pyactivate -m materialize.antithesis.render --output test/antithesis/config/manifests \
    --environmentd-image <environmentd> --clusterd-image <clusterd> \
    --orchestratord-image <orchestratord> --workload-image <antithesis-driver> \
    [--upgrade-from-environmentd-image <upgrade-base-environmentd> \
     --upgrade-from-clusterd-image <upgrade-base-clusterd>]
snouty validate test/antithesis/config
snouty launch --launcher basic_k8s_test --config test/antithesis/config \
    --fault-tolerance high_availability --fault-exclusions '^driver$' ...
```

snouty builds the config image (`FROM scratch` with the rendered manifests under
`/manifests`) and pushes it to `ANTITHESIS_REPOSITORY`. environmentd will not
start under Kubernetes without a license key, and Antithesis runs air-gapped,
so the key is baked into the rendered `materialize-backend` Secret. The config
image must therefore only ever go to the tenant's private Antithesis registry,
never to GHCR.
