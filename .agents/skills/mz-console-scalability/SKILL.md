---
name: mz-console-scalability
description: >
  Run the Console scalability suite against a replayed captured workload and
  measure what Console load costs mz_catalog_server: a replay-only baseline,
  the Playwright suite, the k6 ramp, and an optional sweep of N headless
  viewers fitted to CPU per viewer. Trigger: "console scalability test",
  "run the console against a captured workload", "what does this page cost
  catalog_server", "cost per viewer", "how many users fit on M.1-nano",
  "catalog server CPU under console load", or comparing two console versions
  on catalog_server cost. For one query's plan and latency use
  mz-query-perf. For SQL throughput benchmarks under test/scalability use
  mz-benchmark.
argument-hint: [workload-file]
allowed-tools: [Bash, Read, Grep, Glob]
---

# Console scalability and catalog_server cost

The `console-scalability` workflow in `test/workload-replay/mzcompose.py`
boots a captured workload (objects, initial data, hydration), keeps its
ingestions and queries running, and drives the console bundled in the
materialized image against it. Every console query runs on
`mz_catalog_server`, so its CPU is what limits how many viewers an
environment can hold.

## What a run produces

Per workload, in order:

| Phase | Load on top of the replay | Other output |
|---|---|---|
| `baseline` | none | |
| `playwright` | the `console/e2e-tests/scalability` specs, 1 to 6 tabs | `console/test-results/<workload>/`, query tables in the log |
| `k6` | the suite's captured queries, ramped to 100 virtual users over the SQL HTTP API | `console/e2e-tests/k6/{summary,queries}-<workload>.json` |
| `baseline-before-viewers`, `viewers-<N>`, `baseline-after-viewers` | with `--viewers`, N headless viewers holding one page | |

`console/test-results/catalog-server-<workload>.json` holds, per phase:

* `cores.catalog_server`, `cores.environmentd`: process CPU from /proc in
  cores. Absolute, not above baseline. `null` when the process restarted
  during the phase.
* `scheduled_cores`: time inside dataflow operators on catalog_server, split
  into `index`, `subscribe`, `introspection_subscribe` and `other`.
* `subscribes`: open subscribes at the end of the phase, and the console
  subscribe dataflows' count and average operator count.
* `top_dataflows`: the ten non-subscribe dataflows with the most scheduled
  time, with operator counts.

With `--viewers`, `viewer_fit.<process>.mcpu_per_viewer` is a least-squares
slope through the origin of each step's cores above the mean of the two
viewer baselines, and `above_baseline` lists the points it was fitted to.
The log ends with the same numbers as a table.

The nightly `console-scalability` step runs with `--skip-large` and no
`--viewers`. Its artifacts carry the first three phases for every workload.

## Run it locally

### Before you start

* **Run from a commit whose images CI has built.** mzcompose pulls images by
  fingerprint and builds the ones ghcr lacks, and a local materialized build
  is a full Rust cross-compile (budget 100+ GiB of `target-xcompile` in a
  fresh worktree). The materialized image bundles the console, so any file
  under `console/` changes its fingerprint, untracked files included.
  Git-ignored files do not count. CI's build step pushes every image it
  builds, so a pushed branch becomes runnable once its build step for your
  architecture has finished. Check before starting:

  ```bash
  bin/mzcompose --find workload-replay config | sed -n 's/.*image: \(ghcr[^ ]*\)/\1/p' | sort -u |
    while read -r image; do
      docker image inspect "$image" > /dev/null 2>&1 || docker manifest inspect "$image" > /dev/null 2>&1 \
        && echo "ok      $image" || echo "MISSING $image"
    done
  ```

* **Python 3.13** must be the `python3` on PATH, since `bin/pyactivate` runs
  it. When `VIRTUAL_ENV` points at another venv, as VS Code terminals set
  it, `bin/pyactivate` installs into that venv. Set
  `VIRTUAL_ENV=$PWD/misc/python/venv`.
* **Captured workloads** come from a private repository the workflow clones
  into `test/workload-replay/captured-workloads`. In a worktree, symlink the
  main checkout's copy. Workloads tagged `settings.large: true` take far
  longer and more memory.
* **Keep the host awake and the run detached.** Laptop sleep was observed to
  halt environmentd (exit 166, a persist `Listen ... received a batch` halt
  after the timestamp jumped), and nothing restarts it, so the run is lost. A
  command an agent runs in the background is killed at the tool's time limit,
  which stops the replay. Start the run with `nohup caffeinate -dis ... &` on
  AC power and follow the log. `caffeinate` cannot stop a lid-close sleep on
  battery.
* **One run per compose project.** A run starts by killing and removing every
  container of its project. Pass `--project-name <name>` to run beside
  another replay.

### Command

```bash
PYTHONUNBUFFERED=1 nohup caffeinate -dis bin/mzcompose --find workload-replay \
  run console-scalability --viewers 10,20,30 --cost-window 120 workload_ddnet.yml \
  > console-scalability.log 2>&1 &
grep -E '^\+\+\+|^-- catalog_server cost|Traceback' console-scalability.log
```

On a laptop this takes about 55 minutes: about 10 to boot and hydrate, 20
for the Playwright suite, 6 for k6, and the rest for the viewer sweep.

| Flag | Default | Notes |
|---|---|---|
| `--catalog-server-size` | the bootstrap size | Same worker count and CPU limit as `M.1-nano`. Pin another size to compare. |
| `--console-bundle` | the image's console | A `dist` directory to serve instead, copied in after hydration. See "Compare two console versions". |
| `--viewers` | none | Comma-separated viewer counts. Use three or more to see whether cost grows linearly. |
| `--viewer-path` | the `mz_catalog_server` cluster page | Console path the viewers hold, such as `/regions/local-flexible-deployment/clusters/u2/compute_cluster`. The page matters: on `workload_ddnet`, `main` measured 9.5 mCPU per viewer on the `mz_catalog_server` page and 7.3 on a user cluster's page. User cluster ids follow the order of `clusters` in the workload file, starting at `u2`. |
| `--cost-window` | 90 | Seconds per measured window: the baselines and each viewer step. |
| `--cost-settle` | 30 | Seconds of settling before each window. |
| `--seed` | time | Seeds the replay's generated data and query choices. Fix it when comparing runs. |

The replay's own `--factor-initial-data`, `--factor-ingestions`,
`--factor-queries` and `--max-concurrent-queries` apply too.

## Read the numbers

* **Subtract the baseline.** Every core figure is absolute and includes the
  replay's own queries. What the console costs is a phase minus `baseline`.
* **Check the fit before quoting it.** The slope assumes cost grows in
  proportion to viewers. On `workload_ddnet` the cost per viewer was lower at
  10 viewers than at 20 or 30, so treat `mcpu_per_viewer` as an average over
  the measured range and quote it with that range. A single step far off the
  others usually means its pages did not finish loading: look for
  `page-layout not found` in the log.
* **Process CPU minus scheduled time** is work outside dataflow operators:
  session handling, peeks served from indexes, networking, and dataflows that
  were dropped before the phase ended. A gap that grows with polling load
  points at per-request cost. Each new SQL session was measured at about
  35 ms of catalog_server CPU, and every HTTP `/api/sql` request opens one,
  where a WebSocket keeps one session.
* **Operator counts explain most dataflow cost.** Measured on a one-worker
  catalog_server: about 3 microcores per operator for a maintained index over
  static input, and 3.5 to 5 microcores per operator per viewer for a console
  subscribe. A subscribe with hundreds of operators per viewer is the slope
  to cut, see `subscribes.avg_operators`.
* **Local readings are not capped.** Observed: the process orchestrator does
  not enforce the replica's CPU limit, so catalog_server can read above the
  0.5 cores an `M.1-nano` gets in production, up to about one core per
  worker. A reading above 0.5 means production would saturate.
* **Do not read `mz_cluster_replica_metrics_history` locally.** It was
  observed reading 2.7 to 3.2 times the /proc figure.
* **k6 over-polls queries it does not know.** `console/e2e-tests/k6/cluster-detail.ts`
  replays every label in the captured `queries.json` and polls a label missing
  from its `POLL_INTERVAL_MS` table every 5 seconds per virtual user. A console
  change that adds queries therefore reads costlier in the `k6` phase than it
  is. Diff the two runs' `queries-<workload>.json` labels, and add the new ones
  to `POLL_INTERVAL_MS` with the console's real cadence before comparing `k6`.
  The viewer sweep runs the real console and has no such gap.
* **Browsers share the host.** The viewers and the suite run on the same
  machine as materialized. Compare runs on the same host, and repeat a run
  before trusting a difference of a few percent.

## Compare two console versions

Keep the CI-built images and swap only the console. Build each version in its
own worktree the way the image does, then run once per build with the same
host, workload, `--seed` and flags:

```bash
(cd console && CONSOLE_DEPLOYMENT_MODE=flexible-deployment yarn build)
bin/mzcompose --find workload-replay run console-scalability --seed 1234 \
  --viewers 10,20,30 --console-bundle /path/to/worktree/console/dist workload_ddnet.yml
```

Run the baseline side without `--console-bundle`, or with a build of the base
commit. A feature behind a LaunchDarkly flag is on in this mode unless
`disabledFlexibleDeploymentFlags` in `console/src/config/flexibleDeploymentFlags.ts`
turns it off, so force it on in the build's worktree, never in a commit.

To check the swap took, compare `curl http://127.0.0.1:<6874 port>/index.html`
with the build's `dist/index.html`, and diff the two runs'
`queries-<workload>.json` labels: the new page's queries should appear.
Compare each phase minus its own run's baseline, and `mcpu_per_viewer`, not
absolute cores.
