This directory contains files relevant to running tests in Antithesis.

Use the `antithesis-setup` skill to scaffold and manage this directory. Use
the `antithesis-research` skill to analyze the system and build a property
catalog. Use the `antithesis-workload` skill to implement assertions and test
commands. Use the `antithesis-launch` skill to build, validate, and submit
Antithesis runs — do not run `snouty run` directly.

## Scenarios

Antithesis runs are organized as **N narrow scenarios**, each with its own
compose file under `configs/<name>/`. The split rule and rationale live in
`scratchbook/scenario-strategy.md`. Each scenario is launched separately via
`snouty launch -c antithesis/configs/<name> --source <name>`.

Available scenarios:

| Scenario | Topology | Status |
| --- | --- | --- |
| `base` | materialized + redpanda + driver | active |

When adding a scenario:

1. Drop a new `configs/<name>/mzcompose.py` defining its `SERVICES`.
2. Run `bin/pyactivate antithesis/bin/render-compose-yaml.py --scenario <name>`
   to produce `configs/<name>/docker-compose.yaml`.
3. Add an entry to the table above.

## snouty validate

Validate a scenario locally:

    snouty validate antithesis/configs/<scenario>

The validator runs Docker Compose, watches for the `setup_complete` event,
and inspects discovered Test Composer commands. Run `bin/mzcompose --find
<scenario> build` (or `bin/mzimage acquire --arch x86_64 antithesis-test-driver`)
first to make sure the local images are present.

> **Apple Silicon caveat.** The compose configs pin every service to
> `platform: linux/amd64` because the Antithesis platform is amd64-only. On
> macOS / Apple Silicon, Docker runs amd64 containers under Rosetta, and the
> `clusterd` subprocess that `materialized` spawns segfaults during lgalloc
> initialization (`ExitStatus(unix_wait_status(11))` → container `Exited (139)`).
> This is a host-side emulation limitation, not a harness bug. Run
> `snouty validate` on Linux/amd64 (CI, an x86 dev box, or a remote runner)
> to exercise the full bring-up.

## snouty launch

Once validation passes, use the `antithesis-launch` skill to submit a run.

## Directory layout

**configs/**
One subdirectory per scenario. Each contains:
* `mzcompose.py` — source-of-truth composition for the scenario.
* `docker-compose.yaml` — generated artifact consumed by snouty / Antithesis.

**bin/**
Helpers that operate on this directory:

* `render-compose-yaml.py` — regenerates `configs/<scenario>/docker-compose.yaml`
  from `configs/<scenario>/mzcompose.py` while layering in the
  Antithesis-required attributes mzcompose does not emit
  (`platform: linux/amd64`, matching `container_name`/`hostname`,
  `NO_COLOR=1`, no host port bindings):

      bin/pyactivate antithesis/bin/render-compose-yaml.py --scenario <name>

**test/**
Shared Test Composer command tree. A test template is a directory under
`test/v1/<template>/` containing executable command files. Each command must
have a valid prefix: `parallel_driver_`, `singleton_driver_`, `serial_driver_`,
`first_`, `eventually_`, `finally_`, `anytime_`. Files or directories prefixed
with `helper_` are ignored by Antithesis and can hold shared helper scripts.

The `test-driver` mzbuild image bundles the entire `test/v1/` tree. Antithesis
selects exactly one template per execution history, so a scenario's compose
file controls **which** template runs by virtue of the topology it provides
(e.g. the `mysql_mt_replicas` scenario would only have the MySQL templates'
dependencies satisfied). See
`https://antithesis.com/docs/test_templates/first_test/`.

**test-driver/**
The mzbuild image used by the `antithesis-test-driver` service. Inherits
`MZFROM testdrive` so the testdrive binary, postgres client, and shared
dependencies are already present. Adds Python plus the Antithesis Python
SDK and copies in `misc/python` (so `materialize.antithesis.*` is importable)
and `antithesis/test/v1` (so commands are discoverable at
`/opt/antithesis/test/v1`). One image is shared across scenarios.

**scratchbook/**
Antithesis scratchbook for the codebase: SUT analysis, deployment topology,
scenario strategy, test-driver integration plan, per-property evidence files,
and other persistent integration notes. Keep it up to date as
Antithesis-related decisions change.

**setup-complete.sh**
Reference fallback script that writes the `setup_complete` lifecycle event to
`${ANTITHESIS_OUTPUT_DIR}/sdk.jsonl`. The current `test-driver` entrypoint
emits the same event through the Antithesis Python SDK
(`antithesis.lifecycle.setup_complete`), which is the canonical path. Keep
this script around for any container that cannot link the SDK directly.
