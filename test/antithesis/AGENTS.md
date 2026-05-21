Files relevant to running Materialize under Antithesis.

Use the `antithesis-setup` skill to scaffold and manage this directory. Use the `antithesis-research` skill to analyze the system and build a property catalog. Use the `antithesis-workload` skill to implement assertions and test commands.

**mzcompose.py**
Source of truth for the Antithesis topology. Standard mzcompose composition: services (`postgres-metadata`, `minio`, `redpanda`, `materialized`, `workload`), dependencies, env, ports. The generated `config/docker-compose.yaml` is derived from this.

**export-compose.py**
Renders `mzcompose.py` into a flat docker-compose YAML that Antithesis can consume. Images are emitted as `ghcr.io/materializeinc/materialize/<name>:mzbuild-<fingerprint>` refs that Antithesis pulls directly from public GHCR.

**workload/**
Mzbuild image (`antithesis-workload`) for the Python test driver. Dockerfile, entrypoint, and test-template scripts (`test/*.sh`) live here. Test command files must be prefixed with one of `parallel_driver_`, `singleton_driver_`, `serial_driver_`, `first_`, `eventually_`, `finally_`, `anytime_`; files prefixed with `helper_` are ignored by Test Composer.

**config/**
Mzbuild image (`antithesis-config`) — a `FROM scratch` container holding the generated `docker-compose.yaml`. This is the image Antithesis points at to bring up the environment.

**scratchbook/**
Antithesis scratchbook: system analysis, property catalog, topology plans, per-property evidence files (in `scratchbook/properties/`), property relationship maps, persistent integration notes. Keep up to date as Antithesis-related decisions change.

**setup-complete.sh** (in `workload/`)
Inject this script into a Dockerfile to notify Antithesis that setup is complete. Should only run once the system under test is ready for testing — Antithesis will not run test commands until it receives this event.
