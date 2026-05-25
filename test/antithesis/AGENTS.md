Files relevant to running Materialize under Antithesis.

See [README.md](./README.md) for the canonical architecture overview, group
manifest, and authoring guide. Highlights for agents:

**mzcompose.py**
Source of truth for the kitchen-sink Antithesis topology. Every service every
workload group might want lives here (postgres-metadata, minio, materialized,
clusterd1, fault-orchestrator, workload, plus kafka+zookeeper+schema-registry,
postgres-source, mysql+mysql-replica, sql-server, polaris(+bootstrap),
clusterd2, clusterd-pool-{0..N}, upsert-hammer-{0..M}). Per-group filtering
happens at export time, not here.

**groups.yaml + groups.py**
Manifest: 10 workload groups (kafka, pg-cdc, mysql-cdc, sql-server-cdc,
parallel-workload, upsert-stress, testdrive, workload-replay, platform-checks,
combined). Each group declares its extra services beyond the universal set,
plus its `setup` / `drivers` / `anytime` scripts. `default_drivers` and
`default_anytime` are auto-merged into every group's runtime template at
image build time.

**export-compose.py**
Filters `mzcompose.py` to one group, prunes dropped depends_on, resolves
mzbuild → `${MATERIALIZED_IMAGE}` / `${ANTITHESIS_WORKLOAD_IMAGE}` placeholders
(per-group `antithesis-workload-<group>`), strips host bind-mounts + unsafe
env vars (MZ_EAT_MY_DATA, MZ_LICENSE_KEY, host-path configs), upgrades
`service_started` → `service_healthy` where the dependency has a healthcheck.

**export-env.py**
Writes the per-group `.env` (image fingerprints).

**workload/** (per-group flavors live under `workloads/<group>/`)
Mzbuild image for the Python test driver — one image per group with the same
Dockerfile (symlinked) and per-group `ANTITHESIS_WORKLOAD_GROUP` build-arg.
The `bake-test-templates.sh` script copies the active group's scripts into
`/opt/antithesis/test/v1/<template>/` at image build time. Test command files
must use one of the Test Composer prefixes (`first_`, `parallel_driver_`,
`singleton_driver_`, `anytime_`); `helper_*.py` files are imported by drivers
and ignored by Test Composer's command discovery.

**configs/<group>/**
Each group has its own `antithesis-config-<group>` image — FROM scratch tarball
holding the generated docker-compose.yaml + .env. Antithesis ingests this.

**setup-complete.sh** (in `workload/`)
Notify Antithesis that the SUT is up and the workload entrypoint has
provisioned the cluster. Test commands begin after this event fires.

**fault-orchestrator/pause_faults.sh**
Single bash container alternating `ANTITHESIS_STOP_FAULTS` quiet/active
windows globally so every driver tunes its budgets against one shared cadence
(MIN_ON=20, MAX_ON=40, MIN_OFF=20, MAX_OFF=40 by default).

**push-antithesis.py**
CI: retags + pushes every Materialize-built image (materialized,
antithesis-workload-<group> × 10, antithesis-config-<group> × 10,
antithesis-upsert-hammer) to the Antithesis GCP Artifact Registry.
