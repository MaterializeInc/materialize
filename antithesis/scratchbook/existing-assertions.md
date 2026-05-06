# Existing Antithesis SDK Assertions

## Summary

**No Antithesis SDK assertions exist in the Materialize source code.**

A comprehensive search of the Rust codebase at `materialize/src/` found:

- No `use antithesis` import statements
- No Cargo.toml dependencies on any antithesis crate
- No assertion macros: `assert_always!`, `assert_sometimes!`, `assert_reachable!`, `assert_unreachable!`
- No antithesis function calls in the Python test code within the materialize repository

## Existing Antithesis Integration (Customer Level)

Antithesis integration exists at the **customer-repo level** (outside the materialize source), using the legacy experiment-script approach:

### Experiment Scripts (`guest/opt/antithesis/experiment/`)

- **`materialize.py`**: Docker Compose-based experiment. Uses `antithesis.start_customer_containers()`, `antithesis.start_fault_injector()`, `antithesis.run_process()`, `antithesis.fuzz_msg()`, `antithesis.end_test()`. Orchestrates testdrive workloads with network chaos (latency, packet loss, partitions).
- **`testdrive.py`**: K8s-based variant. Sets up k3s cluster with minio, redpanda, postgres, environmentd. Runs testdrive via kubectl.
- **`materialize-k8s.sh`**: Bash setup for K8s resources.

### Docker Compose Topology (`guest/opt/materialize/docker-compose.yml`)

Uses custom Antithesis-instrumented images:
- `antithesis-cp-combined` (Kafka + Schema Registry)
- `antithesis-materialized` (Materialize)
- `antithesis-testdrive` (Test workload)

### K8s Manifests (`guest/opt/materialize/k8s/antithesis/`)

Full Kubernetes topology: environmentd StatefulSet, postgres StatefulSet, redpanda Deployment, testdrive Pod, with PVs and services.

## Implications for New Work

All property assertions will need to be added fresh. The existing integration provides a starting point for topology but uses an older approach (experiment scripts, custom instrumented images). The new approach should leverage mzcompose for compose generation and add Antithesis SDK assertions either in the workload client or (for deeper coverage) in the Materialize Rust source.
