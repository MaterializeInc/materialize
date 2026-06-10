---
source: src/orchestrator-kubernetes/src/util.rs
revision: 04d29fc3e4
---

# mz-orchestrator-kubernetes::util

Provides the `create_client` helper that constructs a `kube::Client` and returns the default namespace.
Attempts to load configuration from a named kubeconfig context first, falling back to in-cluster environment variables if that fails.
Sets conservative connect/read/write timeouts (10 s / 60 s / 60 s) on the resulting client.
