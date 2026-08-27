---
source: src/mz-debug/src/kubectl_port_forwarder.rs
revision: 253293ef87
---

# mz-debug::kubectl_port_forwarder

Provides `KubectlPortForwarder` and the resulting `PortForwardConnection` token for establishing `kubectl port-forward` sessions to Kubernetes resources.
`KubectlPortForwarder` targets either a `PortForwardTarget::Service` or a `PortForwardTarget::Pod`. Forwarding a service lets Kubernetes route to any backing pod (suitable when any replica will do); forwarding a pod reaches a specific process (required for profiling scaled multi-pod replicas).
`spawn_port_forward` launches a `kubectl` subprocess, parses its stdout to detect the assigned local address and port, and returns a `PortForwardConnection` whose drop kills the child process.
Also exposes `find_environmentd_service` and `find_cluster_services`, which query the Kubernetes API to locate the relevant services; `find_service_pods`, which lists the pod names backing a service by its label selector (returns an empty vec for an empty selector, and sorts pod names for deterministic output); and `create_pg_wire_port_forwarder`, which combines these into a ready-to-use forwarder targeting the `sql` port.
