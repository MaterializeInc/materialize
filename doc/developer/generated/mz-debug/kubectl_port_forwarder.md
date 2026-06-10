---
source: src/mz-debug/src/kubectl_port_forwarder.rs
revision: 047f99dfe6
---

# mz-debug::kubectl_port_forwarder

Provides `KubectlPortForwarder` and the resulting `PortForwardConnection` token for establishing `kubectl port-forward` sessions to Kubernetes services.
`spawn_port_forward` launches a `kubectl` subprocess, parses its stdout to detect the assigned local address and port, and returns a `PortForwardConnection` whose drop kills the child process.
Also exposes `find_environmentd_service` and `find_cluster_services`, which query the Kubernetes API to locate the relevant services, and `create_pg_wire_port_forwarder`, which combines these into a ready-to-use forwarder targeting the `sql` port.
