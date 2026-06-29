---
source: src/clusterd-test-driver/src/ctp.rs
revision: 6d4c0fbb2b
---

# mz-clusterd-test-driver::ctp

CTP (compute transport protocol) connection and the `Hello` step of the controller handshake.

`ComputeCtpClient` is a type alias for `Client<ComputeCommand, ComputeResponse>`.

`connect_and_hello(compute_addr)` opens a transport connection to a clusterd compute controller address and sends the `Hello` message using the release version from `mz_persist_client::BUILD_INFO` (so the version check passes against a real clusterd binary). The controller handshake proper — `CreateInstance`, `UpdateConfiguration`, `InitializationComplete` — is left to the caller, giving explicit control over instance config and reconciliation window timing.

A reconnect re-runs exactly this: a fresh transport connection plus `Hello`.
