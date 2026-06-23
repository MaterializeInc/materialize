// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Resolves the clusterd compute controller address. The driver connects to an
//! externally-managed clusterd (mzcompose or a manually-launched process); it
//! never spawns one.
//!
//! Local iteration without mzcompose:
//!   1. Start CockroachDB (see CLAUDE.md).
//!   2. Launch the driver first so it hosts PubSub on a fixed port (e.g. 0.0.0.0:6879).
//!   3. Launch clusterd manually (run `cargo run -p mz-clusterd -- --help` to
//!      confirm required flags and the exact `TimelyConfig` JSON shape from
//!      `src/cluster-client/src/client.rs`):
//!
//! ```text
//! PERSIST_PUBSUB_URL=http://127.0.0.1:6879 \
//!   cargo run -p mz-clusterd -- \
//!     --compute-controller-listen-addr 127.0.0.1:2101 \
//!     --storage-controller-listen-addr 127.0.0.1:2100 \
//!     --compute-timely-config 'TIMELY_CONFIG_JSON' \
//!     --storage-timely-config 'TIMELY_CONFIG_JSON' \
//!     --process 0 --scratch-directory /tmp/clusterd-scratch
//! ```
//!
//!   4. Run the e2e test with CLUSTERD_COMPUTE_ADDR=127.0.0.1:2101 and the same
//!      PersistHost bind/port the clusterd PubSub URL points at.

/// The clusterd compute controller address, from `CLUSTERD_COMPUTE_ADDR`
/// (default `127.0.0.1:2101`).
pub fn compute_addr() -> String {
    std::env::var("CLUSTERD_COMPUTE_ADDR").unwrap_or_else(|_| "127.0.0.1:2101".to_string())
}

/// The clusterd Prometheus metrics endpoint, from `CLUSTERD_METRICS_URL`. Defaults
/// to clusterd's internal HTTP server on localhost (`127.0.0.1:6878`, the clap
/// default of `--internal-http-listen-addr`), which the local `run-local.py` runner
/// uses unchanged. An mzcompose run, where clusterd is a separate container, must
/// set this to that container's address (e.g. `http://clusterd:6878/metrics`).
pub fn metrics_url() -> String {
    std::env::var("CLUSTERD_METRICS_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:6878/metrics".to_string())
}

/// Whether an end-to-end test should run: true only when a target address was
/// explicitly provided. Integration tests skip when unset to keep `cargo test`
/// green without a running clusterd.
pub fn e2e_enabled() -> bool {
    std::env::var("CLUSTERD_COMPUTE_ADDR").is_ok()
}
