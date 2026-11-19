// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![recursion_limit = "256"]

use std::net::SocketAddr;

use futures::FutureExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::task;
use mz_pgwire::{Config, MetricsConfig, Server};
use mz_pgwire_common::ConnectionCounter;
use mz_server_core::listeners::{AllowedRoles, AuthenticatorKind};
use mz_server_core::{STARTING_UP_MESSAGE, ServeConfig};
use tokio::sync::oneshot;
use tokio_postgres::NoTls;
use tokio_postgres::error::SqlState;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `socket`
async fn startup_before_boot_is_rejected_with_cannot_connect_now() {
    // Holding the sender without sending keeps the server booting.
    let (_adapter_handles_tx, adapter_handles_rx) = oneshot::channel();
    let server = Server::new(Config {
        label: "test",
        adapter_handles_rx: adapter_handles_rx.shared(),
        tls: None,
        frontegg: None,
        authenticator_kind: AuthenticatorKind::None,
        metrics: MetricsConfig::register_into(&MetricsRegistry::new()),
        active_connection_counter: ConnectionCounter::default(),
        helm_chart_version: None,
        allowed_roles: AllowedRoles::Normal,
    });
    let (handle, conns) = mz_server_core::listen(&SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
    let _server_task = task::spawn(|| "pgwire_server", {
        mz_server_core::serve(ServeConfig {
            conns,
            server,
            dyncfg: None,
        })
    });

    let addr = handle.local_addr();
    let Err(err) = tokio_postgres::connect(
        &format!("host={} port={} user=materialize", addr.ip(), addr.port()),
        NoTls,
    )
    .await
    else {
        panic!("connecting before boot should fail");
    };
    assert_eq!(err.code(), Some(&SqlState::CANNOT_CONNECT_NOW), "{err}");
    let db_err = err
        .as_db_error()
        .expect("error should come from the server");
    assert_eq!(db_err.severity(), "FATAL");
    assert_eq!(db_err.message(), STARTING_UP_MESSAGE);
}
