// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Status API and web console for the Materialize Kubernetes operator.
//!
//! This is a separate deployable from `orchestratord` on purpose. Everything it
//! does goes through the Kubernetes API, so it shares no state with the
//! operator and needs strictly fewer permissions: it never reads Secrets, and
//! it can be installed, upgraded, or left out entirely without touching the
//! component that manages databases.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod api;

#[derive(Parser)]
#[clap(name = "orchestratord-console")]
struct Args {
    /// Address to serve the console and its API on.
    #[clap(long, env = "CONSOLE_LISTEN_ADDRESS", default_value = "[::]:8080")]
    listen_address: SocketAddr,
    /// Namespace of the orchestratord Deployment, used to report which operator
    /// build and cloud these environments belong to. Purely informational: when
    /// it cannot be read, the console still works and simply reports less.
    #[clap(long, env = "OPERATOR_NAMESPACE")]
    operator_namespace: Option<String>,
    #[clap(long, env = "LOG_FILTER", default_value = "info")]
    log_filter: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    mz_ore::panic::install_enhanced_handler();
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&args.log_filter))
        .init();

    // Uses the in-cluster service account when running as a pod, and the
    // ambient kubeconfig otherwise, so the same binary works for `kubectl
    // port-forward` style local use.
    let client = kube::Client::try_default().await?;

    let context = Arc::new(api::Context::new(
        client,
        api::ConsoleInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            operator_namespace: args.operator_namespace,
        },
    ));

    let listener = tokio::net::TcpListener::bind(&args.listen_address).await?;
    tracing::info!("serving console on {}", args.listen_address);
    axum::serve(listener, api::router(context).into_make_service()).await?;
    Ok(())
}
