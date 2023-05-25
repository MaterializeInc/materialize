use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use axum::{
    extract::Query,
    response::IntoResponse,
    routing::{get, IntoMakeService},
    Router, Server,
};
use mz_frontegg_auth::AppPassword;
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserAPIToken {
    client_id: String,
    secret: String,
}

/// Request handler for the server waiting the browser API token creation
// Axum requires the handler be async even though we don't await
#[allow(clippy::unused_async)]
async fn request(
    Query(BrowserAPIToken { secret, client_id }): Query<BrowserAPIToken>,
    tx: UnboundedSender<AppPassword>,
) -> impl IntoResponse {
    tx.send(AppPassword {
        client_id: client_id.parse().unwrap(),
        secret_key: secret.parse().unwrap(),
    })
    // TODO: Implement custom error
    .unwrap_or_else(|_| panic!("Error handling login details."));
    (StatusCode::OK, "You can now close the tab.")
}

/// Server for handling login's information.
pub fn server(
    tx: UnboundedSender<AppPassword>,
) -> (
    Server<hyper::server::conn::AddrIncoming, IntoMakeService<Router>>,
    u16,
) {
    let app = Router::new().route("/", get(|body| request(body, tx)));
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    let server = axum::Server::bind(&addr).serve(app.into_make_service());
    let port = server.local_addr().port();

    (server, port)
}
