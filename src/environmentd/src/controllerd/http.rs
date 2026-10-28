// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Plain HTTP control API. Access control belongs outside this server.
//! This is not a SQL proxy. The command owner validates and commits changes
//! and supplies response values and errors safe to expose to callers.

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use axum::{Json, Router};
use mz_repr::AdapterId;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub(super) enum Action {
    ListAdapters,
    CreateAdapter { name: String, size: String },
    DropAdapter { id: AdapterId },
    Inventory,
    Collections(super::collections::Filters),
}

/// Once received, a command must be executed even if its response receiver
/// closes. An HTTP caller disconnecting does not cancel a catalog mutation.
pub(super) struct Command {
    pub action: Action,
    pub response: oneshot::Sender<Result<Value, ApiError>>,
}

#[derive(Debug)]
pub(super) struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "command owner unavailable".into(),
        }
    }

    fn bad_request(message: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        pretty_json(self.status, json!({ "error": self.message }))
    }
}

fn pretty_json(status: StatusCode, value: Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string_pretty(&value).expect("JSON values serialize"),
    )
        .into_response()
}

pub(super) async fn serve(
    listener: tokio::net::TcpListener,
    sender: mpsc::Sender<Command>,
) -> std::io::Result<()> {
    axum::serve(listener, router(sender)).await
}

fn router(sender: mpsc::Sender<Command>) -> Router {
    Router::new()
        .route("/api/adapters", get(list_adapters).post(create_adapter))
        .route("/api/adapters/{id}", delete(drop_adapter))
        .route("/api/inventory", get(inventory))
        .route("/api/collections", get(collections))
        .fallback(|| async {
            ApiError {
                status: StatusCode::NOT_FOUND,
                message: "route not found".into(),
            }
        })
        .method_not_allowed_fallback(|| async {
            ApiError {
                status: StatusCode::METHOD_NOT_ALLOWED,
                message: "method not allowed".into(),
            }
        })
        .with_state(sender)
}

async fn dispatch(sender: mpsc::Sender<Command>, action: Action) -> Result<Response, ApiError> {
    let (response, receiver) = oneshot::channel();
    sender
        .send(Command { action, response })
        .await
        .map_err(|_| ApiError::unavailable())?;
    // The channel now owns the command. Dropping this request only drops its
    // reply receiver, not the queued command or the owner's execution.
    let value = receiver.await.map_err(|_| ApiError::unavailable())??;
    Ok(pretty_json(StatusCode::OK, value))
}

async fn list_adapters(State(sender): State<mpsc::Sender<Command>>) -> Result<Response, ApiError> {
    dispatch(sender, Action::ListAdapters).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateAdapter {
    name: String,
    size: String,
}

async fn create_adapter(
    State(sender): State<mpsc::Sender<Command>>,
    body: Result<Json<CreateAdapter>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(CreateAdapter { name, size }) =
        body.map_err(|_| ApiError::bad_request("expected JSON with string fields name and size"))?;
    dispatch(sender, Action::CreateAdapter { name, size }).await
}

async fn drop_adapter(
    State(sender): State<mpsc::Sender<Command>>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let Path(id) = path.map_err(|_| ApiError::bad_request("invalid adapter ID"))?;
    let id = id
        .parse()
        .map_err(|_| ApiError::bad_request("invalid adapter ID"))?;
    dispatch(sender, Action::DropAdapter { id }).await
}

async fn inventory(State(sender): State<mpsc::Sender<Command>>) -> Result<Response, ApiError> {
    dispatch(sender, Action::Inventory).await
}

async fn collections(
    State(sender): State<mpsc::Sender<Command>>,
    query: Result<Query<super::collections::Patterns>, QueryRejection>,
) -> Result<Response, ApiError> {
    let Query(patterns) = query
        .map_err(|_| ApiError::bad_request("expected optional id and name query parameters"))?;
    let filters = patterns
        .compile()
        .map_err(|error| ApiError::bad_request(&error.to_string()))?;
    dispatch(sender, Action::Collections(filters)).await
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;

    fn request(method: &str, uri: &str, body: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_owned()))
            .unwrap()
    }

    async fn assert_json(response: Response, status: StatusCode, value: Value) {
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            body.as_ref(),
            serde_json::to_string_pretty(&value).unwrap().as_bytes()
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn routes_forward_without_auth_and_return_pretty_json() {
        let (sender, mut receiver) = mpsc::channel::<Command>(1);
        let app = router(sender);
        for (method, uri, body) in [
            ("GET", "/api/adapters", ""),
            (
                "POST",
                "/api/adapters",
                r#"{"name":"example","size":"small"}"#,
            ),
            ("DELETE", "/api/adapters/a1", ""),
            ("GET", "/api/inventory", ""),
            (
                "GET",
                "/api/collections?id=s%25&name=%25mz%5C_catalog.%25",
                "",
            ),
        ] {
            let reply = json!({"result": ["ok"]});
            let owner = async {
                let command = receiver.recv().await.unwrap();
                match (method, uri, command.action) {
                    ("GET", "/api/adapters", Action::ListAdapters)
                    | ("GET", "/api/inventory", Action::Inventory)
                    | ("GET", _, Action::Collections(_)) => (),
                    ("POST", _, Action::CreateAdapter { name, size }) => {
                        assert_eq!(name, "example");
                        assert_eq!(size, "small");
                    }
                    ("DELETE", _, Action::DropAdapter { id }) => assert_eq!(id.to_string(), "a1"),
                    action => panic!("unexpected action: {action:?}"),
                }
                command.response.send(Ok(reply.clone())).unwrap();
            };
            let (response, ()) =
                tokio::join!(app.clone().oneshot(request(method, uri, body)), owner);
            assert_json(response.unwrap(), StatusCode::OK, reply).await;
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn rejects_invalid_input_without_dispatch() {
        let (sender, mut receiver) = mpsc::channel(1);
        let app = router(sender);
        for (method, uri, body) in [
            ("POST", "/api/adapters", "{"),
            ("POST", "/api/adapters", r#"{"name":"x"}"#),
            ("POST", "/api/adapters", r#"{"name":1,"size":"s"}"#),
            (
                "POST",
                "/api/adapters",
                r#"{"name":"x","size":"s","extra":true}"#,
            ),
            ("DELETE", "/api/adapters/u1", ""),
            ("DELETE", "/api/adapters/%FF", ""),
            ("GET", "/api/collections?id=bad%5C", ""),
            ("GET", "/api/collections?unknown=pattern", ""),
            ("GET", "/api/collections?id=a&id=b", ""),
        ] {
            let response = app
                .clone()
                .oneshot(request(method, uri, body))
                .await
                .unwrap();
            let message = if method == "POST" {
                "expected JSON with string fields name and size"
            } else {
                "invalid adapter ID"
            };
            assert_json(response, StatusCode::BAD_REQUEST, json!({"error": message})).await;
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
        for (method, uri, status) in [
            ("POST", "/api/sql", StatusCode::NOT_FOUND),
            ("POST", "/api/inventory", StatusCode::METHOD_NOT_ALLOWED),
            ("GET", "/api/adapters/a1", StatusCode::METHOD_NOT_ALLOWED),
        ] {
            let response = app.clone().oneshot(request(method, uri, "")).await.unwrap();
            assert_eq!(response.status(), status);
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn owner_closure_and_redacted_errors() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let response = router(sender)
            .oneshot(request("GET", "/api/adapters", ""))
            .await
            .unwrap();
        assert_json(
            response,
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "command owner unavailable"}),
        )
        .await;

        for error in [
            None,
            Some(ApiError {
                status: StatusCode::CONFLICT,
                message: "adapter already exists".into(),
            }),
        ] {
            let (sender, mut receiver) = mpsc::channel::<Command>(1);
            let (status, message) = match &error {
                Some(error) => (error.status, error.message.clone()),
                None => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "command owner unavailable".into(),
                ),
            };
            let owner = async {
                let command = receiver.recv().await.unwrap();
                if let Some(error) = error {
                    command.response.send(Err(error)).unwrap();
                }
            };
            let (response, ()) = tokio::join!(
                router(sender).oneshot(request("GET", "/api/inventory", "")),
                owner
            );
            assert_json(response.unwrap(), status, json!({"error": message})).await;
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn disconnect_does_not_remove_sent_command() {
        let (sender, mut receiver) = mpsc::channel(1);
        let task = mz_ore::task::spawn(
            || "controller-http-disconnect-test",
            router(sender).oneshot(request("DELETE", "/api/adapters/a1", "")),
        )
        .into_tokio_handle();
        let command = receiver.recv().await.unwrap();
        // No response has been supplied, so the request must still be waiting.
        assert!(!task.is_finished());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(matches!(
            command.action,
            Action::DropAdapter { id: AdapterId(1) }
        ));
        assert!(command.response.send(Ok(json!({}))).is_err());
    }
}
