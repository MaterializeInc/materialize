// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An AWS SDK HTTP client that spreads requests over a growing set of the
//! SDK's own clients.
//!
//! Over HTTP/2, one SDK client sends every request to a host over a single
//! connection. Each client has its own connections, so spreading requests over
//! several clients spreads them over several connections.

use std::sync::{Arc, Mutex};

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::connector_metadata::ConnectorMetadata;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::runtime_components::{
    RuntimeComponents, RuntimeComponentsBuilder,
};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::config_bag::ConfigBag;
use http_body_util::BodyExt;

/// The pool starts with this many clients.
///
/// In our benchmarks of GCS reads, one connection carried 300 to 400 MiB/s and
/// total reads topped out around 4 GiB/s. Pools of 16 to 64 clients reached
/// that limit, but a pool of 10 did not.
const MIN_CLIENTS: usize = 32;

/// A request goes to a new client once every client has this many requests in
/// flight.
///
/// Matches the `MAX_CONCURRENT_STREAMS: 100` that GCS advertised in its HTTP/2
/// settings, as observed in October 2026 with
/// `curl -v --trace-config http/2 --http2 https://storage.googleapis.com/`.
const MAX_IN_FLIGHT_PER_CLIENT: usize = 100;

#[derive(Debug)]
pub(crate) struct PooledHttpClient {
    pool: Arc<Pool>,
}

impl PooledHttpClient {
    pub(crate) fn new(new_client: fn() -> SharedHttpClient) -> Self {
        PooledHttpClient {
            pool: Arc::new(Pool::new(new_client)),
        }
    }
}

impl HttpClient for PooledHttpClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(PooledConnector {
            pool: Arc::clone(&self.pool),
            settings: settings.clone(),
            components: components.clone(),
        })
    }

    fn validate_base_client_config(
        &self,
        runtime_components: &RuntimeComponentsBuilder,
        cfg: &ConfigBag,
    ) -> Result<(), BoxError> {
        self.pool
            .first()
            .validate_base_client_config(runtime_components, cfg)
    }

    fn validate_final_config(
        &self,
        runtime_components: &RuntimeComponents,
        cfg: &ConfigBag,
    ) -> Result<(), BoxError> {
        self.pool
            .first()
            .validate_final_config(runtime_components, cfg)
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        self.pool.first().connector_metadata()
    }
}

#[derive(Debug)]
struct Pool {
    new_client: fn() -> SharedHttpClient,
    /// Holds at least [`MIN_CLIENTS`]. Members are never removed. An idle member's connections
    /// close on the SDK client's idle timeout, so it costs only memory.
    members: Mutex<Vec<Member>>,
}

impl Pool {
    fn new(new_client: fn() -> SharedHttpClient) -> Self {
        Pool {
            new_client,
            members: Mutex::new(
                (0..MIN_CLIENTS)
                    .map(|_| Member::new(new_client()))
                    .collect(),
            ),
        }
    }

    fn first(&self) -> SharedHttpClient {
        let members = self.members.lock().expect("lock poisoned");
        members[0].client.clone()
    }

    /// Picks the member with the fewest requests in flight, adding a member
    /// if every one is at [`MAX_IN_FLIGHT_PER_CLIENT`]. The request counts
    /// against the member until the returned clone is dropped.
    fn pick(&self) -> Member {
        let mut members = self.members.lock().expect("lock poisoned");
        let least = members
            .iter()
            .min_by_key(|member| member.in_flight())
            .expect("never empty");
        // Cloned while holding the lock, so concurrent picks see the count.
        if least.in_flight() < MAX_IN_FLIGHT_PER_CLIENT {
            least.clone()
        } else {
            let member = Member::new((self.new_client)());
            members.push(member.clone());
            member
        }
    }
}

#[derive(Clone, Debug)]
struct Member {
    client: SharedHttpClient,
    /// Each request in flight holds a clone.
    token: Arc<()>,
}

impl Member {
    fn new(client: SharedHttpClient) -> Self {
        Member {
            client,
            token: Arc::new(()),
        }
    }

    fn in_flight(&self) -> usize {
        Arc::strong_count(&self.token) - 1
    }
}

/// Picks a member per request, so the in-flight counts stay right however long
/// the SDK keeps a connector.
#[derive(Debug)]
struct PooledConnector {
    pool: Arc<Pool>,
    settings: HttpConnectorSettings,
    components: RuntimeComponents,
}

impl HttpConnector for PooledConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let Member { client, token } = self.pool.pick();
        let response = client
            .http_connector(&self.settings, &self.components)
            .call(request);
        HttpConnectorFuture::new(async move {
            let mut response = response.await?;
            // A response holds its HTTP/2 stream open until its body is read
            // or dropped, so the request stays in flight until then.
            //
            // NOTE: The token rides in `map_err` because `MapErr` passes through the body's
            // size hint and `MapFrame` drops it.
            let body = response.take_body().map_err(move |err| {
                let _token = &token;
                err
            });
            *response.body_mut() = SdkBody::from_body_1_x(body);
            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
    use aws_smithy_runtime_api::client::result::ConnectorError;
    use aws_smithy_runtime_api::http::StatusCode;

    use super::*;

    #[derive(Debug)]
    struct UnusedClient;

    impl HttpClient for UnusedClient {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            unimplemented!()
        }
    }

    fn pool() -> Pool {
        Pool::new(|| SharedHttpClient::new(UnusedClient))
    }

    fn in_flight(pool: &Pool) -> Vec<usize> {
        let members = pool.members.lock().expect("lock poisoned");
        members.iter().map(Member::in_flight).collect()
    }

    #[mz_ore::test]
    fn grows_when_every_member_is_full() {
        let pool = pool();
        assert_eq!(in_flight(&pool), [0; MIN_CLIENTS]);
        let full = [MAX_IN_FLIGHT_PER_CLIENT; MIN_CLIENTS];
        let mut held: Vec<_> = (0..MIN_CLIENTS * MAX_IN_FLIGHT_PER_CLIENT)
            .map(|_| pool.pick())
            .collect();
        assert_eq!(in_flight(&pool), full);
        held.push(pool.pick());
        assert_eq!(in_flight(&pool), [&full[..], &[1]].concat());
        held.push(pool.pick());
        assert_eq!(in_flight(&pool), [&full[..], &[2]].concat());
        drop(held);
        assert_eq!(in_flight(&pool), [0; MIN_CLIENTS + 1]);
    }

    #[mz_ore::test]
    fn picks_least_loaded_member() {
        let pool = pool();
        let mut held: Vec<_> = (0..MIN_CLIENTS).map(|_| pool.pick()).collect();
        assert_eq!(in_flight(&pool), [1; MIN_CLIENTS]);
        drop(held.remove(3));
        held.push(pool.pick());
        assert_eq!(in_flight(&pool), [1; MIN_CLIENTS]);
        held.push(pool.pick());
        assert_eq!(in_flight(&pool), [&[2][..], &[1; MIN_CLIENTS - 1]].concat());
    }

    /// Answers every request with a `hello` body, or with an error if `fail`.
    #[derive(Debug)]
    struct FakeClient {
        fail: bool,
    }

    impl HttpClient for FakeClient {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(FakeConnector { fail: self.fail })
        }
    }

    #[derive(Debug)]
    struct FakeConnector {
        fail: bool,
    }

    impl HttpConnector for FakeConnector {
        fn call(&self, _: HttpRequest) -> HttpConnectorFuture {
            if self.fail {
                HttpConnectorFuture::ready(Err(ConnectorError::other("fake".into(), None)))
            } else {
                let status = StatusCode::try_from(200).expect("valid status");
                HttpConnectorFuture::ready(Ok(HttpResponse::new(status, SdkBody::from("hello"))))
            }
        }
    }

    fn connector(pool: &Arc<Pool>) -> PooledConnector {
        PooledConnector {
            pool: Arc::clone(pool),
            settings: HttpConnectorSettings::builder().build(),
            components: RuntimeComponentsBuilder::for_tests()
                .build()
                .expect("valid components"),
        }
    }

    fn total_in_flight(pool: &Pool) -> usize {
        in_flight(pool).iter().sum()
    }

    #[mz_ore::test(tokio::test)]
    async fn request_counts_until_body_dropped() {
        let pool = Arc::new(Pool::new(|| {
            SharedHttpClient::new(FakeClient { fail: false })
        }));
        let response = connector(&pool).call(HttpRequest::empty());
        assert_eq!(total_in_flight(&pool), 1);
        let mut response = response.await.expect("fake response");
        assert_eq!(total_in_flight(&pool), 1);
        assert_eq!(
            response.body().content_length(),
            Some(5),
            "wrapping the body keeps its size hint",
        );
        let body = response.take_body();
        assert_eq!(total_in_flight(&pool), 1);
        let bytes = body.collect().await.expect("fake body").to_bytes();
        assert_eq!(bytes, "hello");
        assert_eq!(total_in_flight(&pool), 0);
    }

    #[mz_ore::test(tokio::test)]
    async fn failed_or_abandoned_request_stops_counting() {
        let pool = Arc::new(Pool::new(|| {
            SharedHttpClient::new(FakeClient { fail: true })
        }));
        let response = connector(&pool).call(HttpRequest::empty());
        assert_eq!(total_in_flight(&pool), 1);
        response.await.expect_err("fake failure");
        assert_eq!(total_in_flight(&pool), 0);

        let response = connector(&pool).call(HttpRequest::empty());
        assert_eq!(total_in_flight(&pool), 1);
        drop(response);
        assert_eq!(total_in_flight(&pool), 0);
    }
}
