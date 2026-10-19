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

use std::sync::{Arc, Mutex, Weak};

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
use mz_ore::metrics::IntCounter;

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

/// Metrics for one pool. A pool created with a clone of these metrics replaces
/// the pool they described before.
#[derive(Clone, Debug)]
pub struct HttpPoolMetrics {
    requests: IntCounter,
    pool: Arc<Mutex<Weak<Pool>>>,
}

impl HttpPoolMetrics {
    /// `requests` counts the requests sent through the pool, including the
    /// SDK's retries.
    pub fn new(requests: IntCounter) -> Self {
        HttpPoolMetrics {
            requests,
            pool: Arc::default(),
        }
    }

    /// Metrics that no registry reports, for a pool that should not count
    /// toward any reported metrics.
    pub fn unreported() -> Self {
        HttpPoolMetrics::new(IntCounter::new("unreported", "unreported").expect("valid counter"))
    }

    /// The clients in the pool, whether or not any request uses them.
    pub fn clients(&self) -> usize {
        self.client_in_flight().len()
    }

    /// The clients in the pool with at least one request in flight.
    pub fn clients_in_use(&self) -> usize {
        self.client_in_flight()
            .into_iter()
            .filter(|&count| count > 0)
            .count()
    }

    /// The requests in flight through the pool.
    pub fn in_flight(&self) -> usize {
        self.client_in_flight().into_iter().sum()
    }

    /// The fewest and the most requests in flight on any one client in the
    /// pool. `(0, 0)` if there is no pool.
    pub fn client_in_flight_range(&self) -> (usize, usize) {
        let counts = self.client_in_flight();
        let fewest = counts.iter().min().copied().unwrap_or(0);
        let most = counts.iter().max().copied().unwrap_or(0);
        (fewest, most)
    }

    /// The requests in flight on each client in the pool.
    fn client_in_flight(&self) -> Vec<usize> {
        let Some(pool) = self.pool.lock().expect("lock poisoned").upgrade() else {
            return Vec::new();
        };
        let members = pool.members.lock().expect("lock poisoned");
        members.iter().map(Member::in_flight).collect()
    }
}

#[derive(Debug)]
pub(crate) struct PooledHttpClient {
    pool: Arc<Pool>,
}

impl PooledHttpClient {
    pub(crate) fn new(new_client: fn() -> SharedHttpClient, metrics: HttpPoolMetrics) -> Self {
        PooledHttpClient {
            pool: Pool::new(new_client, metrics),
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
    metrics: HttpPoolMetrics,
    /// Holds at least [`MIN_CLIENTS`]. Members are never removed. An idle member's connections
    /// close on the SDK client's idle timeout, so it costs only memory.
    members: Mutex<Vec<Member>>,
}

impl Pool {
    fn new(new_client: fn() -> SharedHttpClient, metrics: HttpPoolMetrics) -> Arc<Self> {
        let pool = Arc::new(Pool {
            new_client,
            metrics,
            members: Mutex::new(
                (0..MIN_CLIENTS)
                    .map(|_| Member::new(new_client()))
                    .collect(),
            ),
        });
        *pool.metrics.pool.lock().expect("lock poisoned") = Arc::downgrade(&pool);
        pool
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
        self.pool.metrics.requests.inc();
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

/// Sends each request through `pooled` or `plain`, as `use_pool` says when the
/// request is sent.
#[derive(Debug)]
pub(crate) struct SwitchedHttpClient {
    plain: SharedHttpClient,
    pooled: SharedHttpClient,
    use_pool: UsePool,
}

impl SwitchedHttpClient {
    pub(crate) fn new(
        plain: SharedHttpClient,
        pooled: SharedHttpClient,
        use_pool: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        SwitchedHttpClient {
            plain,
            pooled,
            use_pool: UsePool(Arc::new(use_pool)),
        }
    }
}

#[derive(Clone)]
struct UsePool(Arc<dyn Fn() -> bool + Send + Sync>);

impl std::fmt::Debug for UsePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UsePool")
    }
}

impl HttpClient for SwitchedHttpClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(SwitchedConnector {
            plain: self.plain.http_connector(settings, components),
            pooled: self.pooled.http_connector(settings, components),
            use_pool: self.use_pool.clone(),
        })
    }

    fn validate_base_client_config(
        &self,
        runtime_components: &RuntimeComponentsBuilder,
        cfg: &ConfigBag,
    ) -> Result<(), BoxError> {
        self.plain
            .validate_base_client_config(runtime_components, cfg)?;
        self.pooled
            .validate_base_client_config(runtime_components, cfg)
    }

    fn validate_final_config(
        &self,
        runtime_components: &RuntimeComponents,
        cfg: &ConfigBag,
    ) -> Result<(), BoxError> {
        self.plain.validate_final_config(runtime_components, cfg)?;
        self.pooled.validate_final_config(runtime_components, cfg)
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        self.plain.connector_metadata()
    }
}

/// Asks `use_pool` per request, so a change applies however long the SDK keeps
/// a connector.
#[derive(Debug)]
struct SwitchedConnector {
    plain: SharedHttpConnector,
    pooled: SharedHttpConnector,
    use_pool: UsePool,
}

impl HttpConnector for SwitchedConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        if (self.use_pool.0)() {
            self.pooled.call(request)
        } else {
            self.plain.call(request)
        }
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

    fn pool() -> Arc<Pool> {
        Pool::new(
            || SharedHttpClient::new(UnusedClient),
            HttpPoolMetrics::unreported(),
        )
    }

    fn in_flight(pool: &Pool) -> Vec<usize> {
        let members = pool.members.lock().expect("lock poisoned");
        members.iter().map(Member::in_flight).collect()
    }

    #[mz_ore::test]
    fn grows_when_every_member_is_full() {
        let pool = pool();
        let metrics = pool.metrics.clone();
        assert_eq!(in_flight(&pool), [0; MIN_CLIENTS]);
        assert_eq!(metrics.clients(), MIN_CLIENTS);
        let full = [MAX_IN_FLIGHT_PER_CLIENT; MIN_CLIENTS];
        let mut held: Vec<_> = (0..MIN_CLIENTS * MAX_IN_FLIGHT_PER_CLIENT)
            .map(|_| pool.pick())
            .collect();
        assert_eq!(in_flight(&pool), full);
        held.push(pool.pick());
        assert_eq!(in_flight(&pool), [&full[..], &[1]].concat());
        held.push(pool.pick());
        assert_eq!(in_flight(&pool), [&full[..], &[2]].concat());
        assert_eq!(metrics.clients(), MIN_CLIENTS + 1);
        drop(held);
        assert_eq!(in_flight(&pool), [0; MIN_CLIENTS + 1]);
        drop(pool);
        assert_eq!(metrics.clients(), 0, "a dropped pool removes its clients");
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

    #[mz_ore::test]
    fn metrics_follow_newest_pool() {
        let old = pool();
        let metrics = old.metrics.clone();
        let _old_held = old.pick();
        assert_eq!(metrics.in_flight(), 1);
        let new = Pool::new(|| SharedHttpClient::new(UnusedClient), metrics.clone());
        assert_eq!(
            metrics.in_flight(),
            0,
            "the newer pool replaces the older one"
        );
        let mut held: Vec<_> = (0..3).map(|_| new.pick()).collect();
        assert_eq!(metrics.clients_in_use(), 3);
        held.extend((3..MIN_CLIENTS + 1).map(|_| new.pick()));
        assert_eq!(metrics.client_in_flight_range(), (1, 2));
        assert_eq!(metrics.clients_in_use(), MIN_CLIENTS);
        drop(held);
        assert_eq!(metrics.client_in_flight_range(), (0, 0));
        assert_eq!(metrics.clients_in_use(), 0);
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

    #[mz_ore::test(tokio::test)]
    async fn request_counts_until_body_dropped() {
        let pool = Pool::new(
            || SharedHttpClient::new(FakeClient { fail: false }),
            HttpPoolMetrics::unreported(),
        );
        let response = connector(&pool).call(HttpRequest::empty());
        assert_eq!(pool.metrics.in_flight(), 1);
        assert_eq!(pool.metrics.requests.get(), 1);
        let mut response = response.await.expect("fake response");
        assert_eq!(pool.metrics.in_flight(), 1);
        assert_eq!(
            response.body().content_length(),
            Some(5),
            "wrapping the body keeps its size hint",
        );
        let body = response.take_body();
        assert_eq!(pool.metrics.in_flight(), 1);
        let bytes = body.collect().await.expect("fake body").to_bytes();
        assert_eq!(bytes, "hello");
        assert_eq!(pool.metrics.in_flight(), 0);
    }

    #[mz_ore::test(tokio::test)]
    async fn switch_applies_to_existing_connector() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let use_pool = Arc::new(AtomicBool::new(false));
        let client = SwitchedHttpClient::new(
            SharedHttpClient::new(FakeClient { fail: true }),
            SharedHttpClient::new(FakeClient { fail: false }),
            {
                let use_pool = Arc::clone(&use_pool);
                move || use_pool.load(Ordering::Relaxed)
            },
        );
        let connector = client.http_connector(
            &HttpConnectorSettings::builder().build(),
            &RuntimeComponentsBuilder::for_tests()
                .build()
                .expect("valid components"),
        );
        connector
            .call(HttpRequest::empty())
            .await
            .expect_err("plain client");
        use_pool.store(true, Ordering::Relaxed);
        connector
            .call(HttpRequest::empty())
            .await
            .expect("pooled client");
    }

    #[mz_ore::test(tokio::test)]
    async fn failed_or_abandoned_request_stops_counting() {
        let pool = Pool::new(
            || SharedHttpClient::new(FakeClient { fail: true }),
            HttpPoolMetrics::unreported(),
        );
        let response = connector(&pool).call(HttpRequest::empty());
        assert_eq!(pool.metrics.in_flight(), 1);
        response.await.expect_err("fake failure");
        assert_eq!(pool.metrics.in_flight(), 0);

        let response = connector(&pool).call(HttpRequest::empty());
        assert_eq!(pool.metrics.in_flight(), 1);
        drop(response);
        assert_eq!(pool.metrics.in_flight(), 0);
    }
}
