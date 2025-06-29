// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the Cluster Transport Protocol.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use async_trait::async_trait;
use mz_ore::assert_none;
use mz_ore::retry::Retry;
use mz_service::client::GenericClient;
use mz_service::transport::{self, ChannelHandler, NoopMetrics, Payload};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use semver::Version;
use tokio::sync::{mpsc, oneshot};
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::time::FormatTime;

const VERSION: Version = Version::new(1, 2, 3);
const TIMEOUT: Duration = Duration::from_secs(5);

/// Common setup for turmoil tests.
fn setup() -> turmoil::Sim<'static> {
    configure_tracing_for_turmoil();

    let seed = std::env::var("SEED")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or_else(rand::random);

    info!("initializing rng with seed {seed}");
    let rng = SmallRng::seed_from_u64(seed);

    turmoil::Builder::new()
        .enable_random_order()
        .build_with_rng(Box::new(rng.clone()))
}

/// Helper for connecting to CTP server that retries until it succeeds.
async fn connect_ctp<Out: Payload, In: Payload>(
    address: &str,
    version: Version,
    timeout: Duration,
    metrics: impl transport::Metrics<Out, In>,
) -> transport::Client<Out, In> {
    Retry::default()
        .retry_async(|_| {
            transport::Client::connect(address, version.clone(), timeout, timeout, metrics.clone())
        })
        .await
        .expect("retries forever")
}

#[test] // allow(test-attribute)
fn test_bidirectional_communication() {
    let mut sim = setup();

    sim.host("server", move || async {
        let (in_tx, mut in_rx) = mpsc::unbounded_channel();
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let handler = ChannelHandler::new(in_tx, out_rx);
        let handler = Arc::new(Mutex::new(Some(handler)));

        mz_ore::task::spawn(
            || "serve",
            transport::serve(
                "turmoil:0.0.0.0:7777".parse().unwrap(),
                VERSION,
                Some("server".into()),
                TIMEOUT,
                move || handler.lock().unwrap().take().unwrap(),
                NoopMetrics,
            ),
        );

        out_tx.send("a".to_string())?;
        out_tx.send("b".to_string())?;
        out_tx.send("c".to_string())?;

        assert_eq!(in_rx.recv().await, Some(1));
        assert_eq!(in_rx.recv().await, Some(2));
        assert_eq!(in_rx.recv().await, Some(3));

        out_tx.send("done".to_string())?;

        Ok(())
    });

    sim.client("client", async move {
        let mut client = connect_ctp("turmoil:server:7777", VERSION, TIMEOUT, NoopMetrics).await;

        client.send(1).await?;
        client.send(2).await?;
        client.send(3).await?;

        assert_eq!(client.recv().await?, Some("a".to_string()));
        assert_eq!(client.recv().await?, Some("b".to_string()));
        assert_eq!(client.recv().await?, Some("c".to_string()));

        // Wait for the server to finish.
        assert_eq!(client.recv().await?, Some("done".to_string()));

        Ok(())
    });

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
fn test_server_error() {
    let mut sim = setup();

    sim.host("server", move || async {
        let (in_tx, mut in_rx) = mpsc::unbounded_channel();
        let (_out_tx, out_rx) = mpsc::unbounded_channel::<()>();
        let handler = ChannelHandler::new(in_tx, out_rx);
        let handler = Arc::new(Mutex::new(Some(handler)));

        mz_ore::task::spawn(
            || "serve",
            transport::serve(
                "turmoil:0.0.0.0:7777".parse().unwrap(),
                VERSION,
                Some("server".into()),
                TIMEOUT,
                move || handler.lock().unwrap().take().unwrap(),
                NoopMetrics,
            ),
        );

        // Wait for the client to connect, then shut down.
        assert_eq!(in_rx.recv().await, Some(1));

        Ok(())
    });

    sim.client("client", async move {
        let mut client =
            connect_ctp::<i32, ()>("turmoil:server:7777", VERSION, TIMEOUT, NoopMetrics).await;

        client.send(1).await?;

        // Server has disconnected.
        assert_eq!(
            client.recv().await.map_err(|e| e.to_string()),
            Err("unexpected end of file".into()),
        );
        // Trying to receive on a failed connection yields more errors.
        assert_eq!(
            client.recv().await.map_err(|e| e.to_string()),
            Err("connection closed".into()),
        );

        Ok(())
    });

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
fn test_handshake_version_mismatch() {
    const SERVER_VERSION: Version = Version::new(1, 2, 3);
    const CLIENT_VERSION: Version = Version::new(1, 2, 4);

    let mut sim = setup();

    sim.host("server", move || async {
        let (in_tx, mut in_rx) = mpsc::unbounded_channel::<()>();
        let (_out_tx, out_rx) = mpsc::unbounded_channel::<()>();
        let handler = ChannelHandler::new(in_tx, out_rx);
        let handler = Arc::new(Mutex::new(Some(handler)));

        mz_ore::task::spawn(
            || "serve",
            transport::serve(
                "turmoil:0.0.0.0:7777".parse().unwrap(),
                SERVER_VERSION,
                Some("server".into()),
                TIMEOUT,
                move || handler.lock().unwrap().take().unwrap(),
                NoopMetrics,
            ),
        );

        // Handshake failed.
        assert_none!(in_rx.recv().await);

        Ok(())
    });

    sim.client("client", async move {
        let mut client =
            connect_ctp::<i32, ()>("turmoil:server:7777", CLIENT_VERSION, TIMEOUT, NoopMetrics)
                .await;

        // Handshake failed.
        assert_eq!(
            client.recv().await.map_err(|e| e.to_string()),
            Err("version mismatch: 1.2.3 != 1.2.4".into())
        );

        Ok(())
    });

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
fn test_handshake_fqdn_mismatch() {
    let mut sim = setup();

    sim.host("server", move || async {
        let (in_tx, mut in_rx) = mpsc::unbounded_channel::<()>();
        let (_out_tx, out_rx) = mpsc::unbounded_channel::<()>();
        let handler = ChannelHandler::new(in_tx, out_rx);
        let handler = Arc::new(Mutex::new(Some(handler)));

        mz_ore::task::spawn(
            || "serve",
            transport::serve(
                "turmoil:0.0.0.0:7777".parse().unwrap(),
                VERSION,
                Some("wrong.server".into()),
                TIMEOUT,
                move || handler.lock().unwrap().take().unwrap(),
                NoopMetrics,
            ),
        );

        // Client has disconnected.
        assert_none!(in_rx.recv().await);

        Ok(())
    });

    sim.client("client", async move {
        let mut client =
            connect_ctp::<i32, ()>("turmoil:server:7777", VERSION, TIMEOUT, NoopMetrics).await;

        // Server has disconnected.
        assert_eq!(
            client.recv().await.map_err(|e| e.to_string()),
            Err("server FQDN mismatch: wrong.server != server".into()),
        );

        Ok(())
    });

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
fn test_keepalive_timeout() {
    let mut sim = setup();

    sim.host("server", move || async {
        let (in_tx, _in_rx) = mpsc::unbounded_channel::<()>();
        let (out_tx, out_rx) = mpsc::unbounded_channel::<()>();
        let handler = ChannelHandler::new(in_tx, out_rx);
        let handler = Arc::new(Mutex::new(Some(handler)));

        mz_ore::task::spawn(
            || "serve",
            transport::serve(
                "turmoil:0.0.0.0:7777".parse().unwrap(),
                VERSION,
                Some("server".into()),
                TIMEOUT,
                move || handler.lock().unwrap().take().unwrap(),
                NoopMetrics,
            ),
        );

        out_tx.send(())?;

        // Wait forever to keep the connection open.
        futures::future::pending().await
    });

    let (ready_tx, mut ready_rx) = oneshot::channel();

    sim.client("client", async move {
        let mut client =
            connect_ctp::<i32, ()>("turmoil:server:7777", VERSION, TIMEOUT, NoopMetrics).await;

        client.recv().await?;
        ready_tx.send(()).unwrap();

        // Connection timed out.
        assert_eq!(
            client.recv().await.map_err(|e| e.to_string()),
            Err("deadline has elapsed".into()),
        );

        Ok(())
    });

    // Wait until the client is connected, then introduce a network partition.
    while ready_rx.try_recv().is_err() {
        sim.step().unwrap();
    }
    sim.partition("client", "server");

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
fn test_connection_cancelation() {
    let mut sim = setup();

    // Use a high connection timeout to avoid that the connection is severed by the timeout, which
    // isn't what we want to test here.
    const TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24);

    sim.host("server", move || async {
        transport::serve(
            "turmoil:0.0.0.0:7777".parse().unwrap(),
            VERSION,
            Some("server".into()),
            TIMEOUT,
            OneOutputHandler::new,
            NoopMetrics,
        )
        .await?;

        Ok(())
    });

    let (ready_tx, mut ready_rx) = oneshot::channel();

    sim.client("client1", async move {
        let mut client =
            connect_ctp::<i32, ()>("turmoil:server:7777", VERSION, TIMEOUT, NoopMetrics).await;

        client.recv().await?;
        ready_tx.send(()).unwrap();

        // Connection canceled.
        assert_eq!(
            client.recv().await.map_err(|e| e.to_string()),
            Err("unexpected end of file".into()),
        );

        Ok(())
    });

    // Wait until the first client is connected, then spawn a second one to force the first
    // connection to be canceled.
    while ready_rx.try_recv().is_err() {
        sim.step().unwrap();
    }

    sim.client("client2", async move {
        let mut client =
            connect_ctp::<i32, ()>("turmoil:server:7777", VERSION, TIMEOUT, NoopMetrics).await;

        client.recv().await?;

        Ok(())
    });

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
fn test_metrics() {
    #[derive(Clone, Default)]
    struct Metrics {
        payload_sent: Arc<AtomicU64>,
        payload_received: Arc<AtomicU64>,
        overhead_sent: Arc<AtomicU64>,
        overhead_received: Arc<AtomicU64>,
    }

    impl transport::Metrics<String, String> for Metrics {
        fn message_sent(&mut self, bytes: u64, payload: Option<&String>) {
            match payload {
                Some(_) => self.payload_sent.fetch_add(bytes, Ordering::SeqCst),
                None => self.overhead_sent.fetch_add(bytes, Ordering::SeqCst),
            };
        }

        fn message_received(&mut self, bytes: u64, payload: Option<&String>) {
            match payload {
                Some(_) => self.payload_received.fetch_add(bytes, Ordering::SeqCst),
                None => self.overhead_received.fetch_add(bytes, Ordering::SeqCst),
            };
        }
    }

    let mut sim = setup();

    sim.host("server", move || async {
        let (in_tx, mut in_rx) = mpsc::unbounded_channel();
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let handler = ChannelHandler::new(in_tx, out_rx);
        let handler = Arc::new(Mutex::new(Some(handler)));

        let metrics = Metrics::default();

        mz_ore::task::spawn(
            || "serve",
            transport::serve(
                "turmoil:0.0.0.0:7777".parse().unwrap(),
                VERSION,
                Some("server".into()),
                TIMEOUT,
                move || handler.lock().unwrap().take().unwrap(),
                metrics.clone(),
            ),
        );

        out_tx.send("long message from server".into())?;
        assert_eq!(in_rx.recv().await, Some("short".into()));

        assert_eq!(metrics.payload_sent.load(Ordering::SeqCst), 41);
        assert_eq!(metrics.payload_received.load(Ordering::SeqCst), 22);
        assert_eq!(metrics.overhead_sent.load(Ordering::SeqCst), 32);
        assert_eq!(metrics.overhead_received.load(Ordering::SeqCst), 32);

        Ok(())
    });

    sim.client("client", async move {
        let metrics = Metrics::default();

        let mut client = connect_ctp("turmoil:server:7777", VERSION, TIMEOUT, metrics.clone()).await;

        client.send("short".into()).await?;
        assert_eq!(
            client.recv().await?,
            Some("long message from server".into()),
        );

        assert_eq!(metrics.payload_sent.load(Ordering::SeqCst), 22);
        assert_eq!(metrics.payload_received.load(Ordering::SeqCst), 41);
        assert_eq!(metrics.overhead_sent.load(Ordering::SeqCst), 32);
        assert_eq!(metrics.overhead_received.load(Ordering::SeqCst), 32);

        Ok(())
    });

    sim.run().unwrap();
}

/// A connection handler that produces a single outbound message and then becomes silent.
#[derive(Debug)]
struct OneOutputHandler {
    done: bool,
}

impl OneOutputHandler {
    fn new() -> Self {
        Self { done: false }
    }
}

#[async_trait]
impl GenericClient<(), ()> for OneOutputHandler {
    async fn send(&mut self, _cmd: ()) -> anyhow::Result<()> {
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<Option<()>> {
        if self.done {
            futures::future::pending().await
        } else {
            self.done = true;
            Ok(Some(()))
        }
    }
}

/// Configure tracing for turmoil tests.
///
/// Log events are written to stdout and include the logical time of the simulation.
fn configure_tracing_for_turmoil() {
    #[derive(Clone)]
    struct SimElapsedTime;

    impl FormatTime for SimElapsedTime {
        fn format_time(
            &self,
            w: &mut tracing_subscriber::fmt::format::Writer<'_>,
        ) -> std::fmt::Result {
            tracing_subscriber::fmt::time().format_time(w)?;
            if let Some(sim_elapsed) = turmoil::sim_elapsed() {
                write!(w, " [{:?}]", sim_elapsed)?;
            }
            Ok(())
        }
    }

    static INIT_TRACING: Once = Once::new();
    INIT_TRACING.call_once(|| {
        let env_filter = tracing_subscriber::EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy();
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(env_filter)
            .with_timer(SimElapsedTime)
            .finish();

        tracing::subscriber::set_global_default(subscriber).unwrap();
    });
}
