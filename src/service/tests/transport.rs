// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the Cluster Transport Protocol.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures::future;
use mz_ore::assert_none;
use mz_ore::netio::Listener;
use mz_ore::retry::Retry;
use mz_service::client::GenericClient;
use mz_service::transport::{self, Message, NoopMetrics};
use semver::Version;
use tokio::io::AsyncWriteExt;
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

    turmoil::Builder::new()
        .enable_random_order()
        .rng_seed(seed)
        .build()
}

/// Helper for connecting to a CTP server that retries until it succeeds.
async fn connect_ctp<Out: Message, In: Message>(
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

/// Helper for connecting to a CTP server that retries until it encounters the expected error.
async fn connect_ctp_error<Out: Message, In: Message>(
    address: &str,
    version: Version,
    expected_error: &str,
) -> anyhow::Result<()> {
    Retry::default()
        .retry_async(async |_| {
            let result = transport::Client::<(), ()>::connect(
                address,
                version.clone(),
                TIMEOUT,
                TIMEOUT,
                NoopMetrics,
            )
            .await;
            let error = result.expect_err("connection must fail");
            if error.to_string() == expected_error {
                Ok(())
            } else {
                Err(error)
            }
        })
        .await
}

#[test] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
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
        future::pending().await
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
#[cfg_attr(miri, ignore)] // too slow
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
            Err("unexpected end of file".into()),
        );

        Ok(())
    });

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
fn test_handshake_magic_mismatch() {
    let mut sim = setup();

    sim.host("server", move || async {
        let listener = Listener::bind("turmoil:0.0.0.0:7777").await?;
        let (mut stream, _) = listener.accept().await?;
        stream.write_u64(0xbad).await?;
        Ok(())
    });

    sim.client("client", async move {
        connect_ctp_error::<(), ()>(
            "turmoil:server:7777",
            VERSION,
            "invalid protocol magic: 0xbad",
        )
        .await?;

        Ok(())
    });

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
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
        connect_ctp_error::<(), ()>(
            "turmoil:server:7777",
            CLIENT_VERSION,
            "version mismatch: 1.2.3 != 1.2.4",
        )
        .await?;

        Ok(())
    });

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
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
        connect_ctp_error::<(), ()>(
            "turmoil:server:7777",
            VERSION,
            "server FQDN mismatch: wrong.server != server",
        )
        .await?;

        Ok(())
    });

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
fn test_idle_timeout() {
    let mut sim = setup();

    sim.host("server", move || async {
        let (in_tx, _in_rx) = mpsc::unbounded_channel::<i32>();
        let (out_tx, out_rx) = mpsc::unbounded_channel::<i32>();
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

        out_tx.send(1)?;
        future::pending().await
    });

    let (ready_tx, mut ready_rx) = oneshot::channel();

    sim.client("client", async move {
        let mut client =
            connect_ctp::<i32, i32>("turmoil:server:7777", VERSION, TIMEOUT, NoopMetrics).await;

        client.recv().await?;
        ready_tx.send(1).unwrap();

        // Connection timed out.
        assert_eq!(
            client.recv().await.map_err(|e| e.to_string()),
            Err("timed out".into()),
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
#[cfg_attr(miri, ignore)] // too slow
fn test_keepalive() {
    let mut sim = setup();

    sim.host("server", move || async {
        let (in_tx, _in_rx) = mpsc::unbounded_channel::<i32>();
        let (out_tx, out_rx) = mpsc::unbounded_channel::<i32>();
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

        // Idle time that would time out the connections without keepalives.
        tokio::time::sleep(TIMEOUT + Duration::from_secs(1)).await;

        out_tx.send(1)?;
        future::pending().await
    });

    sim.client("client", async move {
        let mut client =
            connect_ctp::<i32, i32>("turmoil:server:7777", VERSION, TIMEOUT, NoopMetrics).await;

        client.recv().await?;

        Ok(())
    });

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
fn test_connection_cancelation() {
    let mut sim = setup();

    // Use a high connection timeout to avoid that the connection is severed by the timeout, which
    // isn't what we want to test here.
    const TIMEOUT: Duration = Duration::from_secs(60 * 60);

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
            connect_ctp::<i32, i32>("turmoil:server:7777", VERSION, TIMEOUT, NoopMetrics).await;

        client.recv().await?;
        ready_tx.send(1).unwrap();

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
            connect_ctp::<i32, i32>("turmoil:server:7777", VERSION, TIMEOUT, NoopMetrics).await;

        client.recv().await?;

        Ok(())
    });

    sim.run().unwrap();
}

#[test] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
fn test_metrics() {
    #[derive(Clone, Default)]
    struct Metrics {
        bytes_sent: Arc<AtomicUsize>,
        bytes_received: Arc<AtomicUsize>,
        messages_sent: Arc<AtomicU64>,
        messages_received: Arc<AtomicU64>,
    }

    impl transport::Metrics<String, String> for Metrics {
        fn bytes_sent(&mut self, len: usize) {
            self.bytes_sent.fetch_add(len, Ordering::SeqCst);
        }

        fn bytes_received(&mut self, len: usize) {
            self.bytes_received.fetch_add(len, Ordering::SeqCst);
        }

        fn message_sent(&mut self, _msg: &String) {
            self.messages_sent.fetch_add(1, Ordering::SeqCst);
        }

        fn message_received(&mut self, _msg: &String) {
            self.messages_received.fetch_add(1, Ordering::SeqCst);
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

        // Wait for message to be transmitted.
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(metrics.bytes_sent.load(Ordering::SeqCst) >= 63);
        assert!(metrics.bytes_received.load(Ordering::SeqCst) >= 44);
        assert_eq!(metrics.messages_sent.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.messages_received.load(Ordering::SeqCst), 1);

        Ok(())
    });

    sim.client("client", async move {
        let metrics = Metrics::default();

        let mut client =
            connect_ctp("turmoil:server:7777", VERSION, TIMEOUT, metrics.clone()).await;

        client.send("short".into()).await?;
        assert_eq!(
            client.recv().await?,
            Some("long message from server".into()),
        );

        // Wait for message to be transmitted.
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(metrics.bytes_sent.load(Ordering::SeqCst) >= 44);
        assert!(metrics.bytes_received.load(Ordering::SeqCst) >= 63);
        assert_eq!(metrics.messages_sent.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.messages_received.load(Ordering::SeqCst), 1);

        Ok(())
    });

    sim.run().unwrap();
}

/// A connection handler that simply forwards messages over channels.
#[derive(Debug)]
pub struct ChannelHandler<In, Out> {
    tx: mpsc::UnboundedSender<In>,
    rx: mpsc::UnboundedReceiver<Out>,
}

impl<In, Out> ChannelHandler<In, Out> {
    pub fn new(tx: mpsc::UnboundedSender<In>, rx: mpsc::UnboundedReceiver<Out>) -> Self {
        Self { tx, rx }
    }
}

#[async_trait]
impl<In: Message, Out: Message> GenericClient<In, Out> for ChannelHandler<In, Out> {
    async fn send(&mut self, cmd: In) -> anyhow::Result<()> {
        let result = self.tx.send(cmd);
        result.map_err(|_| anyhow!("client channel disconnected"))
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn recv(&mut self) -> anyhow::Result<Option<Out>> {
        // `mpsc::Receiver::recv` is cancel safe.
        match self.rx.recv().await {
            Some(resp) => Ok(Some(resp)),
            None => bail!("client channel disconnected"),
        }
    }
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
impl GenericClient<i32, i32> for OneOutputHandler {
    async fn send(&mut self, _cmd: i32) -> anyhow::Result<()> {
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<Option<i32>> {
        if self.done {
            future::pending().await
        } else {
            self.done = true;
            Ok(Some(123))
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
