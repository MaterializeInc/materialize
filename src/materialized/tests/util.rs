// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use once_cell::sync::Lazy;
use postgres::error::DbError;
use postgres::tls::{MakeTlsConnect, TlsConnect};
use postgres::types::{FromSql, Type};
use postgres::{NoTls, Socket};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tower_http::cors::AllowOrigin;

use materialized::{OrchestratorBackend, OrchestratorConfig, SecretsControllerConfig, TlsMode};
use mz_frontegg_auth::FronteggAuthentication;
use mz_orchestrator_process::ProcessOrchestratorConfig;
use mz_orchestrator_tracing::TracingCliArgs;
use mz_ore::id_gen::PortAllocator;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{NowFn, SYSTEM_TIME};
use mz_ore::task;
use mz_persist_client::PersistLocation;

pub static KAFKA_ADDRS: Lazy<mz_kafka_util::KafkaAddrs> =
    Lazy::new(|| match env::var("KAFKA_ADDRS") {
        Ok(addr) => addr.parse().expect("unable to parse KAFKA_ADDRS"),
        _ => "localhost:9092".parse().unwrap(),
    });
// Port 2181 is used by ZooKeeper.
static PORT_ALLOCATOR: Lazy<Arc<PortAllocator>> =
    Lazy::new(|| Arc::new(PortAllocator::new_with_filter(2100, 2200, &[2181])));

#[derive(Clone)]
pub struct Config {
    data_directory: Option<PathBuf>,
    logging_granularity: Option<Duration>,
    tls: Option<materialized::TlsConfig>,
    frontegg: Option<FronteggAuthentication>,
    unsafe_mode: bool,
    workers: usize,
    logical_compaction_window: Option<Duration>,
    now: NowFn,
    seed: u32,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            data_directory: None,
            logging_granularity: Some(Duration::from_secs(1)),
            tls: None,
            frontegg: None,
            unsafe_mode: false,
            workers: 1,
            logical_compaction_window: None,
            now: SYSTEM_TIME.clone(),
            seed: rand::random(),
        }
    }
}

impl Config {
    pub fn logging_granularity(mut self, granularity: Option<Duration>) -> Self {
        self.logging_granularity = granularity;
        self
    }

    pub fn data_directory(mut self, data_directory: impl Into<PathBuf>) -> Self {
        self.data_directory = Some(data_directory.into());
        self
    }

    pub fn with_tls(
        mut self,
        mode: TlsMode,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.tls = Some(materialized::TlsConfig {
            mode,
            cert: cert_path.into(),
            key: key_path.into(),
        });
        self
    }

    pub fn unsafe_mode(mut self) -> Self {
        self.unsafe_mode = true;
        self
    }

    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    pub fn logical_compaction_window(mut self, logical_compaction_window: Duration) -> Self {
        self.logical_compaction_window = Some(logical_compaction_window);
        self
    }

    pub fn with_frontegg(mut self, frontegg: &FronteggAuthentication) -> Self {
        self.frontegg = Some(frontegg.clone());
        self
    }

    pub fn with_now(mut self, now: NowFn) -> Self {
        self.now = now;
        self
    }
}

pub fn start_server(config: Config) -> Result<Server, anyhow::Error> {
    let runtime = Arc::new(Runtime::new()?);
    let (data_directory, temp_dir) = match config.data_directory {
        None => {
            // If no data directory is provided, we create a temporary
            // directory. The temporary directory is cleaned up when the
            // `TempDir` is dropped, so we keep it alive until the `Server` is
            // dropped.
            let temp_dir = tempfile::tempdir()?;
            (temp_dir.path().to_path_buf(), Some(temp_dir))
        }
        Some(data_directory) => (data_directory, None),
    };
    let (consensus_uri, catalog_postgres_stash, storage_postgres_stash) = {
        let seed = config.seed;
        let postgres_url = env::var("POSTGRES_URL")
            .map_err(|_| anyhow!("POSTGRES_URL environment variable is not set"))?;
        let mut conn = postgres::Client::connect(&postgres_url, NoTls)?;
        conn.batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS consensus_{seed};
             CREATE SCHEMA IF NOT EXISTS catalog_{seed};
             CREATE SCHEMA IF NOT EXISTS storage_{seed};",
        ))?;
        (
            format!("{postgres_url}?options=--search_path=consensus_{seed}"),
            format!("{postgres_url}?options=--search_path=catalog_{seed}"),
            format!("{postgres_url}?options=--search_path=storage_{seed}"),
        )
    };
    let metrics_registry = MetricsRegistry::new();
    let inner = runtime.block_on(materialized::serve(materialized::Config {
        timestamp_frequency: Duration::from_secs(1),
        logical_compaction_window: config.logical_compaction_window,
        persist_location: PersistLocation {
            blob_uri: format!("file://{}/persist/blob", data_directory.display()),
            consensus_uri,
        },
        catalog_postgres_stash,
        storage_postgres_stash,
        orchestrator: OrchestratorConfig {
            backend: OrchestratorBackend::Process(ProcessOrchestratorConfig {
                image_dir: env::current_exe()?
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .to_path_buf(),
                port_allocator: PORT_ALLOCATOR.clone(),
                // NOTE(benesch): would be nice to not have to do this, but
                // the subprocess output wreaks havoc on cargo2junit.
                suppress_output: true,
                data_dir: data_directory.clone(),
                command_wrapper: vec![],
            }),
            storaged_image: "storaged".into(),
            computed_image: "computed".into(),
            linger: false,
            tracing: TracingCliArgs::default(),
        },
        secrets_controller: SecretsControllerConfig::LocalFileSystem(
            data_directory.join("secrets"),
        ),
        sql_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        http_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        internal_sql_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        internal_http_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        tls: config.tls,
        frontegg: config.frontegg,
        unsafe_mode: config.unsafe_mode,
        metrics_registry: metrics_registry.clone(),
        now: config.now,
        cors_allowed_origin: AllowOrigin::list([]),
        replica_sizes: Default::default(),
        availability_zones: Default::default(),
        connector_context: Default::default(),
    }))?;
    let server = Server {
        inner,
        runtime,
        metrics_registry,
        _temp_dir: temp_dir,
    };
    Ok(server)
}

pub struct Server {
    pub inner: materialized::Server,
    pub runtime: Arc<Runtime>,
    _temp_dir: Option<TempDir>,
    pub metrics_registry: MetricsRegistry,
}

impl Server {
    pub fn pg_config(&self) -> postgres::Config {
        let local_addr = self.inner.sql_local_addr();
        let mut config = postgres::Config::new();
        config
            .host(&Ipv4Addr::LOCALHOST.to_string())
            .port(local_addr.port())
            .user("materialize");
        config
    }

    pub fn pg_config_async(&self) -> tokio_postgres::Config {
        let local_addr = self.inner.sql_local_addr();
        let mut config = tokio_postgres::Config::new();
        config
            .host(&Ipv4Addr::LOCALHOST.to_string())
            .port(local_addr.port())
            .user("materialize");
        config
    }

    pub fn connect<T>(&self, tls: T) -> Result<postgres::Client, anyhow::Error>
    where
        T: MakeTlsConnect<Socket> + Send + 'static,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        Ok(self.pg_config().connect(tls)?)
    }

    pub async fn connect_async<T>(
        &self,
        tls: T,
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), anyhow::Error>
    where
        T: MakeTlsConnect<Socket> + Send + 'static,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let (client, conn) = self.pg_config_async().connect(tls).await?;
        let handle = task::spawn(|| "connect_async", async move {
            if let Err(err) = conn.await {
                panic!("connection error: {}", err);
            }
        });
        Ok((client, handle))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct MzTimestamp(pub u64);

impl<'a> FromSql<'a> for MzTimestamp {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<MzTimestamp, Box<dyn Error + Sync + Send>> {
        let n = mz_pgrepr::Numeric::from_sql(ty, raw)?;
        Ok(MzTimestamp(u64::try_from(n.0 .0)?))
    }

    fn accepts(ty: &Type) -> bool {
        mz_pgrepr::Numeric::accepts(ty)
    }
}

pub trait PostgresErrorExt {
    fn unwrap_db_error(self) -> DbError;
}

impl PostgresErrorExt for postgres::Error {
    fn unwrap_db_error(self) -> DbError {
        match self.source().and_then(|e| e.downcast_ref::<DbError>()) {
            Some(e) => e.clone(),
            None => panic!("expected DbError, but got: {:?}", self),
        }
    }
}

impl<T, E> PostgresErrorExt for Result<T, E>
where
    E: PostgresErrorExt,
{
    fn unwrap_db_error(self) -> DbError {
        match self {
            Ok(_) => panic!("expected Err(DbError), but got Ok(_)"),
            Err(e) => e.unwrap_db_error(),
        }
    }
}
