// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryInto;
use std::env;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lazy_static::lazy_static;
use postgres::error::DbError;
use postgres::tls::{MakeTlsConnect, TlsConnect};
use postgres::types::{FromSql, Type};
use postgres::Socket;
use tempfile::TempDir;
use tokio::runtime::Runtime;

use materialized::TlsMode;

lazy_static! {
    pub static ref KAFKA_ADDRS: kafka_util::KafkaAddrs = match env::var("KAFKA_ADDRS") {
        Ok(addr) => addr.parse().expect("unable to parse KAFKA_ADDRS"),
        _ => "localhost:9092".parse().unwrap(),
    };
}

#[derive(Clone)]
pub struct Config {
    data_directory: Option<PathBuf>,
    logging_granularity: Option<Duration>,
    tls: Option<materialized::TlsConfig>,
    experimental_mode: bool,
    safe_mode: bool,
    workers: usize,
    logical_compaction_window: Option<Duration>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            data_directory: None,
            logging_granularity: Some(Duration::from_secs(1)),
            tls: None,
            experimental_mode: false,
            safe_mode: false,
            workers: 1,
            logical_compaction_window: None,
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

    pub fn experimental_mode(mut self) -> Self {
        self.experimental_mode = true;
        self
    }

    pub fn safe_mode(mut self) -> Self {
        self.safe_mode = true;
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
}

pub fn start_server(config: Config) -> Result<Server, Box<dyn Error>> {
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
    let inner = runtime.block_on(materialized::serve(
        materialized::Config {
            logging: config
                .logging_granularity
                .map(|granularity| coord::LoggingConfig {
                    granularity,
                    log_logging: false,
                    retain_readings_for: granularity,
                }),
            timestamp_frequency: Duration::from_secs(1),
            cache: None,
            persistence: None,
            logical_compaction_window: config.logical_compaction_window,
            workers: config.workers,
            timely_worker: timely::WorkerConfig::default(),
            data_directory,
            symbiosis_url: None,
            listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            tls: config.tls,
            experimental_mode: config.experimental_mode,
            safe_mode: config.safe_mode,
            telemetry_url: None,
        },
        runtime.clone(),
    ))?;
    let server = Server {
        inner,
        runtime,
        _temp_dir: temp_dir,
    };
    Ok(server)
}

pub struct Server {
    pub inner: materialized::Server,
    pub runtime: Arc<Runtime>,
    _temp_dir: Option<TempDir>,
}

impl Server {
    pub fn pg_config(&self) -> postgres::Config {
        let local_addr = self.inner.local_addr();
        let mut config = postgres::Config::new();
        config
            .host(&Ipv4Addr::LOCALHOST.to_string())
            .port(local_addr.port())
            .user("materialize");
        config
    }

    pub fn pg_config_async(&self) -> tokio_postgres::Config {
        let local_addr = self.inner.local_addr();
        let mut config = tokio_postgres::Config::new();
        config
            .host(&Ipv4Addr::LOCALHOST.to_string())
            .port(local_addr.port())
            .user("materialize");
        config
    }

    pub fn connect<T>(&self, tls: T) -> Result<postgres::Client, Box<dyn Error>>
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
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), Box<dyn Error>>
    where
        T: MakeTlsConnect<Socket> + Send + 'static,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let (client, conn) = self.pg_config_async().connect(tls).await?;
        let handle = tokio::spawn(async move {
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
        let n = pgrepr::Numeric::from_sql(ty, raw)?;
        if n.0.scale() != 0 {
            return Err("scale of numeric was not 0".into());
        }
        Ok(MzTimestamp(n.0.significand().try_into()?))
    }

    fn accepts(ty: &Type) -> bool {
        pgrepr::Numeric::accepts(ty)
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
