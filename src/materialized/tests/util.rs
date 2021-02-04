// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryInto;
use std::error::Error;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use openssl::asn1::Asn1Time;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::PKey;
use openssl::rsa::Rsa;
use openssl::x509::extension::SubjectAlternativeName;
use openssl::x509::{X509NameBuilder, X509};
use postgres::tls::{MakeTlsConnect, TlsConnect};
use postgres::types::{FromSql, Type};
use postgres::Socket;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[derive(Clone)]
pub struct Config {
    data_directory: Option<PathBuf>,
    logging_granularity: Option<Duration>,
    tls: Option<materialized::TlsConfig>,
    experimental_mode: bool,
    workers: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            data_directory: None,
            logging_granularity: Some(Duration::from_millis(10)),
            tls: None,
            experimental_mode: false,
            workers: 1,
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

    pub fn enable_tls(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.tls = Some(materialized::TlsConfig {
            cert: cert_path.into(),
            key: key_path.into(),
        });
        self
    }

    pub fn experimental_mode(mut self) -> Self {
        self.experimental_mode = true;
        self
    }

    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers;
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
                }),
            timestamp_frequency: Duration::from_millis(10),
            cache: None,
            logical_compaction_window: None,
            workers: config.workers,
            process: 0,
            addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)],
            timely_worker: timely::WorkerConfig::default(),
            data_directory,
            symbiosis_url: None,
            listen_addr: None,
            tls: config.tls,
            experimental_mode: config.experimental_mode,
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

pub fn generate_certs(cert_path: &Path, key_path: &Path) -> Result<(), Box<dyn Error>> {
    let rsa = Rsa::generate(2048)?;
    let pkey = PKey::from_rsa(rsa)?;
    let name = {
        let mut builder = X509NameBuilder::new()?;
        builder.append_entry_by_nid(Nid::COMMONNAME, "test certificate")?;
        builder.build()
    };
    let cert = {
        let mut builder = X509::builder()?;
        builder.set_version(2)?;
        builder.set_pubkey(&pkey)?;
        builder.set_issuer_name(&name)?;
        builder.set_subject_name(&name)?;
        builder.append_extension(
            SubjectAlternativeName::new()
                .ip(&Ipv4Addr::LOCALHOST.to_string())
                .build(&builder.x509v3_context(None, None))?,
        )?;
        builder.set_not_before(&*Asn1Time::days_from_now(0)?)?;
        builder.set_not_after(&*Asn1Time::days_from_now(365)?)?;
        builder.sign(&pkey, MessageDigest::sha256())?;
        builder.build()
    };
    fs::write(cert_path, &cert.to_pem()?)?;
    fs::write(key_path, &pkey.private_key_to_pem_pkcs8()?)?;
    Ok(())
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
