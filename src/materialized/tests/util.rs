// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use openssl::asn1::Asn1Time;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::PKey;
use openssl::rsa::Rsa;
use openssl::x509::extension::SubjectAlternativeName;
use openssl::x509::{X509NameBuilder, X509};

#[derive(Clone)]
pub struct Config {
    data_directory: Option<PathBuf>,
    logging_granularity: Option<Duration>,
    tls: Option<materialized::TlsConfig>,
    experimental_mode: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            data_directory: None,
            logging_granularity: Some(Duration::from_millis(10)),
            tls: None,
            experimental_mode: false,
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
}

pub fn start_server(config: Config) -> Result<(Server, postgres::Client), Box<dyn Error>> {
    let server = Server(materialized::serve(materialized::Config {
        logging_granularity: config.logging_granularity,
        timestamp_frequency: Duration::from_millis(10),
        persistence: None,
        logical_compaction_window: None,
        threads: 1,
        process: 0,
        addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)],
        data_directory: config.data_directory,
        symbiosis_url: None,
        listen_addr: None,
        tls: config.tls,
        experimental_mode: config.experimental_mode,
    })?);
    let client = server.connect()?;
    Ok((server, client))
}

pub struct Server(materialized::Server);

impl Server {
    pub fn pg_config(&self) -> postgres::Config {
        let local_addr = self.0.local_addr();
        let mut config = postgres::Config::new();
        config
            .host(&Ipv4Addr::LOCALHOST.to_string())
            .port(local_addr.port())
            .user("root");
        config
    }

    pub fn pg_config_async(&self) -> tokio_postgres::Config {
        let local_addr = self.0.local_addr();
        let mut config = tokio_postgres::Config::new();
        config
            .host(&Ipv4Addr::LOCALHOST.to_string())
            .port(local_addr.port())
            .user("root");
        config
    }

    pub fn connect(&self) -> Result<postgres::Client, Box<dyn Error>> {
        Ok(self.pg_config().connect(postgres::NoTls)?)
    }

    pub async fn connect_async(&self) -> Result<tokio_postgres::Client, Box<dyn Error>> {
        let (client, conn) = self
            .pg_config_async()
            .connect(tokio_postgres::NoTls)
            .await?;
        tokio::spawn(async move {
            if let Err(err) = conn.await {
                panic!("connection error: {}", err);
            }
        });
        Ok(client)
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
