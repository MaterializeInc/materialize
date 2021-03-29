// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use reqwest::Url;
use serde::{Deserialize, Serialize};

use crate::client::Client;
use crate::tls::{Certificate, Identity};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Auth {
    pub username: String,
    pub password: Option<String>,
}

/// Configuration for a `Client`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientConfig {
    url: Url,
    root_certs: Vec<Certificate>,
    identity: Option<Identity>,
    auth: Option<Auth>,
}

impl ClientConfig {
    /// Constructs a new `ClientConfig` that will target the schema registry at
    /// the specified URL.
    pub fn new(url: Url) -> ClientConfig {
        ClientConfig {
            url,
            root_certs: Vec::new(),
            identity: None,
            auth: None,
        }
    }

    /// Adds a trusted root TLS certificate.
    ///
    /// Certificates in the system's certificate store are trusted by default.
    pub fn add_root_certificate(mut self, cert: Certificate) -> ClientConfig {
        self.root_certs.push(cert);
        self
    }

    /// Enables TLS client authentication with the provided identity.
    pub fn identity(mut self, identity: Identity) -> ClientConfig {
        self.identity = Some(identity);
        self
    }

    /// Enables HTTP basic authentication with the specified username and
    /// optional password.
    pub fn auth(mut self, username: String, password: Option<String>) -> ClientConfig {
        self.auth = Some(Auth { username, password });
        self
    }

    /// Builds the [`Client`].
    pub fn build(self) -> Result<Client, anyhow::Error> {
        let mut builder = http_util::reqwest_client_builder()
            .context("Creating HTTP client for schema registry")?;

        for root_cert in self.root_certs {
            builder = builder.add_root_certificate(root_cert.into());
        }

        if let Some(ident) = self.identity {
            builder = builder.identity(ident.into());
        }

        let inner = builder
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        Ok(Client::new(inner, self.url, self.auth))
    }
}
