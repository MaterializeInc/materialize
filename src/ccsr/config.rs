// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use reqwest::Url;
use serde::{Deserialize, Serialize};

use crate::client::Client;
use crate::tls::{Certificate, Identity};

/// Configuration for a `Client`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientConfig {
    url: Url,
    root_certs: Vec<Certificate>,
    identity: Option<Identity>,
}

impl ClientConfig {
    /// Constructs a new `ClientConfig` that will target the schema registry at
    /// the specified URL.
    pub fn new(url: Url) -> ClientConfig {
        ClientConfig {
            url,
            root_certs: Vec::new(),
            identity: None,
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

    /// Builds the [`Client`].
    pub fn build(self) -> Client {
        let mut builder = reqwest::Client::builder();

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

        Client::new(inner, self.url)
    }
}
