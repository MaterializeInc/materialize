// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use url::Url;

use mz_repr::proto::newapi::{IntoRustIfSome, ProtoType, RustType};
use mz_repr::proto::TryFromProtoError;
use mz_repr::url::any_url;

use crate::client::Client;
use crate::tls::{Certificate, Identity};

include!(concat!(env!("OUT_DIR"), "/mz_ccsr.config.rs"));

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Auth {
    pub username: String,
    pub password: Option<String>,
}

impl RustType<ProtoAuth> for Auth {
    fn into_proto(self: &Self) -> ProtoAuth {
        ProtoAuth {
            username: self.username.clone(),
            password: self.password.clone(),
        }
    }

    fn from_proto(proto: ProtoAuth) -> Result<Self, TryFromProtoError> {
        Ok(Auth {
            username: proto.username,
            password: proto.password,
        })
    }
}

/// Configuration for a `Client`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientConfig {
    url: Url,
    root_certs: Vec<Certificate>,
    identity: Option<Identity>,
    auth: Option<Auth>,
}

impl Arbitrary for ClientConfig {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any_url(),
            any::<Vec<Certificate>>(),
            any::<Option<Identity>>(),
            any::<Option<Auth>>(),
        )
            .prop_map(|(url, root_certs, identity, auth)| ClientConfig {
                url,
                root_certs,
                identity,
                auth,
            })
            .boxed()
    }
}

impl RustType<ProtoClientConfig> for ClientConfig {
    fn into_proto(self: &Self) -> ProtoClientConfig {
        ProtoClientConfig {
            url: Some(self.url.into_proto()),
            root_certs: self.root_certs.into_proto(),
            identity: self.identity.into_proto(),
            auth: self.auth.into_proto(),
        }
    }

    fn from_proto(proto: ProtoClientConfig) -> Result<Self, TryFromProtoError> {
        Ok(ClientConfig {
            url: proto.url.into_rust_if_some("ProtoClientConfig::url")?,
            root_certs: proto.root_certs.into_rust()?,
            identity: proto.identity.into_rust()?,
            auth: proto.auth.into_rust()?,
        })
    }
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
        let mut builder = reqwest::ClientBuilder::new();

        for root_cert in self.root_certs {
            builder = builder.add_root_certificate(root_cert.into());
        }

        if let Some(ident) = self.identity {
            builder = builder.identity(ident.into());
        }

        let inner = builder
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();

        Client::new(inner, self.url, self.auth)
    }
}
