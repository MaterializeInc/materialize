// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![deny(missing_debug_implementations)]

//! Confluent-compatible schema registry API client.

use std::error::Error;
use std::fmt;

use reqwest::Url;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// An API client for a Confluent-compatible schema registry.
#[derive(Debug)]
pub struct Client {
    inner: reqwest::Client,
    url: Url,
}

// Encodes the type of certificate file, as well as the certificate's bytes. In
// the case of der certificates, it also stores the password.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum CertDetails {
    PEM(Vec<u8>),
    DER(Vec<u8>, String),
}

/// Provides a serde wrapper around
/// [`reqwest::Identity`](https://docs.rs/reqwest/latest/reqwest/struct.Identity.html).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    cert: CertDetails,
}

impl Identity {
    /// Wraps
    /// [`reqwest::Identity::from_pem`](https://docs.rs/reqwest/latest/reqwest/struct.Identity.html#method.from_pem).
    pub fn from_pem(pem: &[u8]) -> Result<Self, reqwest::Error> {
        let _ = reqwest::Identity::from_pem(&pem)?;
        Ok(Identity {
            cert: CertDetails::PEM(pem.into()),
        })
    }

    /// Wraps
    /// [`reqwest::Identity::from_pem`](https://docs.rs/reqwest/latest/reqwest/struct.Identity.html#method.from_pkcs12_der).
    pub fn from_pkcs12_der(der: &[u8], password: &str) -> Result<Self, reqwest::Error> {
        let _ = reqwest::Identity::from_pkcs12_der(&der, password)?;
        Ok(Identity {
            cert: CertDetails::DER(der.into(), password.to_string()),
        })
    }
}

impl Into<reqwest::Identity> for Identity {
    fn into(self) -> reqwest::Identity {
        match self.cert {
            CertDetails::PEM(pem) => {
                reqwest::Identity::from_pem(&pem).expect("known to be a valid identity")
            }
            CertDetails::DER(der, pass) => reqwest::Identity::from_pkcs12_der(&der, &pass)
                .expect("known to be a valid identity"),
        }
    }
}

/// Provides a serde wrapper around
/// [`reqwest::Certificate`](https://docs.rs/reqwest/latest/reqwest/struct.Certificate.html).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Certificate {
    der: Vec<u8>,
}
impl Certificate {
    pub fn from_pem(pem: &[u8]) -> native_tls::Result<Certificate> {
        Ok(Certificate {
            der: native_tls::Certificate::from_pem(pem)?.to_der()?,
        })
    }
    pub fn from_der(der: &[u8]) -> native_tls::Result<Certificate> {
        let _ = native_tls::Certificate::from_der(der)?;
        Ok(Certificate { der: der.into() })
    }
}
impl Into<reqwest::Certificate> for Certificate {
    fn into(self) -> reqwest::Certificate {
        reqwest::Certificate::from_der(&self.der).expect("known to be a valid cert")
    }
}

/// Represents the Confluent Schema Registry you want to connect to, including
/// potential TLS configuration.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientConfig {
    pub url: Url,
    pub root_certs: Vec<Certificate>,
    pub identity: Option<Identity>,
}

impl ClientConfig {
    pub fn new(url: Url) -> ClientConfig {
        ClientConfig {
            url,
            root_certs: Vec::new(),
            identity: None,
        }
    }
    pub fn add_root_certificate(mut self, cert: Certificate) -> ClientConfig {
        self.root_certs.push(cert);
        self
    }
    pub fn identity(mut self, identity: Identity) -> ClientConfig {
        self.identity = Some(identity);
        self
    }
    pub fn build(&self) -> Client {
        let mut builder = reqwest::Client::builder();

        for root_cert in &self.root_certs {
            builder = builder.add_root_certificate(root_cert.clone().into());
        }

        if let Some(ident) = &self.identity {
            match ident.cert {
                CertDetails::PEM(_) => {
                    builder = builder.use_rustls_tls();
                }
                CertDetails::DER(_, _) => {
                    builder = builder.use_native_tls();
                }
            }
            builder = builder.identity(ident.clone().into());
        }

        let inner = builder
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        Client {
            inner,
            url: self.url.clone(),
        }
    }
}

impl Client {
    /// Creates a new API client that will send requests to the CSR at `config.url`.
    pub fn new(config: &ClientConfig) -> Self {
        config.build()
    }

    /// Gets the schema with the associated ID.
    pub async fn get_schema_by_id(&self, id: i32) -> Result<Schema, GetByIdError> {
        let mut url = self.url.clone();
        url.set_path(&format!("/schemas/ids/{}", id));
        let res: GetByIdResponse = send_request(self.inner.get(url)).await?;
        Ok(Schema {
            id,
            raw: res.schema,
        })
    }

    /// Gets the latest schema for the specified subject.
    pub async fn get_schema_by_subject(&self, subject: &str) -> Result<Schema, GetBySubjectError> {
        let mut url = self.url.clone();
        url.set_path(&format!("/subjects/{}/versions/latest", subject));
        let res: GetBySubjectResponse = send_request(self.inner.get(url)).await?;
        Ok(Schema {
            id: res.id,
            raw: res.schema,
        })
    }

    /// Publishes a new schema for the specified subject. The ID of the new
    /// schema is returned.
    ///
    /// Note that if a schema that is identical to an existing schema for the
    /// same subject is published, the ID of the existing schema will be
    /// returned.
    pub async fn publish_schema(&self, subject: &str, schema: &str) -> Result<i32, PublishError> {
        let mut url = self.url.clone();
        url.set_path(&format!("/subjects/{}/versions", subject));
        let json = json!({ "schema": schema }).to_string();
        let res: PublishResponse = send_request(self.inner.post(url).body(json)).await?;
        Ok(res.id)
    }

    /// Lists the names of all subjects that the schema registry is aware of.
    pub async fn list_subjects(&self) -> Result<Vec<String>, ListError> {
        let mut url = self.url.clone();
        url.set_path("/subjects");
        Ok(send_request(self.inner.get(url)).await?)
    }

    /// Deletes all schema versions associated with the specified subject.
    ///
    /// This API is only intended to be used in development environments.
    /// Deleting schemas only allows new, potentially incompatible schemas to
    /// be registered under the same subject. It does not allow the schema ID
    /// to be reused.
    pub async fn delete_subject(&self, subject: &str) -> Result<(), DeleteError> {
        let mut url = self.url.clone();
        url.set_path(&format!("/subjects/{}", subject));
        let _res: Vec<i32> = send_request(self.inner.delete(url)).await?;
        Ok(())
    }
}

async fn send_request<T>(req: reqwest::RequestBuilder) -> Result<T, UnhandledError>
where
    T: DeserializeOwned,
{
    let res = req.send().await?;
    let status = res.status();
    if status.is_success() {
        Ok(res.json().await?)
    } else {
        match res.json::<ErrorResponse>().await {
            Ok(err_res) => Err(UnhandledError::Api {
                code: err_res.error_code,
                message: err_res.message,
            }),
            Err(_) => Err(UnhandledError::Api {
                code: i32::from(status.as_u16()),
                message: "unable to decode error details".into(),
            }),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Schema {
    pub id: i32,
    pub raw: String,
}

#[derive(Debug, Deserialize)]
struct GetByIdResponse {
    schema: String,
}

#[derive(Debug)]
pub enum GetByIdError {
    SchemaNotFound,
    Transport(reqwest::Error),
    Server { code: i32, message: String },
}

impl From<UnhandledError> for GetByIdError {
    fn from(err: UnhandledError) -> GetByIdError {
        match err {
            UnhandledError::Transport(err) => GetByIdError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                40403 => GetByIdError::SchemaNotFound,
                _ => GetByIdError::Server { code, message },
            },
        }
    }
}

impl Error for GetByIdError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            GetByIdError::SchemaNotFound | GetByIdError::Server { .. } => None,
            GetByIdError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for GetByIdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GetByIdError::SchemaNotFound => write!(f, "schema not found"),
            GetByIdError::Transport(err) => write!(f, "transport: {}", err),
            GetByIdError::Server { code, message } => {
                write!(f, "server error {}: {}", code, message)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct GetBySubjectResponse {
    id: i32,
    schema: String,
}

#[derive(Debug)]
pub enum GetBySubjectError {
    SubjectNotFound,
    Transport(reqwest::Error),
    Server { code: i32, message: String },
}

impl From<UnhandledError> for GetBySubjectError {
    fn from(err: UnhandledError) -> GetBySubjectError {
        match err {
            UnhandledError::Transport(err) => GetBySubjectError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                40401 => GetBySubjectError::SubjectNotFound,
                _ => GetBySubjectError::Server { code, message },
            },
        }
    }
}

impl Error for GetBySubjectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            GetBySubjectError::SubjectNotFound | GetBySubjectError::Server { .. } => None,
            GetBySubjectError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for GetBySubjectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GetBySubjectError::SubjectNotFound => write!(f, "subject not found"),
            GetBySubjectError::Transport(err) => write!(f, "transport: {}", err),
            GetBySubjectError::Server { code, message } => {
                write!(f, "server error {}: {}", code, message)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct PublishResponse {
    id: i32,
}

#[derive(Debug)]
pub enum PublishError {
    IncompatibleSchema,
    InvalidSchema,
    Transport(reqwest::Error),
    Server { code: i32, message: String },
}

impl From<UnhandledError> for PublishError {
    fn from(err: UnhandledError) -> PublishError {
        match err {
            UnhandledError::Transport(err) => PublishError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                409 => PublishError::IncompatibleSchema,
                42201 => PublishError::InvalidSchema,
                _ => PublishError::Server { code, message },
            },
        }
    }
}

impl Error for PublishError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            PublishError::IncompatibleSchema
            | PublishError::InvalidSchema
            | PublishError::Server { .. } => None,
            PublishError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            // The error descriptions for IncompatibleSchema and InvalidSchema
            // are copied from the schema registry itself.
            PublishError::IncompatibleSchema => write!(
                f,
                "schema being registered is incompatible with an earlier schema"
            ),
            PublishError::InvalidSchema => write!(f, "input schema is an invalid avro schema"),
            PublishError::Transport(err) => write!(f, "transport: {}", err),
            PublishError::Server { code, message } => {
                write!(f, "server error {}: {}", code, message)
            }
        }
    }
}

#[derive(Debug)]
pub enum ListError {
    Transport(reqwest::Error),
    Server { code: i32, message: String },
}

impl From<UnhandledError> for ListError {
    fn from(err: UnhandledError) -> ListError {
        match err {
            UnhandledError::Transport(err) => ListError::Transport(err),
            UnhandledError::Api { code, message } => ListError::Server { code, message },
        }
    }
}

impl Error for ListError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ListError::Server { .. } => None,
            ListError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for ListError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ListError::Transport(err) => write!(f, "transport: {}", err),
            ListError::Server { code, message } => write!(f, "server error {}: {}", code, message),
        }
    }
}

#[derive(Debug)]
pub enum DeleteError {
    SubjectNotFound,
    Transport(reqwest::Error),
    Server { code: i32, message: String },
}

impl From<UnhandledError> for DeleteError {
    fn from(err: UnhandledError) -> DeleteError {
        match err {
            UnhandledError::Transport(err) => DeleteError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                40401 => DeleteError::SubjectNotFound,
                _ => DeleteError::Server { code, message },
            },
        }
    }
}

impl Error for DeleteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DeleteError::SubjectNotFound | DeleteError::Server { .. } => None,
            DeleteError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for DeleteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DeleteError::SubjectNotFound => write!(f, "subject not found"),
            DeleteError::Transport(err) => write!(f, "transport: {}", err),
            DeleteError::Server { code, message } => {
                write!(f, "server error {}: {}", code, message)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error_code: i32,
    message: String,
}

#[derive(Debug)]
pub enum UnhandledError {
    Transport(reqwest::Error),
    Api { code: i32, message: String },
}

impl From<reqwest::Error> for UnhandledError {
    fn from(err: reqwest::Error) -> UnhandledError {
        UnhandledError::Transport(err)
    }
}
