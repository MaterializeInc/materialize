// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
#![warn(clippy::large_futures)]
// END LINT CONFIG

//! An API client for [Metabase].
//!
//! Only the features presently required are implemented. Documentation is
//! sparse to avoid duplicating information in the upstream API documentation.
//! See:
//!
//!   * [Using the REST API](https://github.com/metabase/metabase/wiki/Using-the-REST-API)
//!   * [Auto-generated API documentation](https://github.com/metabase/metabase/blob/master/docs/api-documentation.md)
//!
//! [Metabase]: https://metabase.com

#![warn(missing_debug_implementations)]

use std::fmt;
use std::time::Duration;

use reqwest::{IntoUrl, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// A Metabase API client.
#[derive(Debug)]
pub struct Client {
    inner: reqwest::Client,
    url: Url,
    session_id: Option<String>,
}

impl Client {
    /// Constructs a new `Client` that will target a Metabase instance at `url`.
    ///
    /// `url` must not contain a path nor be a [cannot-be-a-base] URL.
    ///
    /// [cannot-be-a-base]: https://url.spec.whatwg.org/#url-cannot-be-a-base-url-flag
    pub fn new<U>(url: U) -> Result<Self, Error>
    where
        U: IntoUrl,
    {
        let mut url = url.into_url()?;
        if url.path() != "/" {
            return Err(Error::InvalidUrl("base URL cannot have path".into()));
        }
        assert!(!url.cannot_be_a_base());
        url.path_segments_mut()
            .expect("cannot-be-a-base checked to be false")
            .push("api");
        Ok(Client {
            inner: reqwest::Client::new(),
            url,
            session_id: None,
        })
    }

    /// Sets the session ID to include in future requests made by this client.
    pub fn set_session_id(&mut self, session_id: String) {
        self.session_id = Some(session_id);
    }

    /// Fetches public, global properties.
    ///
    /// The underlying API call is `GET /api/session/properties`.
    pub async fn session_properties(&self) -> Result<SessionPropertiesResponse, reqwest::Error> {
        let url = self.api_url(&["session", "properties"]);
        self.send_request(self.inner.get(url)).await
    }

    /// Requests a session ID for the username and password named in `request`.
    ///
    /// Note that usernames are typically email addresses. To authenticate
    /// future requests with the returned session ID, call `set_session_id`.
    ///
    /// The underlying API call is `POST /api/session`.
    pub async fn login(&self, request: &LoginRequest) -> Result<LoginResponse, reqwest::Error> {
        let url = self.api_url(&["session"]);
        self.send_request(self.inner.post(url).json(request)).await
    }

    /// Creates a user and database connection if the Metabase instance has not
    /// yet been set up.
    ///
    /// The request must include the `setup_token` from a
    /// `SessionPropertiesResponse`. If the setup token returned by
    /// [`Client::session_properties`] is `None`, the cluster is already set up,
    /// and this request will fail.
    ///
    /// The underlying API call is `POST /api/setup`.
    pub async fn setup(&self, request: &SetupRequest) -> Result<LoginResponse, reqwest::Error> {
        let url = self.api_url(&["setup"]);
        self.send_request(self.inner.post(url).json(request)).await
    }

    /// Fetches the list of databases known to Metabase.
    ///
    /// The underlying API call is `GET /database`.
    pub async fn databases(&self) -> Result<Vec<Database>, reqwest::Error> {
        let url = self.api_url(&["database"]);
        let res: ListWrapper<_> = self.send_request(self.inner.get(url)).await?;
        Ok(res.data)
    }

    /// Fetches metadata about a particular database.
    ///
    /// The underlying API call is `GET /database/:id/metadata`.
    pub async fn database_metadata(&self, id: usize) -> Result<DatabaseMetadata, reqwest::Error> {
        let url = self.api_url(&["database", &id.to_string(), "metadata"]);
        self.send_request(self.inner.get(url)).await
    }

    fn api_url(&self, endpoint: &[&str]) -> Url {
        let mut url = self.url.clone();
        url.path_segments_mut()
            .expect("url validated on construction")
            .extend(endpoint);
        url
    }

    async fn send_request<T>(&self, mut req: reqwest::RequestBuilder) -> Result<T, reqwest::Error>
    where
        T: DeserializeOwned,
    {
        req = req.timeout(Duration::from_secs(5));
        if let Some(session_id) = &self.session_id {
            req = req.header("X-Metabase-Session", session_id);
        }
        let res = req.send().await?.error_for_status()?;
        res.json().await
    }
}

/// A Metabase error.
#[derive(Debug)]
pub enum Error {
    /// The provided URL was invalid.
    InvalidUrl(String),
    /// The underlying transport mechanism returned na error.
    Transport(reqwest::Error),
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Error {
        Error::Transport(e)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::InvalidUrl(_) => None,
            Error::Transport(e) => Some(e),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::InvalidUrl(msg) => write!(f, "invalid url: {}", msg),
            Error::Transport(e) => write!(f, "transport: {}", e),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
struct ListWrapper<T> {
    data: Vec<T>,
}

/// The response to [`Client::session_properties`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct SessionPropertiesResponse {
    pub setup_token: Option<String>,
}

/// The request for [`Client::setup`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct SetupRequest {
    pub allow_tracking: bool,
    pub database: SetupDatabase,
    pub token: String,
    pub prefs: SetupPrefs,
    pub user: SetupUser,
}

/// A database to create as part of a [`SetupRequest`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct SetupDatabase {
    pub engine: String,
    pub name: String,
    pub details: SetupDatabaseDetails,
}

/// Details for a [`SetupDatabase`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct SetupDatabaseDetails {
    pub host: String,
    pub port: usize,
    pub dbname: String,
    pub user: String,
}

/// Preferences for a [`SetupRequest`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct SetupPrefs {
    pub site_name: String,
}

/// A user to create as part of a [`SetupRequest`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct SetupUser {
    pub email: String,
    pub first_name: String,
    pub last_name: String,
    pub password: String,
    pub site_name: String,
}

/// The request for [`Client::login`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// The response to [`Client::login`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct LoginResponse {
    pub id: String,
}

/// A database returned by [`Client::databases`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct Database {
    pub name: String,
    pub id: usize,
}

/// The response to [`Client::database_metadata`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct DatabaseMetadata {
    pub tables: Vec<Table>,
}

/// A table that is part of [`DatabaseMetadata`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct Table {
    pub name: String,
    pub schema: String,
    pub fields: Vec<TableField>,
}

/// A field of a [`Table`].
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct TableField {
    pub name: String,
    pub database_type: String,
    pub base_type: String,
    pub special_type: Option<String>,
}
