// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! A Frontegg client to interact with the Admin API.
//!
//! # Example
//!
//! ```ignore
//! use mz_frontegg_client::client::{Client};
//! use mz_frontegg_client::config::{ClientBuilder, ClientConfig};
//! use mz_frontegg_auth::AppPassword;
//!
//! let app_password = AppPassword {
//!     client_id: "client-id".parse().unwrap(),
//!     secret_key: "client-secret".parse().unwrap(),
//! };
//!
//! let config = ClientConfig { app_password };
//!
//! let client = ClientBuilder::default()
//!     .build(config);
//!
//! let response = client.list_app_passwords().await?;
//! ```
//!
//! ## Implementation
//!
//! It is divided into three modules: [client], [config], [error]
//!
//! ## Error
//!
//! The [error] crate contains the definitions and structures for all possible errors.
//!
//! ## Client
//!
//! The Frontegg API capabilities are provided by the [client] module,
//! which currently only enables listing or creating users and passwords.
//!
//! ## Config
//!
//! The client's builder, instantiation and  configuration reside in the [config] module.
//! Every client requires an optional endpoint, otherwise, you can use the default.
//!
//! ## Note
//!
//! This crate is implemented following a similar pattern to [rust-orb-billing] and [rust-frontegg].
//!
//! [rust-orb-billing]: https://github.com/MaterializeInc/rust-orb-billing
//! [rust-frontegg]: https://github.com/MaterializeInc/rust-frontegg

pub(crate) mod parse;

pub mod client;
pub mod config;
pub mod error;
