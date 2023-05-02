#![warn(missing_docs)]

//! A Frontegg client to interact with the Admin API.
//!
//! # Example
//!
//! ```ignore
//! use mz_cloud_api::client::{Client};
//! use mz_cloud_api::config::{ClientBuilder, ClientConfig};
//! use mz_frontegg_client::client::{Client as FronteggClient};
//! use mz_frontegg_auth::AppPassword;
//!
//! // Build the Frontegg Client
//! let frontegg_client: FronteggClient;
//!
//! let config = ClientConfig { frontegg_client };
//!
//! let client = ClientBuilder::default()
//!     .build(config);
//!
//! // List all the available providers
//! let cloud_providers = client.list_cloud_providers().await.unwrap();
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
//! The Materialize cloud API capabilities are provided by the [client] module.
//!
//! ## Config
//!
//! The client's builder, instantiation and  configuration reside in the [config] module.
//! Every client requires an optional endpoint, otherwise, you can use the default.
//!
//! ## Note
//!
//! This crate is implemented following a similar pattern to [`mz-frontegg-client`], [rust-orb-billing] and [rust-frontegg].
//!
//! [mz-frontegg-client]:
//! [rust-orb-billing]: https://github.com/MaterializeInc/rust-orb-billing
//! [rust-frontegg]: https://github.com/MaterializeInc/rust-frontegg

pub mod client;
pub mod config;
pub mod error;
