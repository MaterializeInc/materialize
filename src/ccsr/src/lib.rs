// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_debug_implementations)]

//! The `ccsr` crate provides an ergonomic API client for Confluent-compatible
//! schema registries (CCSRs).
//!
//! The only known CCSR implementation is the [Confluent Schema Registry], but
//! this crate is compatible with any implementation that adheres to the
//! CCSR [API specification].
//!
//! ## Example usage
//!
//! ```no_run
//! # async {
//! use mz_ccsr::ClientConfig;
//!
//! let url = "http://localhost:8080".parse()?;
//! let client = ClientConfig::new(url).build()?;
//! let subjects = client.list_subjects().await?;
//! for subject in subjects {
//!     let schema = client.get_schema_by_subject(&subject).await?;
//!     // Do something with `schema`.
//! }
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! # };
//! ```
//!
//!
//! [API specification]: https://docs.confluent.io/current/schema-registry/develop/api.html
//! [Confluent Schema Registry]: https://docs.confluent.io/current/schema-registry/index.html

mod client;
mod config;

pub mod tls;

pub use client::*;
pub use config::{ClientConfig, ProtoClientConfig};
