// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL-dataflow translation.
//!
//! There are two main parts of the SQL–dataflow translation process:
//!
//!   * **Purification** eliminates any external state from a SQL AST. It is an
//!     asynchronous process that may make network calls to external services.
//!     The input and output of purification is a SQL AST.
//!
//!   * **Planning** converts a purified AST to a [`Plan`], which describes an
//!     action that the system should take to effect the results of the query.
//!     Planning is a fast, pure function that always produces the same plan for
//!     a given input.
//!
//! # Details
//!
//! The purification step is, to our knowledge, unique to Materialize. In other
//! SQL databases, there is no concept of purifying a statement before planning
//! it. The reason for this difference is that in Materialize SQL statements can
//! depend on external state: local files, Confluent Schema Registries, etc.
//!
//! Presently only `CREATE SOURCE` statements can depend on external state,
//! though this could change in the future. Consider, for example:
//!
//! ```sql
//! CREATE SOURCE ... FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://csr:8081'
//! ```
//!
//! The shape of the created source is dependent on the Avro schema that is
//! stored in the schema registry running at `csr:8081`.
//!
//! This is problematic, because we need planning to be a pure function of its
//! input. Why?
//!
//!   * Planning locks the catalog while it operates. Therefore it needs to be
//!     fast, because only one SQL query can be planned at a time. Depending on
//!     external state while holding a lock on the catalog would be seriously
//!     detrimental to the latency of other queries running on the system.
//!
//!   * The catalog persists SQL ASTs across restarts of Materialize. If those
//!     ASTs depend on external state, then changes to that external state could
//!     corrupt Materialize's catalog.
//!
//! Purification is the escape hatch. It is a transformation from SQL AST to SQL
//! AST that "inlines" any external state. For example, we purify the schema
//! above by fetching the schema from the schema registry and inlining it.
//!
//! ```sql
//! CREATE SOURCE ... FORMAT AVRO USING SCHEMA '{"name": "foo", "fields": [...]}'
//! ```
//!
//! Importantly, purification cannot hold its reference to the catalog across an
//! await point. That means it can run in its own Tokio task so that it does not
//! block any other SQL commands on the server.
//!
//! [`Plan`]: crate::plan::Plan

#![warn(missing_debug_implementations)]

macro_rules! bail_unsupported {
    ($feature:expr) => {
        return Err(crate::plan::error::PlanError::Unsupported {
            feature: $feature.to_string(),
            issue_no: None,
        }
        .into())
    };
    ($issue:expr, $feature:expr) => {
        return Err(crate::plan::error::PlanError::Unsupported {
            feature: $feature.to_string(),
            issue_no: Some($issue),
        }
        .into())
    };
}

// TODO(benesch): delete these macros once we use structured errors everywhere.
macro_rules! sql_bail {
    ($($e:expr),* $(,)?) => {
        return Err(sql_err!($($e),*))
    }
}
macro_rules! sql_err {
    ($($e:expr),* $(,)?) => {
        crate::plan::error::PlanError::Unstructured(format!($($e),*))
    }
}

pub const DEFAULT_SCHEMA: &str = "public";

pub mod ast;
pub mod catalog;
pub mod func;
pub mod kafka_util;
pub mod names;
#[macro_use]
pub mod normalize;
pub mod parse;
pub mod plan;
pub mod pure;
pub mod query_model;
