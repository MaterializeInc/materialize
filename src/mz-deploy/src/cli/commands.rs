// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Command implementations for the mz-deploy CLI.
//!
//! Each subcommand lives in its own module and exposes a `run()` entry point
//! that returns `Result<T, CliError>`. The [`executor`](super::executor) module
//! dispatches to these functions after setting up configuration and connections.
//!
//! ## Commands
//!
//! - **[`new_project`]** — Scaffold a new mz-deploy project directory.
//! - **[`compile`]** — Parse, validate, and type-check the project locally.
//! - **[`explain`]** — Show the EXPLAIN plan for a materialized view or index.
//! - **[`stage`]** — Deploy the project to a staging environment.
//! - **[`wait`]** — Check hydration status of a staged deployment.
//! - **[`promote`]** — Promote a staged deployment to production.
//! - **[`apply_all`]** — Orchestrate all infrastructure apply steps.
//! - **[`abort`]** — Roll back a staged deployment.
//! - **[`apply_sources`]** — Create sources that don't exist.
//! - **[`apply_tables`]** — Create tables that don't exist.
//! - **[`lock`]** — Generate or refresh the `types.lock` file from
//!   the live region.
//! - **[`describe`]** — Print a summary of the compiled project.
//! - **[`debug`]** — Dump internal state for troubleshooting.
//! - **[`sql`]** — Launch an interactive psql session using the active profile.
//! - **[`mcp`]** — Proxy stdio JSON-RPC to the developer MCP HTTP endpoint.
//! - **[`list`]** — List active deployments.
//! - **[`setup`]** — Initialize deployment tracking infrastructure.
//! - **[`log`]** — Show deployment history.
//! - **[`clusters`]** — List or inspect cluster definitions.
//! - **[`roles`]** — List or inspect role definitions.
//! - **[`apply_network_policies`]** — Apply network policy definitions.
//! - **`test`** — Run SQL unit tests against cached type information.
//!
//! ## Shared Types
//!
//! - [`ObjectRef`] — A `(ObjectId, &DatabaseObject)` pair used as the canonical
//!   unit of work when iterating over objects in dependency order.

use crate::project::ir::compiled::DatabaseObject;
use crate::project::ir::object_id::ObjectId;

/// Fully-qualified object identity paired with its typed SQL representation.
///
/// Used across command modules as the canonical unit of work when iterating
/// over objects in dependency order.
pub type ObjectRef<'a> = (ObjectId, &'a DatabaseObject);

pub mod abort;
pub mod apply_all;
pub mod apply_connections;
pub mod apply_network_policies;
pub mod apply_objects;
pub mod apply_secrets;
pub mod apply_sources;
pub mod apply_tables;
pub mod clean;
pub mod clusters;
pub mod compile;
pub mod debug;
pub mod delete;
pub mod describe;
pub mod dev;
pub mod explain;
pub mod grants;
pub mod list;
pub mod lock;
pub mod log;
pub mod mcp;
pub mod new_project;
pub mod profile;
pub mod promote;
pub mod roles;
pub mod setup;
mod setup_schema;
pub mod sql;
pub mod stage;
pub mod test;
pub mod wait;
