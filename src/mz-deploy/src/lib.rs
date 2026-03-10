//! Safe, testable deployments for Materialize.
//!
//! `mz-deploy` compiles a directory of `.sql` files into a deployment plan,
//! diffs it against the live environment, and executes blue/green schema
//! migrations via Materialize's zero-downtime deployment primitives.
//!
//! ## Architecture
//!
//! The crate is organized into four major layers:
//!
//! - **[`cli`]** — Command-line interface: argument parsing, subcommand dispatch,
//!   and user-facing error formatting.
//! - **[`client`]** — Database client layer: connection management, introspection
//!   queries, DDL provisioning, and deployment operations against a live
//!   Materialize region.
//! - **[`project`]** — Project pipeline: loads `.sql` files from disk, validates
//!   and type-checks them, resolves dependencies, and produces a deployment plan
//!   (see [`project`] for the full `raw → typed → planned` pipeline).
//! - **[`types`]** — Data-contract system: the `types.lock` file that pins column
//!   schemas for external dependencies, plus the [`TypeChecker`](types::TypeChecker)
//!   trait for validating SQL against a real Materialize instance.
//!
//! ## Supporting Modules
//!
//! - **[`log`]** — Verbose logging and the [`verbose!`] macro.
//! - **[`unit_test`]** — In-process test runner that validates SQL unit tests
//!   against cached type information.

pub mod cli;
pub mod client;
pub mod config;
pub mod log;
pub mod output;
pub mod project;
pub mod secret_resolver;
pub mod types;
pub mod unit_test;
