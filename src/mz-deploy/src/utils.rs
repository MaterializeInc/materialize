//! Shared utility modules used across the crate.
//!
//! - **[`docker_runtime`]** — Manages ephemeral Materialize Docker containers
//!   for type checking.
//! - **[`git`]** — Git commit hash retrieval and dirty-repo detection.
//! - **[`log`]** — Logging and verbose output macros.
//! - **[`progress`]** — Terminal progress bars and spinners for long-running
//!   operations.
//! - **[`sql_utils`]** — SQL identifier quoting helpers.

pub mod docker_runtime;
pub mod git;
pub mod log;
pub mod progress;
pub mod sql_utils;
