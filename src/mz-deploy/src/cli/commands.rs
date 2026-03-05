//! Command implementations for the mz-deploy CLI.
//!
//! Each subcommand lives in its own module and exposes a `run()` entry point
//! that returns `Result<T, CliError>`. The [`executor`](super::executor) module
//! dispatches to these functions after setting up configuration and connections.
//!
//! ## Commands
//!
//! - **[`new_project`]** — Scaffold a new mz-deploy project directory.
//! - **[`compile`]** — Parse and validate the project, optionally type-checking
//!   against a Docker container.
//! - **[`stage`]** — Deploy the project to a staging environment using
//!   blue/green schemas.
//! - **[`ready`]** — Check hydration status of a staged deployment.
//! - **[`apply`]** — Cut over a staged deployment to become the live environment.
//! - **[`abort`]** — Roll back a staged deployment.
//! - **[`create_tables`]** — Create tables defined in the project on the target
//!   region.
//! - **[`gen_data_contracts`]** — Generate or refresh the `types.lock` file from
//!   the live region.
//! - **[`describe`]** — Print a summary of the compiled project.
//! - **[`debug`]** — Dump internal state for troubleshooting.
//! - **[`deployments`]** — List active deployments.
//! - **[`history`]** — Show deployment history.
//! - **[`clusters`]** — List or inspect cluster definitions.
//! - **[`roles`]** — List or inspect role definitions.
//! - **[`test`]** — Run SQL unit tests against cached type information.
//!
//! ## Shared Types
//!
//! - [`ObjectRef`] — A `(ObjectId, &DatabaseObject)` pair used as the canonical
//!   unit of work when iterating over objects in dependency order.

use crate::project;

/// Fully-qualified object identity paired with its typed SQL representation.
///
/// Used across command modules as the canonical unit of work when iterating
/// over objects in dependency order.
pub type ObjectRef<'a> = (
    project::object_id::ObjectId,
    &'a project::typed::DatabaseObject,
);

pub mod abort;
pub mod apply;
pub mod clusters;
pub mod compile;
pub mod create_tables;
pub mod debug;
pub mod deployments;
pub mod describe;
pub mod gen_data_contracts;
pub mod history;
pub mod new_project;
pub mod ready;
pub mod roles;
pub mod stage;
pub mod test;
