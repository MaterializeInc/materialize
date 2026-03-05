//! Command implementations for mz-deploy CLI.
//!
//! Each command is implemented in its own module with a consistent
//! `run()` function signature that returns `Result<T, CliError>`.

use crate::project;

/// Fully-qualified object identity paired with its typed SQL representation.
///
/// Used across command modules as the canonical unit of work when iterating
/// over objects in dependency order.
pub type ObjectRef<'a> = (project::object_id::ObjectId, &'a project::typed::DatabaseObject);

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
pub mod ready;
pub mod roles;
pub mod stage;
pub mod new_project;
pub mod test;
