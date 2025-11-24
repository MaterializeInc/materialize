//! Command implementations for mz-deploy CLI.
//!
//! Each command is implemented in its own module with a consistent
//! `run()` function signature that returns `Result<T, CliError>`.

pub mod abort;
pub mod apply;
pub mod compile;
pub mod debug;
pub mod gen_data_contracts;
pub mod stage;
pub mod swap;
pub mod test;
