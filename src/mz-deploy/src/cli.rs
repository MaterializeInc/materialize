//! Command-line interface for mz-deploy.
//!
//! This module defines the CLI structure and shared types used across commands.
//!
//! ## Submodules
//!
//! - **[`commands`]** — One module per CLI subcommand (`stage`, `apply`, `compile`, etc.),
//!   each exposing a `run()` entry point.
//! - **[`executor`]** — Orchestrates the full command lifecycle: loads configuration,
//!   establishes database connections, and dispatches to the appropriate command module.
//! - **`error`** — [`CliError`] enum that unifies all user-facing errors with optional
//!   hints, re-exported at this level for convenience.
//!
//! ## Top-level Items
//!
//! - [`TypeCheckMode`] — Controls whether SQL type checking runs against a local
//!   Materialize Docker container.
//! - [`display_error`] — Renders a [`CliError`] to stderr with rustc-style colored
//!   formatting and hint messages.

pub mod commands;
mod error;
pub mod executor;
pub mod extended_help;

pub use error::CliError;

/// Whether to type-check SQL against a Materialize Docker container.
#[derive(Debug, Clone)]
pub enum TypeCheckMode {
    /// Type-check using the given Docker image.
    Enabled { image: String },
    /// Skip type checking.
    Disabled,
}

/// Display a CLI error and exit with status code 1.
///
/// Formats the error using colored output with rustc-style formatting,
/// including any hints provided by the error's `hint()` method.
pub fn display_error(error: &CliError) {
    use owo_colors::OwoColorize;

    eprintln!("{}: {}", "error".bright_red().bold(), error);

    if let Some(hint) = error.hint() {
        eprintln!(
            "  {} {}",
            "=".bright_blue().bold(),
            format!("help: {}", hint).bold()
        );
    }

    std::process::exit(1);
}
