//! CLI-specific functionality and error types.

pub mod commands;
mod error;
pub mod helpers;

pub use error::CliError;

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
