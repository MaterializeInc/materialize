//! LSP server entry point.
//!
//! Sets up the `tower-lsp` service with [`Backend`] and runs the server
//! over stdin/stdout.

use crate::cli::CliError;
use std::path::PathBuf;
use tower_lsp::{LspService, Server};

use super::server::Backend;

/// Run the LSP server over stdio.
///
/// This is the entry point called from the `lsp` subcommand. It blocks until
/// the client disconnects.
pub async fn run(root: PathBuf) -> Result<(), CliError> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::build(|client| Backend::new_with_root(client, root))
        .custom_method("mz-deploy/dag", Backend::dag)
        .custom_method("mz-deploy/catalog", Backend::catalog)
        .custom_method("mz-deploy/keywords", Backend::keywords)
        .finish();

    Server::new(stdin, stdout, socket).serve(service).await;
    Ok(())
}
