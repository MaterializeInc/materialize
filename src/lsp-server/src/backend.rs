// Copyright (c) 2023 Eyal Kalderon
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the tower-lsp project. The original source
// code was retrieved on 10/02/2023 from:
//
//     https://github.com/ebkalderon/tower-lsp/blob/cc4c858/examples/stdio.rs
//
// The original source code is subject to the terms of the <APACHE|MIT> license, a copy
// of which can be found in the LICENSE file at the root of this repository.

use regex::Regex;
use ropey::Rope;
use serde_json::Value;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

use crate::{PKG_NAME, PKG_VERSION};

/// The [Backend] struct implements the [LanguageServer] trait, and thus must provide implementations for its methods.
/// Most imporant methods includes:
/// - `initialize`: sets up the server.
/// - `did_open`: logs when a file is opened and triggers an `on_change` method.
/// - `did_save`, `did_close`: log messages indicating file actions.
/// - `completion`: Provides completion suggestions. WIP.
/// - `code_lens`: Offers in-editor commands. WIP.
///
/// Most of the `did_` methods re-route the request to the private method `on_change`
/// within the `Backend` struct. This method is triggered whenever there's a change
/// in the file, and it parses the content using `mz_sql_parser`.
/// Depending on the parse result, it either sneds the logs the results or any encountered errors.
#[derive(Debug)]
pub struct Backend {
    /// Handles the communication to the client.
    /// Logs and results must be sent through
    /// the client at the end of each capability.
    pub client: Client,
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            server_info: Some(ServerInfo {
                name: PKG_NAME.clone(),
                version: Some(PKG_VERSION.clone()),
            }),
            offset_encoding: None,
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                workspace: Some(WorkspaceServerCapabilities {
                    workspace_folders: Some(WorkspaceFoldersServerCapabilities {
                        supported: Some(true),
                        change_notifications: Some(OneOf::Left(true)),
                    }),
                    file_operations: None,
                }),
                ..ServerCapabilities::default()
            },
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "initialized!")
            .await;
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_change_workspace_folders(&self, _: DidChangeWorkspaceFoldersParams) {
        self.client
            .log_message(MessageType::INFO, "workspace folders changed!")
            .await;
    }

    async fn did_change_configuration(&self, _: DidChangeConfigurationParams) {
        self.client
            .log_message(MessageType::INFO, "configuration changed!")
            .await;
    }

    async fn did_change_watched_files(&self, _: DidChangeWatchedFilesParams) {
        self.client
            .log_message(MessageType::INFO, "watched files have changed!")
            .await;
    }

    async fn execute_command(&self, _: ExecuteCommandParams) -> Result<Option<Value>> {
        self.client
            .log_message(MessageType::INFO, "command executed!")
            .await;

        match self.client.apply_edit(WorkspaceEdit::default()).await {
            Ok(res) if res.applied => self.client.log_message(MessageType::INFO, "applied").await,
            Ok(_) => self.client.log_message(MessageType::INFO, "rejected").await,
            Err(err) => self.client.log_message(MessageType::ERROR, err).await,
        }

        Ok(None)
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        self.client
            .log_message(MessageType::INFO, "file opened!")
            .await;

        self.parse(TextDocumentItem {
            uri: params.text_document.uri,
            text: params.text_document.text,
            version: params.text_document.version,
        })
        .await
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        self.client
            .log_message(MessageType::INFO, "file changed!")
            .await;

        self.parse(TextDocumentItem {
            uri: params.text_document.uri,
            text: params.content_changes[0].text.clone(),
            version: params.text_document.version,
        })
        .await
    }

    async fn did_save(&self, _: DidSaveTextDocumentParams) {
        self.client
            .log_message(MessageType::INFO, "file saved!")
            .await;
    }

    async fn did_close(&self, _: DidCloseTextDocumentParams) {
        self.client
            .log_message(MessageType::INFO, "file closed!")
            .await;
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let _uri = params.text_document_position.text_document.uri;
        let _position = params.text_document_position.position;

        // TODO: Re enable when position is correct.
        // Ok(completions.map(CompletionResponse::Array))
        Ok(None)
    }

    async fn code_lens(&self, _params: CodeLensParams) -> Result<Option<Vec<CodeLens>>> {
        let _lenses: Vec<CodeLens> = vec![CodeLens {
            range: Range {
                start: Position::new(0, 0),
                end: Position::new(0, 0),
            },
            command: Some(Command {
                title: "Run".to_string(),
                command: "materialize.run".to_string(),
                arguments: None,
            }),
            data: None,
        }];

        // TODO: Re enable when position is correct.
        // Ok(Some(lenses))
        Ok(None)
    }
}

struct TextDocumentItem {
    uri: Url,
    text: String,
    version: i32,
}

impl Backend {
    /// Parses the SQL code and publishes diagnosis about it.
    async fn parse(&self, params: TextDocumentItem) {
        self.client
            .log_message(MessageType::INFO, format!("on_change {:?}", params.uri))
            .await;
        let rope = ropey::Rope::from_str(&params.text);

        // Parse the text
        let parse_result = mz_sql_parser::parser::parse_statements(&params.text);

        match parse_result {
            // The parser will return Ok when everything is well written.
            Ok(_results) => {
                // Clear the diagnostics in case there were issues before.
                self.client
                    .publish_diagnostics(params.uri.clone(), vec![], Some(params.version))
                    .await;
            }

            // If there is at least one error the parser will return Err.
            Err(err_parsing) => {
                let error_position = err_parsing.error.pos;
                let start = offset_to_position(error_position, &rope).unwrap();
                let end = start;
                let range = Range { start, end };

                // Check for Jinja code (dbt)
                // If Jinja code is detected, inform that parsing is not available..
                if self.is_jinja(&err_parsing.error.message, params.text) {
                    self.client
                        .publish_diagnostics(
                            params.uri.clone(),
                            vec![Diagnostic::new_with_code_number(
                                range,
                                DiagnosticSeverity::INFORMATION,
                                1,
                                None,
                                "Jinja code will not be parsed by the Materialize extension."
                                    .to_string(),
                            )],
                            Some(params.version),
                        )
                        .await;
                    return;
                }

                let diagnostics = Diagnostic::new_simple(range, err_parsing.error.message);

                self.client
                    .publish_diagnostics(
                        params.uri.clone(),
                        vec![diagnostics],
                        Some(params.version),
                    )
                    .await;
            }
        }
    }

    /// Detects if the code contains Jinja code using RegEx and
    /// looks for Jinja's delimiters:
    /// - {% ... %} for Statements
    /// - {{ ... }} for Expressions to print to the template output
    /// - {# ... #} for Comments not included in the template output
    ///
    /// Reference: (https://jinja.palletsprojects.com/en/3.0.x/templates/#synopsis)
    ///
    /// The trade-off is that the regex is simple, but it may detect some code as Jinja
    /// when it is not actually Jinja. For example: `SELECT '{{ 100 }}';`.
    /// To handle such cases more successfully, the parser will attempt to parse the
    /// file, and if it fails, it will then check if it contains Jinja code.
    fn contains_jinja_code(&self, s: &str) -> bool {
        let re = Regex::new(r"\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\}").unwrap();
        re.is_match(s)
    }

    /// Returns true if Jinja code is detected.
    fn is_jinja(&self, s: &str, code: String) -> bool {
        s == "unexpected character in input: {" && self.contains_jinja_code(&code)
    }
}

/// This functions is a helper function that converts an offset in the file to a (line, column).
///
/// It is useful when translating an ofsset returned by [mz_sql_parser::parser::parse_statements]
/// to an (x,y) position in the text to represent the error in the correct token.
fn offset_to_position(offset: usize, rope: &Rope) -> Option<Position> {
    let line = rope.try_char_to_line(offset).ok()?;
    let first_char_of_line = rope.try_line_to_char(line).ok()?;
    let column = offset - first_char_of_line;

    // Convert to u32.
    let line_u32 = line.try_into().ok()?;
    let column_u32 = column.try_into().ok()?;

    Some(Position::new(line_u32, column_u32))
}
