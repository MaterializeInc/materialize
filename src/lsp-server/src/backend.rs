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

use ::serde::Deserialize;
use mz_ore::collections::HashMap;
use mz_sql_lexer::keywords::Keyword;
use mz_sql_lexer::lexer::{self, Token};
use mz_sql_parser::ast::{statement_kind_label_value, Raw, Statement};
use mz_sql_parser::parser::parse_statements;
use regex::Regex;
use ropey::Rope;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tower_lsp::jsonrpc::{Error, ErrorCode, Result};
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

use crate::{PKG_NAME, PKG_VERSION};

/// Default formatting width to use in the [LanguageServer::formatting] implementation.
pub const DEFAULT_FORMATTING_WIDTH: usize = 100;

/// This is a re-implemention of [mz_sql_parser::parser::StatementParseResult]
/// but replacing the sql code with a rope.
#[derive(Debug)]
pub struct ParseResult {
    /// Abstract Syntax Trees (AST) for each of the SQL statements
    /// in a file.
    pub asts: Vec<Statement<Raw>>,
    /// Text handler for big files.
    pub rope: Rope,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
/// Represents the structure a client uses to understand
/// statement's kind and sql content.
pub struct ExecuteCommandParseStatement {
    /// The sql content in the statement
    pub sql: String,
    /// The type of statement.
    /// Represents the String version of [Statement].
    pub kind: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
/// Represents the response from the parse command.
pub struct ExecuteCommandParseResponse {
    /// Contains all the valid SQL statements.
    pub statements: Vec<ExecuteCommandParseStatement>,
}

/// Represents the completion items that will
/// be returned to the client when requested.
#[derive(Debug)]
pub struct Completions {
    /// Contains the completion items
    /// after a SELECT token.
    pub select: Vec<CompletionItem>,
    /// Contains the completion items for
    /// after a FROM token.
    pub from: Vec<CompletionItem>,
}

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

    /// Contains parsing results for each open file.
    /// Instead of retrieving the last version from the file
    /// each time a command, like formatting, is executed,
    /// we use the most recent parsing results stored here.
    /// Reading from the file would access old content.
    /// E.g. The user formats or performs an action
    /// prior to save the file.
    pub parse_results: Mutex<HashMap<Url, ParseResult>>,

    /// Contains the latest content for each file.
    pub content: Mutex<HashMap<Url, Rope>>,

    /// Formatting width to use in mz- prettier
    pub formatting_width: Mutex<usize>,

    /// Schema available in the client
    /// used for completion suggestions.
    pub schema: Mutex<Option<Schema>>,

    /// Completion suggestion to return
    /// to the client when requested.
    pub completions: Mutex<Completions>,
}

/// Represents a column from an [ObjectType
#[derive(Debug, Clone, Deserialize)]
pub struct SchemaObjectColumn {
    /// Represents the column's name.
    pub name: String,
    /// Represents the column's type.
    #[serde(rename = "type")]
    pub typ: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
/// Represents each possible object type admissible by the LSP.
pub enum ObjectType {
    /// Represents a materialized view.
    MaterializedView,
    /// Represents a view.
    View,
    /// Represents a source.
    Source,
    /// Represents a table.
    Table,
    /// Represents a sink.
    Sink,
}

impl std::fmt::Display for ObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectType::MaterializedView => write!(f, "Materialized View"),
            ObjectType::View => write!(f, "View"),
            ObjectType::Source => write!(f, "Source"),
            ObjectType::Table => write!(f, "Table"),
            ObjectType::Sink => write!(f, "Sink"),
        }
    }
}

/// Represents a Materialize object present in the schema,
/// and its columns.
///
/// E.g. a table, view, source, etc.
#[derive(Debug, Clone, Deserialize)]
pub struct SchemaObject {
    /// Represents the object type.
    #[serde(rename = "type")]
    pub typ: ObjectType,
    /// Represents the object name.
    pub name: String,
    /// Contains all the columns available in the object.
    pub columns: Vec<SchemaObjectColumn>,
}

/// Represents the current schema, database and all
/// its objects the client is using.
///
/// This is later used to return completion items to the client.
#[derive(Debug, Clone, Deserialize)]
pub struct Schema {
    /// Represents the schema name.
    pub schema: String,
    /// Represents the database name.
    pub database: String,
    /// Contains all the user objects (tables, views, sources, etc.)
    /// available in the current database/schema.
    pub objects: Vec<SchemaObject>,
}

/// Contains customizable options send by the client.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeOptions {
    /// Represents the width used to format text using [mz_sql_pretty].
    pub formatting_width: Option<usize>,
    /// Represents the current schema available in the client.
    pub schema: Option<Schema>,
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        // Load the formatting width and schema option sent by the client.
        if let Some(value_options) = params.initialization_options {
            match serde_json::from_value(value_options) {
                Ok(options) => {
                    let options: InitializeOptions = options;
                    if let Some(formatting_width) = options.formatting_width {
                        let mut formatting_width_guard = self.formatting_width.lock().await;
                        *formatting_width_guard = formatting_width;
                    }

                    if let Some(schema) = options.schema {
                        let mut schema_guard = self.schema.lock().await;
                        *schema_guard = Some(schema.clone());
                        let mut completions = self.completions.lock().await;
                        *completions = self.build_completion_items(schema);
                    };
                }
                Err(err) => {
                    self.client
                        .log_message(
                            MessageType::INFO,
                            format!("Initialization options are erroneus: {:?}", err.to_string()),
                        )
                        .await;
                }
            };
        }

        Ok(InitializeResult {
            server_info: Some(ServerInfo {
                name: PKG_NAME.clone(),
                version: Some(PKG_VERSION.clone()),
            }),
            offset_encoding: None,
            capabilities: ServerCapabilities {
                document_formatting_provider: Some(tower_lsp::lsp_types::OneOf::Left(true)),
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                execute_command_provider: Some(ExecuteCommandOptions {
                    commands: vec!["parse".to_string()],
                    work_done_progress_options: WorkDoneProgressOptions {
                        work_done_progress: None,
                    },
                }),
                completion_provider: Some(CompletionOptions {
                    resolve_provider: Some(false),
                    trigger_characters: Some(vec![".".to_string()]),
                    work_done_progress_options: Default::default(),
                    all_commit_characters: None,
                    completion_item: None,
                }),
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

    /// Executes a single command and returns the response. Def: [workspace/executeCommand](https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#workspace_executeCommand)
    ///
    /// Commands implemented:
    ///
    /// * parse: returns multiple valid statements from a single sql code.
    async fn execute_command(&self, command_params: ExecuteCommandParams) -> Result<Option<Value>> {
        match command_params.command.as_str() {
            "parse" => {
                let json_args = command_params.arguments.get(0);

                if let Some(json_args) = json_args {
                    let args = serde_json::from_value::<String>(json_args.clone())
                        .map_err(|_| build_error("Error deserializing parse args as String."))?;
                    let statements = parse_statements(&args)
                        .map_err(|_| build_error("Error parsing the statements."))?;

                    // Transform raw statements to splitted statements
                    // and infere the kind.
                    // E.g. if it is a select or a create_table statement.
                    let parse_statements: Vec<ExecuteCommandParseStatement> = statements
                        .iter()
                        .map(|x| ExecuteCommandParseStatement {
                            kind: statement_kind_label_value(x.ast.clone().into()).to_string(),
                            sql: x.sql.to_string(),
                        })
                        .collect();

                    return Ok(Some(json!(ExecuteCommandParseResponse {
                        statements: parse_statements
                    })));
                } else {
                    return Err(build_error("Missing command args."));
                }
            }
            "optionsUpdate" => {
                let json_args = command_params.arguments.get(0);

                if let Some(json_args) = json_args {
                    let args = serde_json::from_value::<InitializeOptions>(json_args.clone())
                        .map_err(|_| {
                            build_error("Error deserializing parse args as InitializeOptions.")
                        })?;

                    if let Some(formatting_width) = args.formatting_width {
                        let mut formatting_width_guard = self.formatting_width.lock().await;
                        *formatting_width_guard = formatting_width;
                    }

                    if let Some(schema) = args.schema {
                        let mut schema_guard = self.schema.lock().await;
                        *schema_guard = Some(schema.clone());
                        let mut completions = self.completions.lock().await;
                        *completions = self.build_completion_items(schema);
                    }

                    return Ok(None);
                } else {
                    return Err(build_error("Missing command args."));
                }
            }
            _ => {
                return Err(build_error("Unknown command."));
            }
        }
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

    /// Completion implementation.
    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let uri = params.text_document_position.text_document.uri;
        let position = params.text_document_position.position;

        let content = self.content.lock().await;
        let content = content.get(&uri);

        if let Some(content) = content {
            // Get the lex token.
            let lex_results = lexer::lex(&content.to_string())
                .map_err(|_| build_error("Error getting lex tokens."))?;
            let offset = position_to_offset(position, content)
                .ok_or(build_error("Error getting completion offset."))?;

            let last_keyword = lex_results
                .iter()
                .filter_map(|x| {
                    if x.offset < offset {
                        match x.kind {
                            Token::Keyword(k) => match k {
                                Keyword::Select => Some(k),
                                Keyword::From => Some(k),
                                _ => None,
                            },
                            // Skip the rest for now.
                            _ => None,
                        }
                    } else {
                        None
                    }
                })
                .last();

            if let Some(keyword) = last_keyword {
                return match keyword {
                    Keyword::Select => {
                        let completions = self.completions.lock().await;
                        let select_completions = completions.select.clone();
                        Ok(Some(CompletionResponse::Array(select_completions)))
                    }
                    Keyword::From => {
                        let completions = self.completions.lock().await;
                        let from_completions = completions.from.clone();
                        Ok(Some(CompletionResponse::Array(from_completions)))
                    }
                    _ => Ok(None),
                };
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
    }

    /// Formats the code using [mz_sql_pretty].
    ///
    /// Implements the [`textDocument/formatting`](https://microsoft.github.io/language-server-protocol/specification#textDocument_formatting) language feature.
    async fn formatting(&self, params: DocumentFormattingParams) -> Result<Option<Vec<TextEdit>>> {
        let locked_map = self.parse_results.lock().await;
        let width = self.formatting_width.lock().await;

        if let Some(parse_result) = locked_map.get(&params.text_document.uri) {
            let pretty = parse_result
                .asts
                .iter()
                .map(|ast| mz_sql_pretty::to_pretty(ast, *width))
                .collect::<Vec<String>>()
                .join("\n");
            let rope = &parse_result.rope;

            return Ok(Some(vec![TextEdit {
                new_text: pretty,
                range: Range {
                    // TODO: Remove unwraps.
                    start: offset_to_position(0, rope).unwrap(),
                    end: offset_to_position(rope.len_chars(), rope).unwrap(),
                },
            }]));
        } else {
            return Ok(None);
        }
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

        let mut content = self.content.lock().await;
        let mut parse_results = self.parse_results.lock().await;

        // Parse the text
        let parse_result = mz_sql_parser::parser::parse_statements(&params.text);

        match parse_result {
            // The parser will return Ok when everything is well written.
            Ok(results) => {
                content.insert(params.uri.clone(), rope.clone());

                // Clear the diagnostics in case there were issues before.
                self.client
                    .publish_diagnostics(params.uri.clone(), vec![], Some(params.version))
                    .await;

                let asts = results.iter().map(|x| x.ast.clone()).collect();
                let parse_result: ParseResult = ParseResult { asts, rope };
                parse_results.insert(params.uri, parse_result);
            }

            // If there is at least one error the parser will return Err.
            Err(err_parsing) => {
                let error_position = err_parsing.error.pos;
                let start = offset_to_position(error_position, &rope).unwrap();
                let end = start;
                let range = Range { start, end };

                parse_results.remove(&params.uri);

                // Check for Jinja code (dbt)
                // If Jinja code is detected, inform that parsing is not available..
                if self.is_jinja(&err_parsing.error.message, params.text) {
                    // Do not send any new diagnostics
                    return;
                }

                // Only insert content if it is not Jinja code.
                content.insert(params.uri.clone(), rope.clone());

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
    /// Reference: <https://jinja.palletsprojects.com/en/3.0.x/templates/#synopsis>
    ///
    /// The trade-off is that the regex is simple, but it may detect some code as Jinja
    /// when it is not actually Jinja. For example: `SELECT '{{ 100 }}';`.
    /// To handle such cases more successfully, the server will first attempt to parse the
    /// file, and if it fails, it will then check if it contains Jinja code.
    fn contains_jinja_code(&self, s: &str) -> bool {
        let re = Regex::new(r"\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\}").unwrap();
        re.is_match(s)
    }

    /// Returns true if Jinja code is detected.
    fn is_jinja(&self, s: &str, code: String) -> bool {
        s == "unexpected character in input: {" && self.contains_jinja_code(&code)
    }

    /// Builds the completion items for the following statements:
    ///
    /// * SELECT
    /// * FROM
    ///
    /// Use this function to build the completion items once,
    /// and avoid having to rebuild on every [LanguageServer::completion] call.
    fn build_completion_items(&self, schema: Schema) -> Completions {
        // Build SELECT completion items:
        let mut select_completions = Vec::new();
        let mut from_completions = Vec::new();

        schema.objects.iter().for_each(|object| {
            // Columns
            object.columns.iter().for_each(|column| {
                select_completions.push(CompletionItem {
                    label: column.name.to_string(),
                    label_details: Some(CompletionItemLabelDetails {
                        detail: Some(column.typ.to_string()),
                        description: None,
                    }),
                    kind: Some(CompletionItemKind::FIELD),
                    detail: Some(
                        format!(
                            "From {}.{}.{} ({:?})",
                            schema.database, schema.schema, object.name, object.typ
                        )
                        .to_string(),
                    ),
                    documentation: None,
                    deprecated: Some(false),
                    ..Default::default()
                });
            });

            // Objects
            from_completions.push(CompletionItem {
                label: object.name.to_string(),
                label_details: Some(CompletionItemLabelDetails {
                    detail: Some(object.typ.to_string()),
                    description: None,
                }),
                kind: match object.typ {
                    ObjectType::View => Some(CompletionItemKind::ENUM_MEMBER),
                    ObjectType::MaterializedView => Some(CompletionItemKind::ENUM),
                    ObjectType::Source => Some(CompletionItemKind::CLASS),
                    ObjectType::Sink => Some(CompletionItemKind::CLASS),
                    ObjectType::Table => Some(CompletionItemKind::CONSTANT),
                },
                detail: Some(
                    format!(
                        "Represents {}.{}.{} ({:?})",
                        schema.database, schema.schema, object.name, object.typ
                    )
                    .to_string(),
                ),
                documentation: None,
                deprecated: Some(false),
                ..Default::default()
            });
        });

        Completions {
            from: from_completions,
            select: select_completions,
        }
    }
}

/// This function converts a (line, column) position in the text to an offset in the file.
///
/// It is the inverse of the `offset_to_position` function.
fn position_to_offset(position: Position, rope: &Rope) -> Option<usize> {
    // Convert line and column from u32 back to usize
    let line: usize = position.line.try_into().ok()?;
    let column: usize = position.character.try_into().ok()?;

    // Get the offset of the first character of the line
    let first_char_of_line_offset = rope.try_line_to_char(line).ok()?;

    // Calculate the offset by adding the column number to the first character of the line's offset
    let offset = first_char_of_line_offset + column;

    Some(offset)
}

/// This function is a helper function that converts an offset in the file to a (line, column).
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

/// Builds a [tower_lsp::jsonrpc::Error]
///
/// Use this function to map normal errors to the one the trait expects
fn build_error(message: &'static str) -> tower_lsp::jsonrpc::Error {
    Error {
        code: ErrorCode::InternalError,
        message: std::borrow::Cow::Borrowed(message),
        data: None,
    }
}
