// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

#[cfg(test)]
mod tests {

    use mz_lsp_server::backend::DEFAULT_FORMATTING_WIDTH;
    use mz_lsp_server::{PKG_NAME, PKG_VERSION};
    use mz_ore::collections::HashMap;
    use once_cell::sync::Lazy;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::env::temp_dir;
    use std::fmt::Debug;
    use std::fs;
    use std::path::PathBuf;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::Mutex;
    use tower_lsp::jsonrpc::Error;
    use tower_lsp::lsp_types::*;
    use tower_lsp::{lsp_types::InitializeResult, LspService, Server};

    /// This structure defines the message received from the
    /// [Backend](mz_lsp::backend::Backend).
    ///
    /// The `params` and `results` are difficult to parse and compare.
    /// So the best way to ensure everything is as expected is to write
    /// them using [tower_lsp::lsp_types] and then using the `json!()`
    /// macro.
    ///
    /// This way provides a safe way to handle the types and a simple
    /// way to test the response is the expected.
    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct LspMessage<T, R> {
        jsonrpc: String,
        method: Option<String>,
        params: Option<T>,
        result: Option<R>,
        error: Option<Error>,
        id: Option<i32>,
    }

    /// The file path used during the tests is where the SQL code resides.
    const FILE_PATH: Lazy<PathBuf> = Lazy::new(|| temp_dir().join("foo.sql"));
    /// The SQL code written inside [FILE_PATH].
    const FILE_SQL_CONTENT: Lazy<String> = Lazy::new(|| "SELECT \t\t\t200, 200;".to_string());

    /// Tests the different capabilities of [Backend](mz_lsp::backend::Backend)
    ///
    /// Each capability tested is inside it's own function. To test a capability
    /// a request and the expected response must be written and the function must
    /// assert that both are ok.
    ///
    /// The idea is to only write the request, and response and use `[write_and_assert]`
    /// to do the rest.
    ///
    /// The server must always initialize before parsing or using any other capability.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `pipe2` on OS `linux`
    async fn test_lsp() {
        build_file();
        let (mut req_client, mut resp_client) = start_server();
        test_initialize(&mut req_client, &mut resp_client).await;
        // Test a simple query
        test_query(
            &FILE_SQL_CONTENT,
            Some(vec![]),
            &mut req_client,
            &mut resp_client,
        )
        .await;
        test_formatting(&mut req_client, &mut resp_client).await;
        test_simple_query(&mut req_client, &mut resp_client).await;
        test_jinja_query(&mut req_client, &mut resp_client).await;
    }

    /// Builds the file containing a simple query
    fn build_file() {
        fs::write(FILE_PATH.clone(), FILE_SQL_CONTENT.clone()).unwrap();
    }

    /// Returns the file path as String.
    fn get_file_uri() -> String {
        format!("file://{}", FILE_PATH.clone().display())
    }

    async fn test_jinja_query(req_client: &mut DuplexStream, resp_client: &mut DuplexStream) {
        // This is not Jinja code but it is a tricky case.
        let query = r#"
            SELECT '{{ col }}';
            -- This is a SQL comment, containing Jinja delimiters {# source #}
            SELECT a as "{% column %}";
        "#;
        test_query(query, Some(vec![]), req_client, resp_client).await;

        // This is Jinja code and should not be parsed..
        let query = "CREATE TABLE {{ model }} (A INT);";
        test_query(query, None, req_client, resp_client).await;

        let query = r#"
            {% if True %}
                SELECT 100;
            {% endif %}
        "#;
        test_query(query, None, req_client, resp_client).await;

        let query = r#"
            {%+ if True %}
                SELECT 100;
            {% endif %}
        "#;
        test_query(query, None, req_client, resp_client).await;
    }

    /// Asserts that the server can parse a single SQL statement `SELECT 100;`.
    async fn test_simple_query(req_client: &mut DuplexStream, resp_client: &mut DuplexStream) {
        test_query(&FILE_SQL_CONTENT, Some(vec![]), req_client, resp_client).await;
    }

    /// Asserts that the server initialize correctly.
    ///
    /// Attention:
    /// Every time a new capability to the server is added,
    /// the response for this test will change.
    async fn test_initialize(req_client: &mut DuplexStream, resp_client: &mut DuplexStream) {
        let request = r#"{
            "jsonrpc":"2.0",
            "method":"initialize",
            "params":{
                "capabilities":{
                    "textDocumentSync": 1,
                    "documentFormattingProvider": 1
                }
            },
            "id":1
        }"#;
        let expected_response: Vec<LspMessage<bool, InitializeResult>> = vec![LspMessage {
            jsonrpc: "2.0".to_string(),
            error: None,
            method: None,
            params: None,
            result: Some(InitializeResult {
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
                    workspace: Some(WorkspaceServerCapabilities {
                        workspace_folders: Some(WorkspaceFoldersServerCapabilities {
                            supported: Some(true),
                            change_notifications: Some(OneOf::Left(true)),
                        }),
                        file_operations: None,
                    }),
                    ..ServerCapabilities::default()
                },
            }),
            id: Some(1),
        }];

        write_and_assert(
            req_client,
            resp_client,
            &mut [0; 1024],
            request,
            expected_response,
        )
        .await;
    }

    /// Writes a request to the LSP server and asserts that the expected output
    /// message is ok, otherwise it will fail.
    async fn write_and_assert<'de, T, R>(
        req_client: &mut DuplexStream,
        resp_client: &mut DuplexStream,
        buf: &'de mut [u8],
        input_message: &str,
        expected_output_message: Vec<LspMessage<T, R>>,
    ) where
        T: Debug + Deserialize<'de> + PartialEq + ToOwned + Clone,
        R: Debug + Deserialize<'de> + PartialEq + ToOwned + Clone,
    {
        req_client
            .write_all(req(input_message).as_bytes())
            .await
            .unwrap();

        let n = resp_client.read(buf).await.unwrap();
        let buf_as = std::str::from_utf8(&buf[..n]).unwrap();

        let messages = parse_response::<T, R>(buf_as);
        assert_eq!(messages, expected_output_message)
    }

    async fn test_formatting(req_client: &mut DuplexStream, resp_client: &mut DuplexStream) {
        let request = format!(
            r#"{{
            "jsonrpc":"2.0",
            "id": 2,
            "method":"textDocument/formatting",
            "params": {{
                "options": {{
                    "tabSize": 1,
                    "insertSpaces": true
                }},
                "textDocument": {{
                    "uri": "{}"
                }}
            }}
        }}"#,
            get_file_uri()
        );

        let formatting_response: Vec<LspMessage<serde_json::Value, Vec<TextEdit>>> =
            vec![LspMessage {
                jsonrpc: "2.0".to_string(),
                id: Some(2),
                method: None,
                params: None,
                result: Some(vec![TextEdit {
                    range: Range {
                        end: Position {
                            line: 0,
                            character: 19,
                        },
                        start: Position {
                            line: 0,
                            character: 0,
                        },
                    },
                    new_text: "SELECT 200, 200;".to_string(),
                }]),
                error: None,
            }];

        write_and_assert(
            req_client,
            resp_client,
            &mut [0; 1024],
            &request,
            formatting_response,
        )
        .await;
    }

    /// A utility function to test a query and assert
    /// that the diagnostic is as expected.
    async fn test_query(
        query: &str,
        diagnostics: Option<Vec<Diagnostic>>,
        req_client: &mut DuplexStream,
        resp_client: &mut DuplexStream,
    ) {
        // This is not Jinja code but it is a tricky case.
        let (request, expected_response) = build_query_and_diagnostics(query, diagnostics);
        write_and_assert(
            req_client,
            resp_client,
            &mut [0; 1024],
            request.as_str(),
            expected_response,
        )
        .await;
    }

    /// Parses the response from the server.
    ///
    /// The server can return multiple responses in a single transaction.
    /// Each response contains a `Content-Length` header with its size,
    /// and it is followed by the content, containing a single [LspMessage].
    fn parse_response<'de, T, R>(response: &'de str) -> Vec<LspMessage<T, R>>
    where
        T: Debug + Deserialize<'de> + PartialEq + ToOwned + Clone,
        R: Debug + Deserialize<'de> + PartialEq + ToOwned + Clone,
    {
        let mut messages: Vec<LspMessage<T, R>> = Vec::new();
        let mut slices = response.as_bytes();

        while !slices.is_empty() {
            // Parse headers
            let mut dst = [httparse::EMPTY_HEADER; 2];
            let (headers_len, _) = match httparse::parse_headers(slices, &mut dst).unwrap() {
                httparse::Status::Complete(output) => output,
                httparse::Status::Partial => panic!("Partial headers"),
            };

            // Extract content length
            let content_length = dst
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case("Content-Length"))
                .and_then(|header| std::str::from_utf8(header.value).ok())
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap();

            // Extract the message body using content length
            let str_slice: &str =
                std::str::from_utf8(&slices[headers_len..headers_len + content_length]).unwrap();
            messages.push(serde_json::from_str::<LspMessage<T, R>>(str_slice).unwrap());

            // Move the slice pointer past the current message (header + content length)
            slices = &slices[headers_len + content_length..];
        }

        messages
    }

    /// Starts the [Backend](mz_lsp::backend::Backend) in an [LspService].
    /// Returns the two clients to send and read request to and from the
    /// server.
    fn start_server() -> (tokio::io::DuplexStream, tokio::io::DuplexStream) {
        let (req_client, req_server) = tokio::io::duplex(1024);
        let (resp_server, resp_client) = tokio::io::duplex(1024);

        let (service, socket) = LspService::new(|client| mz_lsp_server::backend::Backend {
            client,
            parse_results: Mutex::new(HashMap::new()),
            formatting_width: DEFAULT_FORMATTING_WIDTH.into(),
        });

        mz_ore::task::spawn(
            || format!("taskname:{}", "lsp_server"),
            Server::new(req_server, resp_server, socket).serve(service),
        );

        (req_client, resp_client)
    }

    /// Appends a message's content length header.
    fn req(msg: &str) -> String {
        format!("Content-Length: {}\r\n\r\n{}", msg.len(), msg)
    }

    /// Returns a valid log message structure
    fn build_log_message(message: &str) -> LspMessage<serde_json::Value, String> {
        LspMessage {
            jsonrpc: "2.0".to_string(),
            method: Some("window/logMessage".to_string()),
            params: Some(json!(json!(LogMessageParams {
                message: message.to_string(),
                typ: MessageType::INFO
            }))),
            error: None,
            result: None,
            id: None,
        }
    }

    /// Returns the action of open a new SQL file
    fn build_query_and_diagnostics(
        sql: &str,
        diagnostics: Option<Vec<Diagnostic>>,
    ) -> (String, Vec<LspMessage<serde_json::Value, String>>) {
        let did_open_message = json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didOpen",
            "params": {
                "textDocument": {
                    "uri": get_file_uri(),
                    "languageId": "sql",
                    "version": 1,
                    "text": sql
                }
            }
        })
        .to_string();

        let mut did_open_response: Vec<LspMessage<serde_json::Value, String>> = vec![
            build_log_message("file opened!"),
            build_log_message(&format!(
                r#"on_change Url {{ scheme: "file", cannot_be_a_base: false, username: "", password: None, host: None, port: None, path: {:?}, query: None, fragment: None }}"#,
                FILE_PATH.clone().display()
            )),
        ];

        if let Some(diagnostics) = diagnostics {
            did_open_response.push(LspMessage {
                jsonrpc: "2.0".to_string(),
                method: Some("textDocument/publishDiagnostics".to_string()),
                params: Some(json!(json!(PublishDiagnosticsParams {
                    uri: get_file_uri().parse().unwrap(),
                    diagnostics,
                    version: Some(1),
                }))),
                error: None,
                result: None,
                id: None,
            });
        }

        (did_open_message, did_open_response)
    }
}
