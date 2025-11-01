// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Model Context Protocol (MCP) HTTP handlers.
//!
//! Exposes Materialize data products to AI agents via JSON-RPC 2.0 over HTTP POST.
//!
//! ## Endpoints
//!
//! - `/api/mcp/agents` - User data products for customer AI agents
//! - `/api/mcp/observatory` - System catalog (`mz_*`) for troubleshooting
//!
//! ## Tools
//!
//! **Agents:** `get_data_products`, `get_data_product_details`, `query`
//! **Observatory:** `query_system_catalog`
//!
//! Data products are discovered via `mz_internal.mz_mcp_data_products` system view.

use anyhow::anyhow;
use axum::Json;
use axum::response::IntoResponse;
use http::StatusCode;
use mz_sql::parse::parse;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast::display::escaped_string_literal;
use mz_sql_parser::ast::visit::{self, Visit};
use mz_sql_parser::ast::{Raw, RawItemName};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tracing::{debug, warn};

use crate::http::AuthedClient;
use crate::http::sql::{SqlRequest, SqlResponse, SqlResult, execute_request};

// To add a new tool: add entry to tools/list, add handler function, add dispatch case.
const DISCOVERY_QUERY: &str = "SELECT * FROM mz_internal.mz_mcp_data_products";

/// MCP request errors, mapped to JSON-RPC error codes.
#[derive(Debug, Error)]
enum McpRequestError {
    #[error("Invalid JSON-RPC version: expected 2.0")]
    InvalidJsonRpcVersion,
    #[error("Method not found: {0}")]
    #[allow(dead_code)] // Handled by serde deserialization, kept for error mapping
    MethodNotFound(String),
    #[error("Tool not found: {0}")]
    ToolNotFound(String),
    #[error("Data product not found: {0}")]
    DataProductNotFound(String),
    #[error("Query validation failed: {0}")]
    QueryValidationFailed(String),
    #[error("Query execution failed: {0}")]
    QueryExecutionFailed(String),
    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl McpRequestError {
    fn error_code(&self) -> i32 {
        match self {
            Self::InvalidJsonRpcVersion => error_codes::INVALID_REQUEST,
            Self::MethodNotFound(_) => error_codes::METHOD_NOT_FOUND,
            Self::ToolNotFound(_) => error_codes::INVALID_PARAMS,
            Self::DataProductNotFound(_) => error_codes::INVALID_PARAMS,
            Self::QueryValidationFailed(_) => error_codes::INVALID_PARAMS,
            Self::QueryExecutionFailed(_) | Self::Internal(_) => error_codes::INTERNAL_ERROR,
        }
    }

    fn error_type(&self) -> &'static str {
        match self {
            Self::InvalidJsonRpcVersion => "InvalidRequest",
            Self::MethodNotFound(_) => "MethodNotFound",
            Self::ToolNotFound(_) => "ToolNotFound",
            Self::DataProductNotFound(_) => "DataProductNotFound",
            Self::QueryValidationFailed(_) => "ValidationError",
            Self::QueryExecutionFailed(_) => "ExecutionError",
            Self::Internal(_) => "InternalError",
        }
    }
}

/// JSON-RPC 2.0 request. Requests have `id`; notifications don't.
#[derive(Debug, Deserialize)]
pub(crate) struct McpRequest {
    jsonrpc: String,
    id: Option<serde_json::Value>,
    #[serde(flatten)]
    method: McpMethod,
}

/// MCP method variants with their associated parameters.
#[derive(Debug, Deserialize)]
#[serde(tag = "method", content = "params")]
enum McpMethod {
    /// Initialize method - params accepted but not currently used
    #[serde(rename = "initialize")]
    Initialize(#[allow(dead_code)] InitializeParams),
    #[serde(rename = "tools/list")]
    ToolsList,
    #[serde(rename = "tools/call")]
    ToolsCall(ToolsCallParams),
    /// Catch-all for unknown methods (e.g. `notifications/initialized`)
    #[serde(other)]
    Unknown,
}

impl std::fmt::Display for McpMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            McpMethod::Initialize(_) => write!(f, "initialize"),
            McpMethod::ToolsList => write!(f, "tools/list"),
            McpMethod::ToolsCall(_) => write!(f, "tools/call"),
            McpMethod::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct InitializeParams {
    /// Protocol version from client. Not currently validated but accepted for MCP compliance.
    #[serde(rename = "protocolVersion")]
    #[allow(dead_code)]
    protocol_version: String,
    /// Client capabilities. Not currently used but accepted for MCP compliance.
    #[serde(default)]
    #[allow(dead_code)]
    capabilities: serde_json::Value,
    /// Client information (name, version). Not currently used but accepted for MCP compliance.
    #[serde(rename = "clientInfo")]
    #[allow(dead_code)]
    client_info: Option<ClientInfo>,
}

#[derive(Debug, Deserialize)]
struct ClientInfo {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    version: String,
}

/// Tool call parameters, deserialized via adjacently tagged enum.
/// Serde maps `name` to the variant and `arguments` to the variant's data.
#[derive(Debug, Deserialize)]
#[serde(tag = "name", content = "arguments")]
#[serde(rename_all = "snake_case")]
enum ToolsCallParams {
    // Agents endpoint tools
    // Uses an ignored empty struct so MCP clients sending `"arguments": {}` can deserialize.
    GetDataProducts(#[serde(default)] ()),
    GetDataProductDetails(GetDataProductDetailsParams),
    Query(QueryParams),
    // Observatory endpoint tools
    QuerySystemCatalog(QuerySystemCatalogParams),
}

impl std::fmt::Display for ToolsCallParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ToolsCallParams::GetDataProducts(_) => write!(f, "get_data_products"),
            ToolsCallParams::GetDataProductDetails(_) => write!(f, "get_data_product_details"),
            ToolsCallParams::Query(_) => write!(f, "query"),
            ToolsCallParams::QuerySystemCatalog(_) => write!(f, "query_system_catalog"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct GetDataProductDetailsParams {
    name: String,
}

#[derive(Debug, Deserialize)]
struct QueryParams {
    cluster: String,
    sql_query: String,
}

#[derive(Debug, Deserialize)]
struct QuerySystemCatalogParams {
    sql_query: String,
}

#[derive(Debug, Serialize)]
struct McpResponse {
    jsonrpc: String,
    id: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<McpResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<McpError>,
}

/// Typed MCP response results.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum McpResult {
    Initialize(InitializeResult),
    ToolsList(ToolsListResult),
    ToolContent(ToolContentResult),
}

#[derive(Debug, Serialize)]
struct InitializeResult {
    #[serde(rename = "protocolVersion")]
    protocol_version: String,
    capabilities: Capabilities,
    #[serde(rename = "serverInfo")]
    server_info: ServerInfo,
}

#[derive(Debug, Serialize)]
struct Capabilities {
    tools: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ServerInfo {
    name: String,
    version: String,
}

#[derive(Debug, Serialize)]
struct ToolsListResult {
    tools: Vec<ToolDefinition>,
}

#[derive(Debug, Serialize)]
struct ToolDefinition {
    name: String,
    description: String,
    #[serde(rename = "inputSchema")]
    input_schema: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ToolContentResult {
    content: Vec<ContentBlock>,
}

#[derive(Debug, Serialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    content_type: String,
    text: String,
}

/// JSON-RPC 2.0 error codes.
mod error_codes {
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;
}

#[derive(Debug, Serialize)]
struct McpError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
}

impl From<McpRequestError> for McpError {
    fn from(err: McpRequestError) -> Self {
        McpError {
            code: err.error_code(),
            message: err.to_string(),
            data: Some(json!({
                "error_type": err.error_type(),
            })),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum McpEndpointType {
    Agents,
    Observatory,
}

impl std::fmt::Display for McpEndpointType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            McpEndpointType::Agents => write!(f, "agents"),
            McpEndpointType::Observatory => write!(f, "observatory"),
        }
    }
}

/// Agents endpoint: exposes user data products.
pub async fn handle_mcp_agents(
    client: AuthedClient,
    Json(request): Json<McpRequest>,
) -> impl IntoResponse {
    handle_mcp_request(client, request, McpEndpointType::Agents).await
}

/// Observatory endpoint: exposes system catalog (mz_*) only.
pub async fn handle_mcp_observatory(
    client: AuthedClient,
    Json(request): Json<McpRequest>,
) -> impl IntoResponse {
    handle_mcp_request(client, request, McpEndpointType::Observatory).await
}

async fn handle_mcp_request(
    mut client: AuthedClient,
    request: McpRequest,
    endpoint_type: McpEndpointType,
) -> impl IntoResponse {
    let user = client.client.session().user().name.clone();
    let is_notification = request.id.is_none();

    debug!(
        method = %request.method,
        endpoint = %endpoint_type,
        user = %user,
        is_notification = is_notification,
        "MCP request received"
    );

    // Handle notifications (no response needed)
    if is_notification {
        debug!(method = %request.method, "Received notification (no response will be sent)");
        return StatusCode::OK.into_response();
    }

    // Spawn task for fault isolation
    let response = mz_ore::task::spawn(|| "mcp_request", async move {
        handle_mcp_request_inner(&mut client, request, endpoint_type).await
    })
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

async fn handle_mcp_request_inner(
    client: &mut AuthedClient,
    request: McpRequest,
    endpoint_type: McpEndpointType,
) -> McpResponse {
    // Extract request ID (guaranteed to be Some since notifications are filtered earlier)
    let request_id = request.id.clone().unwrap_or(serde_json::Value::Null);

    let result = handle_mcp_method(client, &request, endpoint_type).await;

    match result {
        Ok(result_value) => McpResponse {
            jsonrpc: "2.0".to_string(),
            id: request_id,
            result: Some(result_value),
            error: None,
        },
        Err(e) => {
            // Log non-trivial errors
            if !matches!(
                e,
                McpRequestError::MethodNotFound(_) | McpRequestError::InvalidJsonRpcVersion
            ) {
                warn!(error = %e, method = %request.method, "MCP method execution failed");
            }
            McpResponse {
                jsonrpc: "2.0".to_string(),
                id: request_id,
                result: None,
                error: Some(e.into()),
            }
        }
    }
}

async fn handle_mcp_method(
    client: &mut AuthedClient,
    request: &McpRequest,
    endpoint_type: McpEndpointType,
) -> Result<McpResult, McpRequestError> {
    // Validate JSON-RPC version
    if request.jsonrpc != "2.0" {
        return Err(McpRequestError::InvalidJsonRpcVersion);
    }

    // Handle different MCP methods using pattern matching
    match &request.method {
        McpMethod::Initialize(_) => {
            debug!(endpoint = %endpoint_type, "Processing initialize");
            handle_initialize(endpoint_type).await
        }
        McpMethod::ToolsList => {
            debug!(endpoint = %endpoint_type, "Processing tools/list");
            handle_tools_list(endpoint_type).await
        }
        McpMethod::ToolsCall(params) => {
            debug!(tool = %params, endpoint = %endpoint_type, "Processing tools/call");
            handle_tools_call(client, params, endpoint_type).await
        }
        McpMethod::Unknown => Err(McpRequestError::MethodNotFound(
            "unknown method".to_string(),
        )),
    }
}

async fn handle_initialize(endpoint_type: McpEndpointType) -> Result<McpResult, McpRequestError> {
    Ok(McpResult::Initialize(InitializeResult {
        protocol_version: "2024-11-05".to_string(),
        capabilities: Capabilities { tools: json!({}) },
        server_info: ServerInfo {
            name: format!("materialize-mcp-{}", endpoint_type),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
    }))
}

async fn handle_tools_list(endpoint_type: McpEndpointType) -> Result<McpResult, McpRequestError> {
    let tools = match endpoint_type {
        McpEndpointType::Agents => {
            vec![
                ToolDefinition {
                    name: "get_data_products".to_string(),
                    description: "Discover all available real-time data views (data products) that represent business entities like customers, orders, products, etc. Each data product provides fresh, queryable data with defined schemas. Use this first to see what data is available before querying specific information.".to_string(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {},
                        "required": []
                    }),
                },
                ToolDefinition {
                    name: "get_data_product_details".to_string(),
                    description: "Get the complete schema and structure of a specific data product. This shows you exactly what fields are available, their types, and what data you can query. Use this after finding a data product from get_data_products() to understand how to query it.".to_string(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "Exact name of the data product from get_data_products() list"
                            }
                        },
                        "required": ["name"]
                    }),
                },
                ToolDefinition {
                    name: "query".to_string(),
                    description: "Execute SQL queries against real-time data products to retrieve current business information. Use standard PostgreSQL syntax. You can JOIN multiple data products together, but ONLY if they are all hosted on the same cluster. Always specify the cluster parameter from the data product details. This provides fresh, up-to-date results from materialized views.".to_string(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {
                            "cluster": {
                                "type": "string",
                                "description": "Exact cluster name from the data product details - required for query execution"
                            },
                            "sql_query": {
                                "type": "string",
                                "description": "PostgreSQL-compatible SELECT statement to retrieve data. Use the fully qualified data product name exactly as provided (with double quotes). You can JOIN multiple data products, but only those on the same cluster."
                            }
                        },
                        "required": ["cluster", "sql_query"]
                    }),
                },
            ]
        }
        McpEndpointType::Observatory => {
            vec![
                ToolDefinition {
                    name: "query_system_catalog".to_string(),
                    description: "Query Materialize system catalog tables (mz_*) for troubleshooting and observability. Only mz_* tables are accessible.".to_string(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {
                            "sql_query": {
                                "type": "string",
                                "description": "SQL query restricted to mz_* system tables"
                            }
                        },
                        "required": ["sql_query"]
                    }),
                },
            ]
        }
    };

    Ok(McpResult::ToolsList(ToolsListResult { tools }))
}

async fn handle_tools_call(
    client: &mut AuthedClient,
    params: &ToolsCallParams,
    endpoint_type: McpEndpointType,
) -> Result<McpResult, McpRequestError> {
    match (endpoint_type, params) {
        (McpEndpointType::Agents, ToolsCallParams::GetDataProducts(_)) => {
            get_data_products(client).await
        }
        (McpEndpointType::Agents, ToolsCallParams::GetDataProductDetails(p)) => {
            get_data_product_details(client, &p.name).await
        }
        (McpEndpointType::Agents, ToolsCallParams::Query(p)) => {
            execute_query(client, &p.cluster, &p.sql_query).await
        }
        (McpEndpointType::Observatory, ToolsCallParams::QuerySystemCatalog(p)) => {
            query_system_catalog(client, &p.sql_query).await
        }
        // Tool called on wrong endpoint
        (endpoint, tool) => Err(McpRequestError::ToolNotFound(format!(
            "{} is not available on {} endpoint",
            tool, endpoint
        ))),
    }
}

/// Execute SQL via `execute_request` from sql.rs.
async fn execute_sql(
    client: &mut AuthedClient,
    query: &str,
) -> Result<Vec<Vec<serde_json::Value>>, McpRequestError> {
    let mut response = SqlResponse::new();

    execute_request(
        client,
        SqlRequest::Simple {
            query: query.to_string(),
        },
        &mut response,
    )
    .await
    .map_err(|e| McpRequestError::QueryExecutionFailed(e.to_string()))?;

    // Extract the result with rows (the user's single SELECT/SHOW query)
    // Other results will be OK (from BEGIN, SET, COMMIT) or Err
    for result in response.results {
        match result {
            SqlResult::Rows { rows, .. } => return Ok(rows),
            SqlResult::Err { error, .. } => {
                return Err(McpRequestError::QueryExecutionFailed(error.message));
            }
            SqlResult::Ok { .. } => continue,
        }
    }

    Err(McpRequestError::QueryExecutionFailed(
        "Query did not return any results".to_string(),
    ))
}

async fn get_data_products(client: &mut AuthedClient) -> Result<McpResult, McpRequestError> {
    debug!("Executing get_data_products");
    let rows = execute_sql(client, DISCOVERY_QUERY).await?;
    debug!("get_data_products returned {} rows", rows.len());
    if rows.is_empty() {
        warn!("No data products found - indexes must have comments");
    }

    let text =
        serde_json::to_string_pretty(&rows).map_err(|e| McpRequestError::Internal(anyhow!(e)))?;

    Ok(McpResult::ToolContent(ToolContentResult {
        content: vec![ContentBlock {
            content_type: "text".to_string(),
            text,
        }],
    }))
}

async fn get_data_product_details(
    client: &mut AuthedClient,
    name: &str,
) -> Result<McpResult, McpRequestError> {
    debug!(name = %name, "Executing get_data_product_details");

    let query = format!(
        "SELECT * FROM mz_internal.mz_mcp_data_products WHERE object_name = {}",
        escaped_string_literal(name)
    );

    let rows = execute_sql(client, &query).await?;

    if rows.is_empty() {
        return Err(McpRequestError::DataProductNotFound(name.to_string()));
    }

    let text =
        serde_json::to_string_pretty(&rows).map_err(|e| McpRequestError::Internal(anyhow!(e)))?;

    Ok(McpResult::ToolContent(ToolContentResult {
        content: vec![ContentBlock {
            content_type: "text".to_string(),
            text,
        }],
    }))
}

/// Validates query is a single SELECT, SHOW, or EXPLAIN statement.
fn validate_readonly_query(sql: &str) -> Result<(), McpRequestError> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(McpRequestError::QueryValidationFailed(
            "Empty query".to_string(),
        ));
    }

    // Parse the SQL to get AST
    let stmts = parse(sql).map_err(|e| {
        McpRequestError::QueryValidationFailed(format!("Failed to parse SQL: {}", e))
    })?;

    // Only allow a single statement
    if stmts.len() != 1 {
        return Err(McpRequestError::QueryValidationFailed(format!(
            "Only one query allowed at a time. Found {} statements.",
            stmts.len()
        )));
    }

    // Allowlist: Only SELECT, SHOW, and EXPLAIN statements permitted
    let stmt = &stmts[0];
    use mz_sql_parser::ast::Statement;

    match &stmt.ast {
        Statement::Select(_) | Statement::Show(_) | Statement::ExplainPlan(_) => {
            // Allowed - read-only operations
            Ok(())
        }
        _ => Err(McpRequestError::QueryValidationFailed(
            "Only SELECT, SHOW, and EXPLAIN statements are allowed".to_string(),
        )),
    }
}

async fn execute_query(
    client: &mut AuthedClient,
    cluster: &str,
    sql_query: &str,
) -> Result<McpResult, McpRequestError> {
    debug!(cluster = %cluster, "Executing user query");

    validate_readonly_query(sql_query)?;

    // Use READ ONLY transaction to prevent modifications
    // Combine with SET CLUSTER (prometheus.rs:29-33 pattern)
    let combined_query = format!(
        "BEGIN READ ONLY; SET CLUSTER = {}; {}; COMMIT;",
        escaped_string_literal(cluster),
        sql_query
    );

    let rows = execute_sql(client, &combined_query).await?;

    let text =
        serde_json::to_string_pretty(&rows).map_err(|e| McpRequestError::Internal(anyhow!(e)))?;

    Ok(McpResult::ToolContent(ToolContentResult {
        content: vec![ContentBlock {
            content_type: "text".to_string(),
            text,
        }],
    }))
}

async fn query_system_catalog(
    client: &mut AuthedClient,
    sql_query: &str,
) -> Result<McpResult, McpRequestError> {
    debug!("Executing query_system_catalog");

    // First validate it's a read-only query
    validate_readonly_query(sql_query)?;

    // Then validate that query only references mz_* tables by parsing the SQL
    validate_system_catalog_query(sql_query)?;

    // Use READ ONLY transaction for defense-in-depth
    let wrapped_query = format!("BEGIN READ ONLY; {}; COMMIT;", sql_query);
    let rows = execute_sql(client, &wrapped_query).await?;

    let text =
        serde_json::to_string_pretty(&rows).map_err(|e| McpRequestError::Internal(anyhow!(e)))?;

    Ok(McpResult::ToolContent(ToolContentResult {
        content: vec![ContentBlock {
            content_type: "text".to_string(),
            text,
        }],
    }))
}

/// Collects table references from SQL AST with their schema qualification.
struct TableReferenceCollector {
    /// Stores (schema, table_name) tuples. Schema is None if unqualified.
    tables: Vec<(Option<String>, String)>,
    /// CTE names to exclude from validation (they're not real tables)
    cte_names: std::collections::BTreeSet<String>,
}

impl TableReferenceCollector {
    fn new() -> Self {
        Self {
            tables: Vec::new(),
            cte_names: std::collections::BTreeSet::new(),
        }
    }
}

impl<'ast> Visit<'ast, Raw> for TableReferenceCollector {
    fn visit_cte(&mut self, cte: &'ast mz_sql_parser::ast::Cte<Raw>) {
        // Track CTE names so we don't treat them as table references
        self.cte_names
            .insert(cte.alias.name.as_str().to_lowercase());
        visit::visit_cte(self, cte);
    }

    fn visit_table_factor(&mut self, table_factor: &'ast mz_sql_parser::ast::TableFactor<Raw>) {
        // Only visit actual table references in FROM/JOIN clauses, not function names
        if let mz_sql_parser::ast::TableFactor::Table { name, .. } = table_factor {
            match name {
                RawItemName::Name(n) | RawItemName::Id(_, n, _) => {
                    let parts = &n.0;
                    if !parts.is_empty() {
                        let table_name = parts.last().unwrap().as_str().to_lowercase();

                        // Skip if this is a CTE reference, not a real table
                        if self.cte_names.contains(&table_name) {
                            visit::visit_table_factor(self, table_factor);
                            return;
                        }

                        // Extract schema if qualified (e.g., mz_catalog.mz_tables)
                        let schema = if parts.len() >= 2 {
                            Some(parts[parts.len() - 2].as_str().to_lowercase())
                        } else {
                            None
                        };
                        self.tables.push((schema, table_name));
                    }
                }
            }
        }
        visit::visit_table_factor(self, table_factor);
    }
}

/// Validates query references only mz_* system catalog tables.
fn validate_system_catalog_query(sql: &str) -> Result<(), McpRequestError> {
    // Parse the SQL to validate it
    let stmts = parse(sql).map_err(|e| {
        McpRequestError::QueryValidationFailed(format!("Failed to parse SQL: {}", e))
    })?;

    if stmts.is_empty() {
        return Err(McpRequestError::QueryValidationFailed(
            "Empty query".to_string(),
        ));
    }

    // Walk the AST to collect all table references
    let mut collector = TableReferenceCollector::new();
    for stmt in &stmts {
        collector.visit_statement(&stmt.ast);
    }

    // Allowed system schemas
    const ALLOWED_SCHEMAS: &[&str] = &[
        "mz_catalog",
        "mz_internal",
        "pg_catalog",
        "information_schema",
    ];

    // Helper to check if a table reference is allowed
    let is_system_table = |(schema, table_name): &(Option<String>, String)| {
        match schema {
            // Explicitly qualified with allowed schema
            Some(s) => ALLOWED_SCHEMAS.contains(&s.as_str()),
            // Unqualified: allow if starts with mz_ (common Materialize system tables)
            None => table_name.starts_with("mz_"),
        }
    };

    // Check that all table references are system tables
    let non_system_tables: Vec<String> = collector
        .tables
        .iter()
        .filter(|t| !is_system_table(t))
        .map(|(schema, table)| match schema {
            Some(s) => format!("{}.{}", s, table),
            None => table.clone(),
        })
        .collect();

    if !non_system_tables.is_empty() {
        return Err(McpRequestError::QueryValidationFailed(format!(
            "Query references non-system tables: {}. Only system catalog tables (mz_*, pg_catalog, information_schema) are allowed.",
            non_system_tables.join(", ")
        )));
    }

    // Ensure at least one system table is referenced
    if collector.tables.is_empty() || !collector.tables.iter().any(is_system_table) {
        return Err(McpRequestError::QueryValidationFailed(
            "Query must reference at least one system catalog table".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_validate_readonly_query_select() {
        assert!(validate_readonly_query("SELECT * FROM mz_tables").is_ok());
        assert!(validate_readonly_query("SELECT 1 + 2").is_ok());
        assert!(validate_readonly_query("  SELECT 1  ").is_ok());
    }

    #[mz_ore::test]
    fn test_validate_readonly_query_subqueries() {
        // Simple subquery in WHERE clause
        assert!(
            validate_readonly_query(
                "SELECT * FROM mz_tables WHERE id IN (SELECT id FROM mz_columns)"
            )
            .is_ok()
        );

        // Subquery in FROM clause
        assert!(
            validate_readonly_query(
                "SELECT * FROM (SELECT * FROM mz_tables WHERE name LIKE 'test%') AS t"
            )
            .is_ok()
        );

        // Correlated subquery
        assert!(validate_readonly_query(
            "SELECT * FROM mz_tables t WHERE EXISTS (SELECT 1 FROM mz_columns c WHERE c.id = t.id)"
        )
        .is_ok());

        // Nested subqueries
        assert!(validate_readonly_query(
            "SELECT * FROM mz_tables WHERE id IN (SELECT id FROM mz_columns WHERE type IN (SELECT name FROM mz_types))"
        )
        .is_ok());

        // Subquery with aggregation
        assert!(
            validate_readonly_query(
                "SELECT * FROM mz_tables WHERE id = (SELECT MAX(id) FROM mz_columns)"
            )
            .is_ok()
        );
    }

    #[mz_ore::test]
    fn test_validate_readonly_query_show() {
        assert!(validate_readonly_query("SHOW CLUSTERS").is_ok());
        assert!(validate_readonly_query("SHOW TABLES").is_ok());
    }

    #[mz_ore::test]
    fn test_validate_readonly_query_explain() {
        assert!(validate_readonly_query("EXPLAIN SELECT 1").is_ok());
    }

    #[mz_ore::test]
    fn test_validate_readonly_query_rejects_writes() {
        assert!(validate_readonly_query("INSERT INTO t VALUES (1)").is_err());
        assert!(validate_readonly_query("UPDATE t SET a = 1").is_err());
        assert!(validate_readonly_query("DELETE FROM t").is_err());
        assert!(validate_readonly_query("CREATE TABLE t (a INT)").is_err());
        assert!(validate_readonly_query("DROP TABLE t").is_err());
    }

    #[mz_ore::test]
    fn test_validate_readonly_query_rejects_multiple() {
        assert!(validate_readonly_query("SELECT 1; SELECT 2").is_err());
    }

    #[mz_ore::test]
    fn test_validate_readonly_query_rejects_empty() {
        assert!(validate_readonly_query("").is_err());
        assert!(validate_readonly_query("   ").is_err());
    }

    #[mz_ore::test]
    fn test_validate_system_catalog_query_accepts_mz_tables() {
        assert!(validate_system_catalog_query("SELECT * FROM mz_tables").is_ok());
        assert!(validate_system_catalog_query("SELECT * FROM mz_internal.mz_comments").is_ok());
        assert!(
            validate_system_catalog_query(
                "SELECT * FROM mz_tables t JOIN mz_columns c ON t.id = c.id"
            )
            .is_ok()
        );
    }

    #[mz_ore::test]
    fn test_validate_system_catalog_query_subqueries() {
        // Subquery with mz_* tables
        assert!(
            validate_system_catalog_query(
                "SELECT * FROM mz_tables WHERE id IN (SELECT id FROM mz_columns)"
            )
            .is_ok()
        );

        // Nested subqueries with mz_* tables
        assert!(validate_system_catalog_query(
            "SELECT * FROM mz_tables WHERE id IN (SELECT table_id FROM mz_columns WHERE type IN (SELECT id FROM mz_types))"
        )
        .is_ok());

        // Subquery in FROM clause
        assert!(
            validate_system_catalog_query(
                "SELECT * FROM (SELECT * FROM mz_tables WHERE name LIKE 'test%') AS t"
            )
            .is_ok()
        );

        // Reject subqueries that reference non-mz_* tables
        assert!(
            validate_system_catalog_query(
                "SELECT * FROM mz_tables WHERE id IN (SELECT table_id FROM user_data)"
            )
            .is_err()
        );

        // Reject mixed references in nested subqueries
        assert!(validate_system_catalog_query(
            "SELECT * FROM mz_tables WHERE id IN (SELECT id FROM (SELECT id FROM user_table) AS t)"
        )
        .is_err());
    }

    #[mz_ore::test]
    fn test_validate_system_catalog_query_rejects_user_tables() {
        assert!(validate_system_catalog_query("SELECT * FROM user_data").is_err());
        assert!(validate_system_catalog_query("SELECT * FROM my_table").is_err());
        // Security: reject queries that mention mz_ in a non-table context
        assert!(
            validate_system_catalog_query("SELECT * FROM user_data WHERE 'mz_' IS NOT NULL")
                .is_err()
        );
    }

    #[mz_ore::test]
    fn test_validate_system_catalog_query_allows_functions() {
        // Function names should not be treated as table references
        assert!(
            validate_system_catalog_query(
                "SELECT date_part('year', now())::int4 AS y FROM mz_tables LIMIT 1"
            )
            .is_ok()
        );
        assert!(validate_system_catalog_query("SELECT length(name) FROM mz_tables").is_ok());
        assert!(
            validate_system_catalog_query(
                "SELECT count(*) FROM mz_sources WHERE now() > created_at"
            )
            .is_ok()
        );
    }

    #[mz_ore::test]
    fn test_validate_system_catalog_query_schema_qualified() {
        // Qualified with allowed schemas should work
        assert!(validate_system_catalog_query("SELECT * FROM mz_catalog.mz_tables").is_ok());
        assert!(validate_system_catalog_query("SELECT * FROM mz_internal.mz_sessions").is_ok());
        assert!(validate_system_catalog_query("SELECT * FROM pg_catalog.pg_type").is_ok());
        assert!(validate_system_catalog_query("SELECT * FROM information_schema.tables").is_ok());

        // Qualified with disallowed schema should fail
        assert!(validate_system_catalog_query("SELECT * FROM public.user_table").is_err());
        assert!(validate_system_catalog_query("SELECT * FROM myschema.mytable").is_err());

        // Mixed: system and user schemas should fail
        assert!(
            validate_system_catalog_query(
                "SELECT * FROM mz_catalog.mz_tables JOIN public.user_data ON true"
            )
            .is_err()
        );
    }

    #[mz_ore::test]
    fn test_validate_system_catalog_query_adversarial_cases() {
        // Try to sneak in user table via CTE
        assert!(
            validate_system_catalog_query(
                "WITH user_cte AS (SELECT * FROM user_data) \
                 SELECT * FROM mz_tables, user_cte"
            )
            .is_err(),
            "Should reject CTE referencing user table"
        );

        // Complex multi-level CTE with user table buried deep
        assert!(
            validate_system_catalog_query(
                "WITH \
                   cte1 AS (SELECT * FROM mz_tables), \
                   cte2 AS (SELECT * FROM cte1), \
                   cte3 AS (SELECT * FROM user_data) \
                 SELECT * FROM cte2"
            )
            .is_err(),
            "Should reject CTE chain with user table"
        );

        // Multiple joins - user table in the middle
        assert!(
            validate_system_catalog_query(
                "SELECT * FROM mz_tables t1 \
                 JOIN user_data u ON t1.id = u.id \
                 JOIN mz_sources s ON t1.id = s.id"
            )
            .is_err(),
            "Should reject multi-join with user table"
        );

        // LEFT JOIN trying to hide user table
        assert!(
            validate_system_catalog_query(
                "SELECT * FROM mz_tables t \
                 LEFT JOIN user_data u ON t.id = u.table_id \
                 WHERE u.id IS NULL"
            )
            .is_err(),
            "Should reject LEFT JOIN with user table"
        );

        // Nested subquery with user table in FROM
        assert!(
            validate_system_catalog_query(
                "SELECT * FROM mz_tables WHERE id IN \
                 (SELECT table_id FROM (SELECT * FROM user_data) AS u)"
            )
            .is_err(),
            "Should reject nested subquery with user table"
        );

        // UNION trying to mix system and user data
        assert!(
            validate_system_catalog_query(
                "SELECT name FROM mz_tables \
                 UNION \
                 SELECT name FROM user_data"
            )
            .is_err(),
            "Should reject UNION with user table"
        );

        // UNION ALL variation
        assert!(
            validate_system_catalog_query(
                "SELECT id FROM mz_sources \
                 UNION ALL \
                 SELECT id FROM products"
            )
            .is_err(),
            "Should reject UNION ALL with user table"
        );

        // Cross join with user table
        assert!(
            validate_system_catalog_query("SELECT * FROM mz_tables CROSS JOIN user_data").is_err(),
            "Should reject CROSS JOIN with user table"
        );

        // Subquery in SELECT clause referencing user table
        assert!(
            validate_system_catalog_query(
                "SELECT t.*, (SELECT COUNT(*) FROM user_data) AS cnt FROM mz_tables t"
            )
            .is_err(),
            "Should reject subquery in SELECT with user table"
        );

        // Try to use a schema name that looks similar to allowed ones
        assert!(
            validate_system_catalog_query("SELECT * FROM mz_catalogg.fake_table").is_err(),
            "Should reject typo-squatting schema name"
        );
        assert!(
            validate_system_catalog_query("SELECT * FROM mz_catalog_hack.fake_table").is_err(),
            "Should reject fake schema with mz_catalog prefix"
        );

        // Lateral join with user table
        assert!(
            validate_system_catalog_query(
                "SELECT * FROM mz_tables t, LATERAL (SELECT * FROM user_data WHERE id = t.id) u"
            )
            .is_err(),
            "Should reject LATERAL join with user table"
        );

        // Valid complex query - all system tables
        assert!(
            validate_system_catalog_query(
                "WITH \
                   tables AS (SELECT * FROM mz_tables), \
                   sources AS (SELECT * FROM mz_sources) \
                 SELECT t.name, s.name \
                 FROM tables t \
                 JOIN sources s ON t.id = s.id \
                 WHERE t.id IN (SELECT id FROM mz_columns)"
            )
            .is_ok(),
            "Should allow complex query with only system tables"
        );

        // Valid UNION of system tables
        assert!(
            validate_system_catalog_query(
                "SELECT name FROM mz_tables \
                 UNION \
                 SELECT name FROM mz_sources"
            )
            .is_ok(),
            "Should allow UNION of system tables"
        );
    }

    #[mz_ore::test]
    fn test_validate_system_catalog_query_rejects_mixed_tables() {
        assert!(
            validate_system_catalog_query(
                "SELECT * FROM mz_tables t JOIN user_data u ON t.id = u.table_id"
            )
            .is_err()
        );
    }

    #[mz_ore::test]
    fn test_mcp_error_codes() {
        assert_eq!(
            McpRequestError::InvalidJsonRpcVersion.error_code(),
            error_codes::INVALID_REQUEST
        );
        assert_eq!(
            McpRequestError::MethodNotFound("test".to_string()).error_code(),
            error_codes::METHOD_NOT_FOUND
        );
        assert_eq!(
            McpRequestError::QueryExecutionFailed("test".to_string()).error_code(),
            error_codes::INTERNAL_ERROR
        );
    }
}
