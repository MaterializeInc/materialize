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
//! **Observatory:** `query_system_catalog`, `cluster_health_check`
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
use tracing::{error, info, warn};

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
    MethodNotFound(String),
    #[error("Missing required parameter: {0}")]
    MissingParameter(String),
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
            Self::MissingParameter(_) => error_codes::INVALID_PARAMS,
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
            Self::MissingParameter(_) => "ParameterError",
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
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct McpResponse {
    jsonrpc: String,
    id: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<McpError>,
}

/// JSON-RPC 2.0 error codes.
mod error_codes {
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;
}

/// MCP method names per spec.
mod methods {
    pub const INITIALIZE: &str = "initialize";
    pub const TOOLS_LIST: &str = "tools/list";
    pub const TOOLS_CALL: &str = "tools/call";
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

    info!(
        method = %request.method,
        endpoint = %endpoint_type,
        user = %user,
        is_notification = is_notification,
        "MCP request received"
    );

    // Handle notifications (no response needed)
    if is_notification {
        info!(method = %request.method, "Received notification (no response will be sent)");
        return StatusCode::OK.into_response();
    }

    // Spawn task for fault isolation
    let handle = mz_ore::task::spawn(|| "mcp_request", async move {
        handle_mcp_request_inner(&mut client, request, endpoint_type).await
    });

    match handle.await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => {
            error!(error = %e, "MCP request processing failed");
            let response = McpResponse {
                jsonrpc: "2.0".to_string(),
                id: serde_json::Value::Null,
                result: None,
                error: Some(McpError {
                    code: error_codes::INTERNAL_ERROR,
                    message: format!("Internal error: {}", e),
                    data: Some(json!({
                        "error_details": e.to_string(),
                    })),
                }),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

async fn handle_mcp_request_inner(
    client: &mut AuthedClient,
    request: McpRequest,
    endpoint_type: McpEndpointType,
) -> Result<McpResponse, anyhow::Error> {
    // Extract request ID (guaranteed to be Some since notifications are filtered earlier)
    let request_id = request.id.clone().unwrap_or(serde_json::Value::Null);

    let result = handle_mcp_method(client, &request, endpoint_type).await;

    Ok(match result {
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
    })
}

async fn handle_mcp_method(
    client: &mut AuthedClient,
    request: &McpRequest,
    endpoint_type: McpEndpointType,
) -> Result<serde_json::Value, McpRequestError> {
    // Validate JSON-RPC version
    if request.jsonrpc != "2.0" {
        return Err(McpRequestError::InvalidJsonRpcVersion);
    }

    info!(
        method = %request.method,
        endpoint = %endpoint_type,
        "Processing MCP method"
    );

    // Handle different MCP methods
    match request.method.as_str() {
        methods::INITIALIZE => handle_initialize(endpoint_type).await,
        methods::TOOLS_LIST => handle_tools_list(endpoint_type).await,
        methods::TOOLS_CALL => handle_tools_call(client, &request.params, endpoint_type).await,
        method => Err(McpRequestError::MethodNotFound(method.to_string())),
    }
}

async fn handle_initialize(
    endpoint_type: McpEndpointType,
) -> Result<serde_json::Value, McpRequestError> {
    info!(endpoint = %endpoint_type, "MCP initialize");

    Ok(serde_json::json!({
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {}
        },
        "serverInfo": {
            "name": format!("materialize-mcp-{}", endpoint_type),
            "version": env!("CARGO_PKG_VERSION")
        }
    }))
}

async fn handle_tools_list(
    endpoint_type: McpEndpointType,
) -> Result<serde_json::Value, McpRequestError> {
    info!(endpoint = %endpoint_type, "MCP tools/list");

    let tools = match endpoint_type {
        McpEndpointType::Agents => {
            vec![
                serde_json::json!({
                    "name": "get_data_products",
                    "description": "Discover all available real-time data views (data products) that represent business entities like customers, orders, products, etc. Each data product provides fresh, queryable data with defined schemas. Use this first to see what data is available before querying specific information.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                }),
                serde_json::json!({
                    "name": "get_data_product_details",
                    "description": "Get the complete schema and structure of a specific data product. This shows you exactly what fields are available, their types, and what data you can query. Use this after finding a data product from get_data_products() to understand how to query it.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "Exact name of the data product from get_data_products() list"
                            }
                        },
                        "required": ["name"]
                    }
                }),
                serde_json::json!({
                    "name": "query",
                    "description": "Execute SQL queries against real-time data products to retrieve current business information. Use standard PostgreSQL syntax. You can JOIN multiple data products together, but ONLY if they are all hosted on the same cluster. Always specify the cluster parameter from the data product details. This provides fresh, up-to-date results from materialized views.",
                    "inputSchema": {
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
                    }
                }),
            ]
        }
        McpEndpointType::Observatory => {
            vec![
                serde_json::json!({
                    "name": "query_system_catalog",
                    "description": "Query Materialize system catalog tables (mz_*) for troubleshooting and observability. Only mz_* tables are accessible.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "sql_query": {
                                "type": "string",
                                "description": "SQL query restricted to mz_* system tables"
                            }
                        },
                        "required": ["sql_query"]
                    }
                }),
                serde_json::json!({
                    "name": "cluster_health_check",
                    "description": "Cluster health diagnostic tool. If a user reports issues with their cluster (slow queries, lagging objects, freshness concerns, etc.), the agent should offer to run this health check to diagnose potential problems. The agent will summarize the results showing total checks run and failures found, list failed checks with brief descriptions, and offer to provide detailed assistance for fixing identified issues.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "cluster": {
                                "type": "string",
                                "description": "Name of the cluster to check"
                            }
                        },
                        "required": ["cluster"]
                    }
                }),
            ]
        }
    };

    Ok(serde_json::json!({
        "tools": tools
    }))
}

async fn handle_tools_call(
    client: &mut AuthedClient,
    params: &serde_json::Value,
    endpoint_type: McpEndpointType,
) -> Result<serde_json::Value, McpRequestError> {
    // Extract tool name and arguments from params
    let tool_name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpRequestError::MissingParameter("name".to_string()))?;

    let arguments = params
        .get("arguments")
        .ok_or_else(|| McpRequestError::MissingParameter("arguments".to_string()))?;

    info!(tool = %tool_name, endpoint = %endpoint_type, "MCP tools/call");

    match (endpoint_type, tool_name) {
        (McpEndpointType::Agents, "get_data_products") => get_data_products(client).await,
        (McpEndpointType::Agents, "get_data_product_details") => {
            let name = arguments
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| McpRequestError::MissingParameter("arguments.name".to_string()))?;
            get_data_product_details(client, name).await
        }
        (McpEndpointType::Agents, "query") => {
            let cluster = arguments
                .get("cluster")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    McpRequestError::MissingParameter("arguments.cluster".to_string())
                })?;
            let sql_query = arguments
                .get("sql_query")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    McpRequestError::MissingParameter("arguments.sql_query".to_string())
                })?;
            execute_query(client, cluster, sql_query).await
        }
        (McpEndpointType::Observatory, "query_system_catalog") => {
            let sql_query = arguments
                .get("sql_query")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    McpRequestError::MissingParameter("arguments.sql_query".to_string())
                })?;
            query_system_catalog(client, sql_query).await
        }
        (McpEndpointType::Observatory, "cluster_health_check") => {
            let cluster = arguments
                .get("cluster")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    McpRequestError::MissingParameter("arguments.cluster".to_string())
                })?;
            cluster_health_check(client, cluster).await
        }
        _ => Err(McpRequestError::ToolNotFound(format!(
            "{} (endpoint: {})",
            tool_name, endpoint_type
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
    // Other results will be OK (from BEGIN, SET, COMMIT)
    for result in response.results {
        if let SqlResult::Rows { rows, .. } = result {
            return Ok(rows);
        }
    }

    Err(McpRequestError::QueryExecutionFailed(
        "Query did not return any rows".to_string(),
    ))
}

async fn get_data_products(
    client: &mut AuthedClient,
) -> Result<serde_json::Value, McpRequestError> {
    info!("Executing get_data_products");
    let rows = execute_sql(client, DISCOVERY_QUERY).await?;
    info!("get_data_products returned {} rows", rows.len());
    if rows.is_empty() {
        warn!("No data products found - indexes must have comments");
    }

    let text =
        serde_json::to_string_pretty(&rows).map_err(|e| McpRequestError::Internal(anyhow!(e)))?;

    Ok(json!({
        "content": [{
            "type": "text",
            "text": text
        }]
    }))
}

async fn get_data_product_details(
    client: &mut AuthedClient,
    name: &str,
) -> Result<serde_json::Value, McpRequestError> {
    info!(name = %name, "Executing get_data_product_details");

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

    Ok(json!({
        "content": [{
            "type": "text",
            "text": text
        }]
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
) -> Result<serde_json::Value, McpRequestError> {
    info!(cluster = %cluster, "Executing user query");

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

    Ok(json!({
        "content": [{
            "type": "text",
            "text": text
        }]
    }))
}

async fn query_system_catalog(
    client: &mut AuthedClient,
    sql_query: &str,
) -> Result<serde_json::Value, McpRequestError> {
    info!("Executing query_system_catalog");

    // First validate it's a read-only query
    validate_readonly_query(sql_query)?;

    // Then validate that query only references mz_* tables by parsing the SQL
    validate_system_catalog_query(sql_query)?;

    let rows = execute_sql(client, sql_query).await?;

    let text =
        serde_json::to_string_pretty(&rows).map_err(|e| McpRequestError::Internal(anyhow!(e)))?;

    Ok(json!({
        "content": [{
            "type": "text",
            "text": text
        }]
    }))
}

/// Collects table references from SQL AST.
struct TableReferenceCollector {
    tables: Vec<String>,
}

impl TableReferenceCollector {
    fn new() -> Self {
        Self { tables: Vec::new() }
    }
}

impl<'ast> Visit<'ast, Raw> for TableReferenceCollector {
    fn visit_item_name(&mut self, name: &'ast <Raw as mz_sql_parser::ast::AstInfo>::ItemName) {
        match name {
            RawItemName::Name(n) | RawItemName::Id(_, n, _) => {
                if let Some(ident) = n.0.last() {
                    self.tables.push(ident.as_str().to_lowercase());
                }
            }
        }
        visit::visit_item_name(self, name);
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

    // Check that all table references are mz_* tables
    let non_system_tables: Vec<&str> = collector
        .tables
        .iter()
        .filter(|name| !name.starts_with("mz_"))
        .map(|s| s.as_str())
        .collect();

    if !non_system_tables.is_empty() {
        return Err(McpRequestError::QueryValidationFailed(format!(
            "Query references non-system tables: {}. Only mz_* system catalog tables are allowed.",
            non_system_tables.join(", ")
        )));
    }

    // Ensure at least one mz_* table is referenced
    if collector.tables.is_empty() || !collector.tables.iter().any(|name| name.starts_with("mz_")) {
        return Err(McpRequestError::QueryValidationFailed(
            "Query must reference at least one mz_* system catalog table".to_string(),
        ));
    }

    Ok(())
}

/// Placeholder for cluster health diagnostics (not yet implemented).
async fn cluster_health_check(
    _client: &mut AuthedClient,
    cluster: &str,
) -> Result<serde_json::Value, McpRequestError> {
    info!(cluster = %cluster, "Executing cluster_health_check (placeholder)");

    let result = json!({
        "cluster_name": cluster,
        "status": "not_implemented",
        "message": "Cluster health check is not yet implemented in this POC. This tool will provide diagnostics including replica utilization, long-running queries, wallclock lag, and dependency analysis.",
        "planned_checks": [
            "Replica CPU, memory, and disk utilization",
            "Long-running query detection",
            "Object wallclock lag analysis",
            "Dependency lag analysis with recursive queries"
        ]
    });

    let text =
        serde_json::to_string_pretty(&result).map_err(|e| McpRequestError::Internal(anyhow!(e)))?;

    Ok(json!({
        "content": [{
            "type": "text",
            "text": text
        }]
    }))
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
            McpRequestError::MissingParameter("test".to_string()).error_code(),
            error_codes::INVALID_PARAMS
        );
        assert_eq!(
            McpRequestError::QueryExecutionFailed("test".to_string()).error_code(),
            error_codes::INTERNAL_ERROR
        );
    }
}
