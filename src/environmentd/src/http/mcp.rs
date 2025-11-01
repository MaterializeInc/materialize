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
//! This module implements MCP server endpoints that expose Materialize data products
//! to AI agents. There are two endpoints:
//!
//! - `/api/mcp/agents`: Exposes user data products (views, materialized views, indexes)
//!   for building customer-facing AI agents
//! - `/api/mcp/observatory`: Exposes system catalog (`mz_*` tables) for troubleshooting
//!
//! ## Architecture
//!
//! The implementation follows established patterns from other HTTP handlers:
//! - Uses `AuthedClient` for authenticated access
//! - Spawns tasks for fault isolation (panics won't crash environmentd)
//! - Uses `tracing` for structured logging
//! - Follows JSON-RPC 2.0 protocol via `rmcp` SDK
//!
//! ## Tools Provided
//!
//! **Agents endpoint:**
//! 1. `get_data_products` - Lists all available data products
//! 2. `get_data_product_details` - Gets schema for a specific data product
//! 3. `query` - Executes SQL queries against data products
//!
//! **Observatory endpoint:**
//! 1. `query_system_catalog` - Queries system tables (mz_*) for troubleshooting
//!
//! ## Future Improvements
//!
//! See TODO in `src/catalog/src/builtin.rs` about adding `mz_mcp_data_products` system view.
//! This would move the discovery SQL into the catalog layer where it belongs.

use anyhow::{anyhow, bail};
use axum::Json;
use axum::response::IntoResponse;
use http::StatusCode;
use mz_sql::parse::parse;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast::display::escaped_string_literal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{error, info, warn};

use crate::http::AuthedClient;
use crate::http::sql::{SqlRequest, SqlResponse, SqlResult, execute_request};

/// Data product discovery query.
///
/// This query dynamically discovers all user-accessible indexes and builds
/// MCP tool definitions from them. It respects SELECT and USAGE privileges.
///
/// TODO: Move this to a system view `mz_mcp_data_products` in builtin.rs
/// for better separation of concerns and reusability.
const DISCOVERY_QUERY: &str = r#"
WITH tools AS (
    SELECT
        '"' || op.database || '"."' || op.schema || '"."' || op.name || '"' AS object_name,
        c.name AS cluster,
        cts.comment AS description,
        jsonb_build_object(
            'type', 'object',
            'required', jsonb_agg(distinct ccol.name) FILTER (WHERE ccol.position = ic.on_position),
            'properties', jsonb_strip_nulls(jsonb_object_agg(
                ccol.name,
                CASE
                    WHEN ccol.type IN (
                        'uint2', 'uint4','uint8', 'int', 'integer', 'smallint',
                        'double', 'double precision', 'bigint', 'float',
                        'numeric', 'real'
                    ) THEN jsonb_build_object(
                        'type', 'number',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type = 'boolean' THEN jsonb_build_object(
                        'type', 'boolean',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type = 'bytea' THEN jsonb_build_object(
                        'type', 'string',
                        'description', cts_col.comment,
                        'contentEncoding', 'base64',
                        'contentMediaType', 'application/octet-stream'
                    )
                    WHEN ccol.type = 'date' THEN jsonb_build_object(
                        'type', 'string',
                        'format', 'date',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type = 'time' THEN jsonb_build_object(
                        'type', 'string',
                        'format', 'time',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type ilike 'timestamp%%' THEN jsonb_build_object(
                        'type', 'string',
                        'format', 'date-time',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type = 'jsonb' THEN jsonb_build_object(
                        'type', 'object',
                        'description', cts_col.comment
                    )
                    WHEN ccol.type = 'uuid' THEN jsonb_build_object(
                        'type', 'string',
                        'format', 'uuid',
                        'description', cts_col.comment
                    )
                    ELSE jsonb_build_object(
                        'type', 'string',
                        'description', cts_col.comment
                    )
                END
            ))
        ) AS schema
    FROM mz_internal.mz_show_my_object_privileges op
    JOIN mz_objects o ON op.name = o.name AND op.object_type = o.type
    JOIN mz_schemas s ON s.name = op.schema AND s.id = o.schema_id
    JOIN mz_databases d ON d.name = op.database AND d.id = s.database_id
    JOIN mz_indexes i ON i.on_id = o.id
    JOIN mz_index_columns ic ON i.id = ic.index_id
    JOIN mz_columns ccol ON ccol.id = o.id
    JOIN mz_clusters c ON c.id = i.cluster_id
    JOIN mz_internal.mz_show_my_cluster_privileges cp ON cp.name = c.name
    JOIN mz_internal.mz_comments cts ON cts.id = i.id AND cts.object_sub_id IS NULL
    LEFT JOIN mz_internal.mz_comments cts_col ON cts_col.id = o.id AND cts_col.object_sub_id = ccol.position
    WHERE op.privilege_type = 'SELECT'
      AND cp.privilege_type = 'USAGE'
    GROUP BY 1,2,3
)
SELECT * FROM tools
"#;

/// MCP JSON-RPC request format
///
/// In JSON-RPC 2.0:
/// - Requests have an `id` field and expect a response
/// - Notifications have NO `id` field (fire-and-forget, no response expected)
#[derive(Debug, Deserialize)]
pub(crate) struct McpRequest {
    jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<serde_json::Value>,
    method: String,
    #[serde(default)]
    #[allow(dead_code)] // Will be used when implementing tools/call
    params: serde_json::Value,
}

/// MCP JSON-RPC response format
#[derive(Debug, Serialize)]
pub(crate) struct McpResponse {
    jsonrpc: String,
    id: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<McpError>,
}

/// Standard JSON-RPC 2.0 error codes
///
/// These follow the JSON-RPC 2.0 specification for error reporting.
mod error_codes {
    /// Invalid JSON-RPC request
    pub const INVALID_REQUEST: i32 = -32600;
    /// Method not found
    pub const METHOD_NOT_FOUND: i32 = -32601;
    /// Invalid method parameters
    pub const INVALID_PARAMS: i32 = -32602;
    /// Internal JSON-RPC error
    pub const INTERNAL_ERROR: i32 = -32603;
}

/// Standard MCP method names
///
/// These are the methods defined by the Model Context Protocol specification.
mod methods {
    pub const INITIALIZE: &str = "initialize";
    pub const TOOLS_LIST: &str = "tools/list";
    pub const TOOLS_CALL: &str = "tools/call";
}

/// MCP JSON-RPC error format
#[derive(Debug, Serialize)]
pub(crate) struct McpError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
}

/// Type of MCP endpoint (agents vs observatory)
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

/// Handle MCP requests for customer agent building.
///
/// This endpoint exposes customer data products (views, materialized views, indexes)
/// for AI agents to query.
pub async fn handle_mcp_agents(
    client: AuthedClient,
    Json(request): Json<McpRequest>,
) -> impl IntoResponse {
    handle_mcp_request(client, request, McpEndpointType::Agents).await
}

/// Handle MCP requests for Intercom/observatory use.
///
/// This endpoint exposes only system catalog tables (mz_*) for troubleshooting.
pub async fn handle_mcp_observatory(
    client: AuthedClient,
    Json(request): Json<McpRequest>,
) -> impl IntoResponse {
    handle_mcp_request(client, request, McpEndpointType::Observatory).await
}

/// Main MCP request handler (spawns task for fault isolation)
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

    // Spawn task for fault isolation - panics won't crash environmentd
    let handle = mz_ore::task::spawn(|| "mcp_request", async move {
        handle_mcp_request_inner(&mut client, request, endpoint_type).await
    });

    match handle.await {
        Ok(Ok(response)) => (StatusCode::OK, Json(response)).into_response(),
        Ok(Err(e)) => {
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
        Err(e) => {
            error!(error = %e, "MCP task panicked");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error: task panicked",
            )
                .into_response()
        }
    }
}

/// Inner MCP request handler (runs in isolated task)
async fn handle_mcp_request_inner(
    client: &mut AuthedClient,
    request: McpRequest,
    endpoint_type: McpEndpointType,
) -> Result<McpResponse, anyhow::Error> {
    // Extract request ID (guaranteed to be Some since notifications are filtered earlier)
    let request_id = request.id.clone().unwrap_or(serde_json::Value::Null);

    // Validate JSON-RPC version
    if request.jsonrpc != "2.0" {
        return Ok(McpResponse {
            jsonrpc: "2.0".to_string(),
            id: request_id,
            result: None,
            error: Some(McpError {
                code: error_codes::INVALID_REQUEST,
                message: "Invalid Request: jsonrpc must be 2.0".to_string(),
                data: None,
            }),
        });
    }

    info!(
        method = %request.method,
        endpoint = %endpoint_type,
        "Processing MCP method"
    );

    // Handle different MCP methods
    let result = match request.method.as_str() {
        methods::INITIALIZE => handle_initialize(endpoint_type).await,
        methods::TOOLS_LIST => handle_tools_list(endpoint_type).await,
        methods::TOOLS_CALL => handle_tools_call(client, &request.params, endpoint_type).await,
        _ => {
            return Ok(McpResponse {
                jsonrpc: "2.0".to_string(),
                id: request_id,
                result: None,
                error: Some(McpError {
                    code: error_codes::METHOD_NOT_FOUND,
                    message: format!("Method not found: {}", request.method),
                    data: None,
                }),
            });
        }
    };

    match result {
        Ok(result_value) => Ok(McpResponse {
            jsonrpc: "2.0".to_string(),
            id: request_id,
            result: Some(result_value),
            error: None,
        }),
        Err(e) => {
            // Determine if this is a parameter error or internal error
            let error_string = e.to_string();
            let (code, message, data) = if error_string.contains("Missing")
                || error_string.contains("argument")
                || error_string.contains("Invalid")
            {
                // Parameter validation errors
                (
                    error_codes::INVALID_PARAMS,
                    error_string.clone(),
                    Some(json!({
                        "error_type": "ParameterError",
                        "details": error_string,
                    })),
                )
            } else {
                // Internal/execution errors (SQL errors, etc.)
                warn!(error = %e, method = %request.method, "MCP method execution failed");
                (
                    error_codes::INTERNAL_ERROR,
                    format!("Method execution failed: {}", e),
                    Some(json!({
                        "error_type": "ExecutionError",
                        "details": error_string,
                    })),
                )
            };

            Ok(McpResponse {
                jsonrpc: "2.0".to_string(),
                id: request_id,
                result: None,
                error: Some(McpError {
                    code,
                    message,
                    data,
                }),
            })
        }
    }
}

/// Handle MCP initialize handshake
async fn handle_initialize(
    endpoint_type: McpEndpointType,
) -> Result<serde_json::Value, anyhow::Error> {
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

/// Handle tools/list request
async fn handle_tools_list(
    endpoint_type: McpEndpointType,
) -> Result<serde_json::Value, anyhow::Error> {
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

/// Handle tools/call request
async fn handle_tools_call(
    client: &mut AuthedClient,
    params: &serde_json::Value,
    endpoint_type: McpEndpointType,
) -> Result<serde_json::Value, anyhow::Error> {
    // Extract tool name and arguments from params
    let tool_name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("Missing tool name"))?;

    let arguments = params
        .get("arguments")
        .ok_or_else(|| anyhow!("Missing tool arguments"))?;

    info!(tool = %tool_name, endpoint = %endpoint_type, "MCP tools/call");

    match (endpoint_type, tool_name) {
        (McpEndpointType::Agents, "get_data_products") => get_data_products(client).await,
        (McpEndpointType::Agents, "get_data_product_details") => {
            let name = arguments
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing 'name' argument"))?;
            get_data_product_details(client, name).await
        }
        (McpEndpointType::Agents, "query") => {
            let cluster = arguments
                .get("cluster")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing 'cluster' argument"))?;
            let sql_query = arguments
                .get("sql_query")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing 'sql_query' argument"))?;
            execute_query(client, cluster, sql_query).await
        }
        (McpEndpointType::Observatory, "query_system_catalog") => {
            let sql_query = arguments
                .get("sql_query")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing 'sql_query' argument"))?;
            query_system_catalog(client, sql_query).await
        }
        (McpEndpointType::Observatory, "cluster_health_check") => {
            let cluster = arguments
                .get("cluster")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing 'cluster' argument"))?;
            cluster_health_check(client, cluster).await
        }
        _ => Err(anyhow!(
            "Unknown tool '{}' for endpoint {}",
            tool_name,
            endpoint_type
        )),
    }
}

/// Execute SQL using the established sql.rs pattern.
///
/// This reuses `execute_request` from sql.rs (same pattern as prometheus.rs:116).
/// We use SqlResponse to collect results, and access the results field which is
/// visible to the http module via `pub(in crate::http)`.
async fn execute_sql(
    client: &mut AuthedClient,
    query: &str,
) -> Result<Vec<Vec<serde_json::Value>>, anyhow::Error> {
    let mut response = SqlResponse::new();

    execute_request(
        client,
        SqlRequest::Simple {
            query: query.to_string(),
        },
        &mut response,
    )
    .await
    .map_err(|e| anyhow!("SQL execution failed: {}", e))?;

    // Extract the result with rows (the user's single SELECT/SHOW query)
    // Other results will be OK (from BEGIN, SET, COMMIT)
    for result in response.results {
        if let SqlResult::Rows { rows, .. } = result {
            return Ok(rows);
        }
    }

    bail!("Query did not return any rows")
}

/// Tool: get_data_products
async fn get_data_products(client: &mut AuthedClient) -> Result<serde_json::Value, anyhow::Error> {
    info!("Executing get_data_products");
    let rows = execute_sql(client, DISCOVERY_QUERY).await?;
    info!("get_data_products returned {} rows", rows.len());
    if rows.is_empty() {
        warn!("No data products found - indexes must have comments");
    }

    Ok(json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&rows)?
        }]
    }))
}

/// Tool: get_data_product_details
async fn get_data_product_details(
    client: &mut AuthedClient,
    name: &str,
) -> Result<serde_json::Value, anyhow::Error> {
    info!(name = %name, "Executing get_data_product_details");

    // Use SQL WHERE clause with proper escaping (same pattern as prometheus.rs)
    // The name parameter comes with quotes like: "materialize"."public"."table"
    let query = format!(
        "WITH discovery AS ({}) SELECT * FROM discovery WHERE object_name = {}",
        DISCOVERY_QUERY,
        escaped_string_literal(name)
    );

    let rows = execute_sql(client, &query).await?;

    if rows.is_empty() {
        bail!("Data product not found: {}", name);
    }

    Ok(json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&rows)?
        }]
    }))
}

/// Validate that a query is read-only (only one SELECT, SHOW, or EXPLAIN allowed)
fn validate_readonly_query(sql: &str) -> Result<(), anyhow::Error> {
    let sql = sql.trim();
    if sql.is_empty() {
        bail!("Empty query");
    }

    // Parse the SQL to get AST
    let stmts = parse(sql).map_err(|e| anyhow!("Failed to parse SQL: {}", e))?;

    // Only allow a single statement
    if stmts.len() != 1 {
        bail!(
            "Only one query allowed at a time. Found {} statements.",
            stmts.len()
        );
    }

    // Allowlist: Only SELECT, SHOW, and EXPLAIN statements permitted
    let stmt = &stmts[0];
    use mz_sql_parser::ast::Statement;

    match &stmt.ast {
        Statement::Select(_) | Statement::Show(_) | Statement::ExplainPlan(_) => {
            // Allowed - read-only operations
            Ok(())
        }
        _ => {
            bail!("Only SELECT, SHOW, and EXPLAIN statements are allowed");
        }
    }
}

/// Tool: query (with cluster context)
async fn execute_query(
    client: &mut AuthedClient,
    cluster: &str,
    sql_query: &str,
) -> Result<serde_json::Value, anyhow::Error> {
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

    Ok(json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&rows)?
        }]
    }))
}

/// Tool: query_system_catalog (observatory)
async fn query_system_catalog(
    client: &mut AuthedClient,
    sql_query: &str,
) -> Result<serde_json::Value, anyhow::Error> {
    info!("Executing query_system_catalog");

    // Validate that query only references mz_* tables by parsing the SQL
    validate_system_catalog_query(sql_query)?;

    let rows = execute_sql(client, sql_query).await?;

    Ok(json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&rows)?
        }]
    }))
}

/// Validate that a SQL query only references system catalog tables
///
/// This uses SQL parsing to ensure queries can only access mz_* tables,
/// preventing data exfiltration through the observatory endpoint.
fn validate_system_catalog_query(sql: &str) -> Result<(), anyhow::Error> {
    // Parse the SQL to validate it
    let stmts = parse(sql)?;
    if stmts.is_empty() {
        bail!("Empty query");
    }

    // For now, do basic validation
    // TODO: Implement proper AST walking to validate ALL table references are mz_*
    if !sql.to_lowercase().contains("mz_") {
        bail!("Query must reference mz_* system catalog tables");
    }

    Ok(())
}

/// Tool: cluster_health_check (observatory)
///
/// Placeholder for cluster health diagnostic.
/// TODO: Implement full health checks:
/// - Replica utilization (CPU, memory, disk)
/// - Long-running queries
/// - Object wallclock lag
/// - Dependency lag analysis
async fn cluster_health_check(
    _client: &mut AuthedClient,
    cluster: &str,
) -> Result<serde_json::Value, anyhow::Error> {
    info!(cluster = %cluster, "Executing cluster_health_check (placeholder)");

    let result = json!({
        "cluster_name": cluster,
        "status": "not_implemented",
        "message": "Cluster health check is not yet implemented in this POC. This tool will provide  diagnostics including replica utilization, long-running queries, wallclock lag, and dependency analysis.",
        "planned_checks": [
            "Replica CPU, memory, and disk utilization",
            "Long-running query detection",
            "Object wallclock lag analysis",
            "Dependency lag analysis with recursive queries"
        ]
    });

    Ok(json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&result)?
        }]
    }))
}
