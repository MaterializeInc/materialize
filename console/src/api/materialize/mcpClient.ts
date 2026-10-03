// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * MCP (Model Context Protocol) client for Materialize's built-in MCP servers.
 *
 * Talks to the developer endpoint (`/api/mcp/developer`) for system catalog
 * diagnostics, and the agent endpoint (`/api/mcp/agent`) for data product
 * discovery. Uses the same auth as the SQL API.
 */

import { getStore } from "~/jotai";
import { currentEnvironmentState } from "~/store/environments";
import { assert } from "~/util";

import { apiClient } from "../apiClient";

// JSON-RPC 2.0 types

interface McpRequest {
  jsonrpc: "2.0";
  id: number;
  method: string;
  params?: Record<string, unknown>;
}

interface McpSuccessResponse {
  jsonrpc: "2.0";
  id: number;
  result: {
    content?: Array<{ type: string; text: string }>;
    tools?: Array<{
      name: string;
      description: string;
      inputSchema: Record<string, unknown>;
    }>;
    protocolVersion?: string;
    capabilities?: Record<string, unknown>;
    serverInfo?: { name: string; version: string };
  };
}

interface McpErrorResponse {
  jsonrpc: "2.0";
  id: number;
  error: {
    code: number;
    message: string;
    data?: Record<string, unknown>;
  };
}

type McpResponse = McpSuccessResponse | McpErrorResponse;

export class McpError extends Error {
  code: number;
  constructor(code: number, message: string) {
    super(message);
    this.name = "McpError";
    this.code = code;
  }
}

type McpEndpoint = "developer" | "agent";

let requestId = 0;

async function getHttpAddress(): Promise<string> {
  const environment = await getStore().get(currentEnvironmentState);
  assert(environment && environment.state === "enabled");
  return environment.httpAddress;
}

function buildMcpUrl(httpAddress: string, endpoint: McpEndpoint): string {
  return `${apiClient.mzHttpUrlScheme}://${httpAddress}/api/mcp/${endpoint}`;
}

async function mcpRequest(
  endpoint: McpEndpoint,
  method: string,
  params?: Record<string, unknown>,
): Promise<McpSuccessResponse["result"]> {
  const httpAddress = await getHttpAddress();
  const url = buildMcpUrl(httpAddress, endpoint);

  const body: McpRequest = {
    jsonrpc: "2.0",
    id: ++requestId,
    method,
    ...(params !== undefined && { params }),
  };

  const response = await apiClient.mzApiFetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60_000),
  });

  if (!response.ok) {
    if (response.status === 503) {
      throw new McpError(
        -1,
        "MCP endpoint is disabled. Enable it via system parameter.",
      );
    }
    const text = await response.text();
    throw new McpError(-1, text || `HTTP ${response.status}`);
  }

  const json: McpResponse = await response.json();

  if ("error" in json && json.error) {
    throw new McpError(json.error.code, json.error.message);
  }

  return (json as McpSuccessResponse).result;
}

// Developer tools

/** Query system catalog tables (mz_*) via the developer MCP server. */
export async function mcpQuerySystemCatalog(sqlQuery: string): Promise<string> {
  const result = await mcpRequest("developer", "tools/call", {
    name: "query_system_catalog",
    arguments: { sql_query: sqlQuery },
  });
  const text = result.content?.[0]?.text;
  if (!text) {
    throw new McpError(-1, "Empty response from developer");
  }
  return text;
}

// Agent tools

/** Discover available data products via the agents MCP server. */
export async function mcpGetDataProducts(): Promise<string> {
  const result = await mcpRequest("agent", "tools/call", {
    name: "get_data_products",
    arguments: {},
  });
  const text = result.content?.[0]?.text;
  if (!text) {
    throw new McpError(-1, "Empty response from agents endpoint");
  }
  return text;
}

/** Get detailed schema for a specific data product. */
export async function mcpGetDataProductDetails(name: string): Promise<string> {
  const result = await mcpRequest("agent", "tools/call", {
    name: "get_data_product_details",
    arguments: { name },
  });
  const text = result.content?.[0]?.text;
  if (!text) {
    throw new McpError(-1, "Empty response from agents endpoint");
  }
  return text;
}

/** List available tools on an MCP endpoint. */
export async function mcpListTools(endpoint: McpEndpoint) {
  const result = await mcpRequest(endpoint, "tools/list");
  return result.tools ?? [];
}
