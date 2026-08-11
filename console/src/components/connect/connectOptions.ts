// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { toBase64 } from "~/utils/format";

export const MCP_TOKEN_PLACEHOLDER = "<mcp-token>";
export const APP_PASSWORD_PLACEHOLDER = "<app-password>";

/** Deployment-derived inputs the connect panels render from. */
export interface ConnectContext {
  isCloud: boolean;
  /** SQL user shown in connection strings. */
  user: string;
  host: string;
  port: string;
  database: string;
  /** Whether the SQL endpoint requires TLS. */
  ssl: boolean;
  /** Scheme plus host serving the MCP endpoints, without a trailing slash. */
  mcpBaseUrl: string;
  /** Whether MCP clients can sign in through the browser instead of using a
   * Basic-auth token. */
  oauthAvailable: boolean;
  /** Cluster to preselect in the psql connection string. */
  clusterName?: string;
  /** Whether the panel offers creating app passwords (cloud sessions only). */
  canCreateAppPassword: boolean;
  /** Self-managed OIDC ID token, used as the SQL password. */
  idToken?: string;
}

export type McpServerId = "developer" | "agent";

export interface McpServer {
  id: McpServerId;
  label: string;
  blurb: string;
  /** Name the server is registered under in the client's MCP config. */
  serverName: string;
  /** Path of the MCP endpoint on the environment's HTTP address. */
  path: string;
}

export const MCP_SERVERS: McpServer[] = [
  {
    id: "developer",
    label: "Developer",
    blurb: "Troubleshooting & observability",
    serverName: "materialize-developer",
    path: "/api/mcp/developer",
  },
  {
    id: "agent",
    label: "Agent",
    blurb: "Query your data products",
    serverName: "materialize-agent",
    path: "/api/mcp/agent",
  },
];

export type McpClientId =
  | "claude-code"
  | "cursor"
  | "claude-desktop"
  | "vscode"
  | "windsurf"
  | "codex"
  | "other";

export type McpConfigKind = "cli" | "json" | "toml";

export interface McpClient {
  id: McpClientId;
  name: string;
  kind: McpConfigKind;
  /** Instruction shown above the config snippet, e.g. where the file lives. */
  configLocation: string;
  /** Whether the client supports one-click install via a deep link. */
  oneClickInstall?: boolean;
}

export const MCP_CLIENTS: McpClient[] = [
  {
    id: "claude-code",
    name: "Claude Code",
    kind: "cli",
    configLocation: "Run this in your terminal",
  },
  {
    id: "cursor",
    name: "Cursor",
    kind: "json",
    configLocation: "Add to ~/.cursor/mcp.json",
    oneClickInstall: true,
  },
  {
    id: "claude-desktop",
    name: "Claude Desktop",
    kind: "json",
    configLocation: "Add to claude_desktop_config.json",
  },
  {
    id: "vscode",
    name: "VS Code",
    kind: "json",
    configLocation: "Add to .vscode/mcp.json",
    oneClickInstall: true,
  },
  {
    id: "windsurf",
    name: "Windsurf",
    kind: "json",
    configLocation: "Add to ~/.codeium/windsurf/mcp_config.json",
  },
  {
    id: "codex",
    name: "Codex CLI",
    kind: "toml",
    configLocation: "Add to ~/.codex/config.toml",
  },
  {
    id: "other",
    name: "Other",
    kind: "json",
    configLocation: "Add to your client's MCP config",
  },
];

export interface McpSnippetOptions {
  client: McpClient;
  server: McpServer;
  /** Scheme plus host serving the MCP endpoints, without a trailing slash. */
  baseUrl: string;
  /** Base64 `user:app-password` token. Omitted for the browser OAuth flow. */
  token?: string;
}

const mcpServerUrl = (server: McpServer, baseUrl: string) =>
  `${baseUrl}${server.path}`;

const buildCliSnippet = ({ server, baseUrl, token }: McpSnippetOptions) => {
  const header = token ? ` \\\n  --header "Authorization: Basic ${token}"` : "";
  return `claude mcp add --transport http ${server.serverName} \\\n  ${mcpServerUrl(server, baseUrl)}${header}`;
};

const buildJsonSnippet = ({ server, baseUrl, token }: McpSnippetOptions) => {
  const entry: Record<string, unknown> = {
    url: mcpServerUrl(server, baseUrl),
  };
  if (token) {
    entry.headers = { Authorization: `Basic ${token}` };
  }
  return JSON.stringify(
    { mcpServers: { [server.serverName]: entry } },
    null,
    2,
  );
};

const buildTomlSnippet = ({ server, baseUrl, token }: McpSnippetOptions) => {
  const header = token
    ? `\nhttp_headers = { "Authorization" = "Basic ${token}" }`
    : "";
  const tableName = server.serverName.replace(/-/g, "_");
  return `[mcp_servers.${tableName}]\nurl = "${mcpServerUrl(server, baseUrl)}"${header}`;
};

/** Builds the client-specific MCP config snippet, using
 * `MCP_TOKEN_PLACEHOLDER` when the token flow is active but no token has been
 * generated yet. */
export const buildMcpSnippet = (options: McpSnippetOptions): string => {
  switch (options.client.kind) {
    case "cli":
      return buildCliSnippet(options);
    case "toml":
      return buildTomlSnippet(options);
    case "json":
      return buildJsonSnippet(options);
  }
};

/** One-click install link for clients that register a URL scheme handler.
 * Only built for the OAuth flow so tokens never end up in URLs. */
export const buildMcpInstallLink = (
  client: McpClient,
  server: McpServer,
  baseUrl: string,
): string | undefined => {
  const config = { url: mcpServerUrl(server, baseUrl) };
  switch (client.id) {
    case "cursor":
      return `cursor://anysphere.cursor-deeplink/mcp/install?name=${encodeURIComponent(
        server.serverName,
      )}&config=${encodeURIComponent(toBase64(JSON.stringify(config)))}`;
    case "vscode":
      return `vscode:mcp/install?${encodeURIComponent(
        JSON.stringify({ name: server.serverName, type: "http", ...config }),
      )}`;
    default:
      return undefined;
  }
};

export const AGENT_SKILLS_COMMAND =
  "npx skills add MaterializeInc/agent-skills";

export interface ConnectionDetails {
  host: string;
  port: string;
  database: string;
  user: string;
  /** Actual app password once created, otherwise callers show a placeholder. */
  password?: string;
  /** Whether the endpoint requires TLS (cloud always does). */
  ssl: boolean;
}

export type ExternalToolId = "dbeaver" | "dbt" | "env";

export interface ExternalTool {
  id: ExternalToolId;
  label: string;
  /** Instruction shown above the snippet. */
  instruction: string;
  buildSnippet: (details: ConnectionDetails) => string;
}

const passwordOrPlaceholder = (details: ConnectionDetails) =>
  details.password ?? APP_PASSWORD_PLACEHOLDER;

export const EXTERNAL_TOOLS: ExternalTool[] = [
  {
    id: "dbeaver",
    label: "DBeaver",
    instruction:
      "In DBeaver, create a new PostgreSQL connection with these settings:",
    buildSnippet: (details) =>
      [
        `Host      ${details.host}`,
        `Port      ${details.port}`,
        `Database  ${details.database}`,
        `User      ${details.user}`,
        `Password  ${passwordOrPlaceholder(details)}`,
        `SSL       ${details.ssl ? "require" : "disable"}`,
      ].join("\n"),
  },
  {
    id: "dbt",
    label: "dbt",
    instruction: "Add this profile to your dbt profiles.yml:",
    buildSnippet: (details) =>
      [
        "materialize:",
        "  target: prod",
        "  outputs:",
        "    prod:",
        "      type: materialize",
        `      host: ${details.host}`,
        `      port: ${details.port}`,
        `      user: ${details.user}`,
        `      pass: ${passwordOrPlaceholder(details)}`,
        `      database: ${details.database}`,
        `      sslmode: ${details.ssl ? "require" : "disable"}`,
      ].join("\n"),
  },
  {
    id: "env",
    label: ".env",
    instruction: "Copy these variables into your .env file:",
    buildSnippet: (details) =>
      [
        `MZ_HOST=${details.host}`,
        `MZ_PORT=${details.port}`,
        `MZ_DATABASE=${details.database}`,
        `MZ_USER=${details.user}`,
        `MZ_PASSWORD=${passwordOrPlaceholder(details)}`,
        `DATABASE_URL=postgres://${encodeURIComponent(details.user)}:${passwordOrPlaceholder(details)}@${details.host}:${details.port}/${details.database}${details.ssl ? "?sslmode=require" : ""}`,
      ].join("\n"),
  },
];

/** Builds the psql connection command.
 *
 * NOTE(benesch): We'd like to use `sslmode=verify-full` to prevent MITM
 * attacks, but that mode requires specifying `sslrootcert=/path/to/cabundle`,
 * and that path varies by platform. So instead we use `require`, which is
 * at least better than the default of `prefer`. */
export const buildPsqlCommand = (
  details: Pick<
    ConnectionDetails,
    "host" | "port" | "database" | "user" | "ssl"
  >,
  clusterName?: string,
): string => {
  const params: string[] = [];
  if (details.ssl) {
    params.push("sslmode=require");
  }
  if (clusterName) {
    params.push(`options=--cluster%3D${clusterName}`);
  }
  const query = params.length > 0 ? `?${params.join("&")}` : "";
  return `psql "postgres://${encodeURIComponent(details.user)}@${details.host}:${details.port}/${details.database}${query}"`;
};

/** Command to Base64-encode `user:app-password` for the MCP token flow. */
export const buildBase64TokenCommand = (user: string) =>
  `printf '%s' '${user}:${APP_PASSWORD_PLACEHOLDER}' | base64 -w0`;
