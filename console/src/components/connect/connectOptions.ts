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
export const ID_TOKEN_PLACEHOLDER = "<id-token>";
export const PASSWORD_PLACEHOLDER = "<password>";

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

export type McpConfigKind = "cli" | "json" | "toml" | "connector";

export interface McpClient {
  id: McpClientId;
  name: string;
  kind: McpConfigKind;
  /** Where the config goes, used as the final step's title. */
  configLocation: string;
  /** Overrides `configLocation` for the token flow. Used by clients whose
   * token setup goes somewhere else than the OAuth setup. */
  tokenConfigLocation?: string;
  /** How the user completes the browser sign-in for the OAuth flow. Use the
   * function form when the hint names the server. */
  signInHint: string | ((serverName: string) => string);
  /** Whether the client supports one-click install via a deep link. */
  oneClickInstall?: boolean;
}

// Config formats and sign-in behavior verified against each client's docs.
// When a client changes its config schema, this registry is the single place
// to update.
export const MCP_CLIENTS: McpClient[] = [
  {
    id: "claude-code",
    name: "Claude Code",
    kind: "cli",
    configLocation: "Run this in your terminal",
    signInHint:
      "Claude Code opens your browser to sign in on first use. Run /mcp inside Claude Code to sign in right away.",
  },
  {
    id: "cursor",
    name: "Cursor",
    kind: "json",
    configLocation: "Add to ~/.cursor/mcp.json",
    signInHint:
      "Cursor prompts you to sign in when the server first connects. You can also click the server under Settings > MCP.",
    oneClickInstall: true,
  },
  {
    // Claude Desktop's config file only launches local stdio servers. Remote
    // servers are added as custom connectors in the app for OAuth, or through
    // the mcp-remote proxy for the token flow.
    id: "claude-desktop",
    name: "Claude Desktop",
    kind: "connector",
    configLocation: "Add as a custom connector under Settings > Connectors",
    tokenConfigLocation: "Add to claude_desktop_config.json",
    signInHint:
      "Claude Desktop opens your browser to sign in when you add the connector.",
  },
  {
    id: "vscode",
    name: "VS Code",
    kind: "json",
    configLocation: "Add to .vscode/mcp.json",
    signInHint:
      "VS Code prompts you to authenticate the server the first time chat uses it.",
    oneClickInstall: true,
  },
  {
    id: "windsurf",
    name: "Windsurf",
    kind: "json",
    configLocation: "Add to ~/.codeium/windsurf/mcp_config.json",
    signInHint:
      "Windsurf opens your browser to sign in when the server first connects.",
  },
  {
    id: "codex",
    name: "Codex CLI",
    kind: "toml",
    configLocation: "Add to ~/.codex/config.toml",
    signInHint: (serverName) =>
      `Run codex mcp login ${serverName} to sign in through your browser.`,
  },
  {
    id: "other",
    name: "Other",
    kind: "json",
    configLocation: "Add to your client's MCP config",
    signInHint: "Your client opens a browser to sign in on first connect.",
  },
];

export const resolveSignInHint = (client: McpClient, serverName: string) =>
  typeof client.signInHint === "function"
    ? client.signInHint(serverName)
    : client.signInHint;

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

const buildJsonSnippet = ({
  client,
  server,
  baseUrl,
  token,
}: McpSnippetOptions) => {
  const url = mcpServerUrl(server, baseUrl);
  const headers = token
    ? { headers: { Authorization: `Basic ${token}` } }
    : undefined;
  // Each client expects a slightly different JSON schema for remote servers.
  let config: Record<string, unknown>;
  switch (client.id) {
    // VS Code roots at "servers" and needs an explicit transport type,
    // otherwise it treats the entry as a stdio command.
    case "vscode":
      config = {
        servers: { [server.serverName]: { type: "http", url, ...headers } },
      };
      break;
    // Windsurf's canonical key for remote servers is "serverUrl".
    case "windsurf":
      config = {
        mcpServers: { [server.serverName]: { serverUrl: url, ...headers } },
      };
      break;
    default:
      config = { mcpServers: { [server.serverName]: { url, ...headers } } };
  }
  return JSON.stringify(config, null, 2);
};

/** Claude Desktop: the OAuth flow pastes the URL into the connectors UI, the
 * token flow proxies through mcp-remote since the config file is stdio-only. */
const buildConnectorSnippet = ({
  server,
  baseUrl,
  token,
}: McpSnippetOptions) => {
  const url = mcpServerUrl(server, baseUrl);
  if (!token) return url;
  return JSON.stringify(
    {
      mcpServers: {
        [server.serverName]: {
          command: "npx",
          args: [
            "mcp-remote",
            url,
            "--header",
            `Authorization: Basic ${token}`,
          ],
        },
      },
    },
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
    case "connector":
      return buildConnectorSnippet(options);
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

/** Command to Base64-encode the user and their credential for the MCP token
 * flow. The credential differs by deployment: an app password on cloud, the
 * OIDC ID token or the login password on self-managed. */
export const buildBase64TokenCommand = (
  user: string,
  credentialPlaceholder: string = APP_PASSWORD_PLACEHOLDER,
) => `printf '%s' '${user}:${credentialPlaceholder}' | base64 -w0`;
