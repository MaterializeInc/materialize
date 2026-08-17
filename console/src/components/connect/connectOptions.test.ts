// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  buildBase64TokenCommand,
  buildMcpInstallLink,
  buildMcpSnippet,
  buildPsqlCommand,
  EXTERNAL_TOOLS,
  MCP_CLIENTS,
  MCP_SERVERS,
  McpClientId,
  McpServerId,
} from "./connectOptions";

const BASE_URL = "https://region.example.com";

const server = (id: McpServerId) => {
  const found = MCP_SERVERS.find((candidate) => candidate.id === id);
  if (!found) throw new Error(`unknown server ${id}`);
  return found;
};

const client = (id: McpClientId) => {
  const found = MCP_CLIENTS.find((candidate) => candidate.id === id);
  if (!found) throw new Error(`unknown client ${id}`);
  return found;
};

const tool = (id: string) => {
  const found = EXTERNAL_TOOLS.find((candidate) => candidate.id === id);
  if (!found) throw new Error(`unknown tool ${id}`);
  return found;
};

const CONNECTION_DETAILS = {
  host: "region.example.com",
  port: "6875",
  database: "materialize",
  user: "user@example.com",
  ssl: true,
};

describe("buildMcpSnippet", () => {
  it("builds a claude mcp add command for the OAuth flow", () => {
    const snippet = buildMcpSnippet({
      client: client("claude-code"),
      server: server("developer"),
      baseUrl: BASE_URL,
    });
    expect(snippet).toEqual(
      `claude mcp add --transport http materialize-developer \\\n  ${BASE_URL}/api/mcp/developer`,
    );
  });

  it("adds a Basic auth header to the command for the token flow", () => {
    const snippet = buildMcpSnippet({
      client: client("claude-code"),
      server: server("agent"),
      baseUrl: BASE_URL,
      token: "dG9rZW4=",
    });
    expect(snippet).toContain(`${BASE_URL}/api/mcp/agent`);
    expect(snippet).toContain('--header "Authorization: Basic dG9rZW4="');
  });

  it("builds valid mcpServers JSON for JSON-configured clients", () => {
    const snippet = buildMcpSnippet({
      client: client("cursor"),
      server: server("developer"),
      baseUrl: BASE_URL,
      token: "dG9rZW4=",
    });
    expect(JSON.parse(snippet)).toEqual({
      mcpServers: {
        "materialize-developer": {
          url: `${BASE_URL}/api/mcp/developer`,
          headers: { Authorization: "Basic dG9rZW4=" },
        },
      },
    });
  });

  it("omits headers from JSON configs for the OAuth flow", () => {
    const snippet = buildMcpSnippet({
      client: client("cursor"),
      server: server("developer"),
      baseUrl: BASE_URL,
    });
    expect(JSON.parse(snippet)).toEqual({
      mcpServers: {
        "materialize-developer": {
          url: `${BASE_URL}/api/mcp/developer`,
        },
      },
    });
  });

  it("roots VS Code configs at servers with an explicit http type", () => {
    const snippet = buildMcpSnippet({
      client: client("vscode"),
      server: server("developer"),
      baseUrl: BASE_URL,
      token: "dG9rZW4=",
    });
    expect(JSON.parse(snippet)).toEqual({
      servers: {
        "materialize-developer": {
          type: "http",
          url: `${BASE_URL}/api/mcp/developer`,
          headers: { Authorization: "Basic dG9rZW4=" },
        },
      },
    });
  });

  it("uses serverUrl for Windsurf configs", () => {
    const snippet = buildMcpSnippet({
      client: client("windsurf"),
      server: server("developer"),
      baseUrl: BASE_URL,
    });
    expect(JSON.parse(snippet)).toEqual({
      mcpServers: {
        "materialize-developer": {
          serverUrl: `${BASE_URL}/api/mcp/developer`,
        },
      },
    });
  });

  it("shows just the URL for Claude Desktop's OAuth connector flow", () => {
    const snippet = buildMcpSnippet({
      client: client("claude-desktop"),
      server: server("developer"),
      baseUrl: BASE_URL,
    });
    expect(snippet).toEqual(`${BASE_URL}/api/mcp/developer`);
  });

  it("proxies Claude Desktop's token flow through mcp-remote", () => {
    const snippet = buildMcpSnippet({
      client: client("claude-desktop"),
      server: server("developer"),
      baseUrl: BASE_URL,
      token: "dG9rZW4=",
    });
    expect(JSON.parse(snippet)).toEqual({
      mcpServers: {
        "materialize-developer": {
          command: "npx",
          args: [
            "mcp-remote",
            `${BASE_URL}/api/mcp/developer`,
            "--header",
            "Authorization: Basic dG9rZW4=",
          ],
        },
      },
    });
  });

  it("builds TOML with underscored table names for Codex", () => {
    const snippet = buildMcpSnippet({
      client: client("codex"),
      server: server("agent"),
      baseUrl: BASE_URL,
      token: "dG9rZW4=",
    });
    expect(snippet).toEqual(
      `[mcp_servers.materialize_agent]\nurl = "${BASE_URL}/api/mcp/agent"\nhttp_headers = { "Authorization" = "Basic dG9rZW4=" }`,
    );
  });
});

describe("buildMcpInstallLink", () => {
  it("builds a Cursor deep link with Base64-encoded config", () => {
    const link = buildMcpInstallLink(
      client("cursor"),
      server("developer"),
      BASE_URL,
    );
    expect(link).toBeDefined();
    const url = new URL(link ?? "");
    expect(url.protocol).toEqual("cursor:");
    expect(url.searchParams.get("name")).toEqual("materialize-developer");
    const config = JSON.parse(atob(url.searchParams.get("config") ?? ""));
    expect(config).toEqual({ url: `${BASE_URL}/api/mcp/developer` });
  });

  it("builds a VS Code deep link with URL-encoded config", () => {
    const link = buildMcpInstallLink(
      client("vscode"),
      server("agent"),
      BASE_URL,
    );
    expect(link).toBeDefined();
    const [scheme, encoded] = (link ?? "").split("?");
    expect(scheme).toEqual("vscode:mcp/install");
    expect(JSON.parse(decodeURIComponent(encoded))).toEqual({
      name: "materialize-agent",
      type: "http",
      url: `${BASE_URL}/api/mcp/agent`,
    });
  });

  it("returns undefined for clients without one-click install", () => {
    expect(
      buildMcpInstallLink(client("claude-code"), server("developer"), BASE_URL),
    ).toBeUndefined();
  });
});

describe("EXTERNAL_TOOLS", () => {
  it("shows a placeholder password until one is created", () => {
    const snippet = tool("dbeaver").buildSnippet(CONNECTION_DETAILS);
    expect(snippet).toContain("Password  <app-password>");
    expect(snippet).toContain("SSL       require");
  });

  it("substitutes a created app password", () => {
    const snippet = tool("dbt").buildSnippet({
      ...CONNECTION_DETAILS,
      password: "mzp_abc123",
    });
    expect(snippet).toContain("pass: mzp_abc123");
    expect(snippet).toContain("sslmode: require");
  });

  it("builds a DATABASE_URL with the user encoded", () => {
    const snippet = tool("env").buildSnippet({
      ...CONNECTION_DETAILS,
      password: "mzp_abc123",
    });
    expect(snippet).toContain(
      "DATABASE_URL=postgres://user%40example.com:mzp_abc123@region.example.com:6875/materialize?sslmode=require",
    );
  });

  it("disables SSL for deployments without TLS", () => {
    const snippet = tool("dbeaver").buildSnippet({
      ...CONNECTION_DETAILS,
      ssl: false,
    });
    expect(snippet).toContain("SSL       disable");
  });
});

describe("buildPsqlCommand", () => {
  it("encodes the user and requires SSL", () => {
    expect(buildPsqlCommand(CONNECTION_DETAILS)).toEqual(
      'psql "postgres://user%40example.com@region.example.com:6875/materialize?sslmode=require"',
    );
  });

  it("appends the cluster option when set", () => {
    expect(buildPsqlCommand(CONNECTION_DETAILS, "quickstart")).toContain(
      "sslmode=require&options=--cluster%3Dquickstart",
    );
  });

  it("omits query params without TLS or cluster", () => {
    expect(buildPsqlCommand({ ...CONNECTION_DETAILS, ssl: false })).toEqual(
      'psql "postgres://user%40example.com@region.example.com:6875/materialize"',
    );
  });
});

describe("buildBase64TokenCommand", () => {
  it("embeds the user with an app password placeholder", () => {
    expect(buildBase64TokenCommand("user@example.com")).toEqual(
      "printf '%s' 'user@example.com:<app-password>' | base64 -w0",
    );
  });
});
