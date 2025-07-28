---
title: Large Language Models
description: "Learn how to integrate Materialize with Large Language Models (LLMs) using MCP"
make_table_row_headers_searchable: true
menu:
  main:
    parent: "integrations"
    weight: 24
---

The [Model Context Protocol (MCP) Server for Materialize](https://materialize.com/blog/materialize-turns-views-into-tools-for-agents/) lets large language models (LLMs) call your indexed views as real-time tools.
The MCP Server automatically turns any indexed view with a comment into a callable, typed interface that LLMs can use to fetch structured, up-to-date answers—directly from the database.

These tools behave like stable APIs.
They're governed by your SQL privileges, kept fresh by Materialize's incremental view maintenance, and ready to power applications that rely on live context instead of static embeddings or unpredictable prompt chains.

## Get Started

We recommend using [uv](https://docs.astral.sh/uv/) to install and run the server.
It provides fast, reliable Python environments with dependency resolution that matches pip.

If you don't have uv installed, you can install it first:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

To install and launch the MCP Server for Materialize:

```bash
uv venv
uv pip install mcp-materialize
uv run mcp-materialize
```

You can configure it using CLI flags or environment variables:

| Flag              | Env Var             | Default                                               | Description                                   |
| ----------------- | ------------------- | ----------------------------------------------------- | --------------------------------------------- |
| `--mz-dsn`        | `MZ_DSN`            | `postgres://materialize@localhost:6875/materialize`   | Materialize connection string                 |
| `--transport`     | `MCP_TRANSPORT`     | `stdio`                                               | Communication mode (`stdio`, `sse`, or `http`) |
| `--host`          | `MCP_HOST`          | `0.0.0.0`                                             | Host for `sse` and `http` modes               |
| `--port`          | `MCP_PORT`          | `3001` (sse), `8001` (http)                           | Port for `sse` and `http` modes               |
| `--pool-min-size` | `MCP_POOL_MIN_SIZE` | `1`                                                   | Minimum DB pool size                          |
| `--pool-max-size` | `MCP_POOL_MAX_SIZE` | `10`                                                  | Maximum DB pool size                          |
| `--log-level`     | `MCP_LOG_LEVEL`     | `INFO`                                                | Logging verbosity                             |


## Define Tools

Any view in Materialize can become a callable tool as long as it meets a few requirements to ensure that the tool is fast to query, safe to expose, and easy for language models to use correctly.

- [The view is indexed.](#1-define-and-index)
- [The view includes a top level comment.](#2-comment)
- [The role used to run the MCP Server must have required privileges.](#3-set-rbac-permissions)

### 1. Define and Index

You must create at least one [index](/concepts/indexes/) on the view. The columns in the index define the required input fields for the tool.

You can index a single column:

```mzsql
CREATE INDEX ON payment_status_summary (order_id);
```

Or multiple columns:

```mzsql
CREATE INDEX ON payment_status_summary (user_id, order_id);
```

Every indexed column becomes part of the tool's input schema.

### 2. Comment

The view must include a top-level comment that is used as the tool's description.
Comments should be descriptive as they help the model reason about what the tool does and when to use it.
You can optionally add a comment on any of the indexed columns to improve the tool's schema with descriptions for each field.

```mzsql
COMMENT ON VIEW payment_status_summary IS
  'Given a user ID and order ID, return the current payment status and last update time.
   Use this tool to drive user-facing payment tracking.';

COMMENT ON COLUMN payment_status_summary.user_id IS
  'The ID of the user who placed the order';

COMMENT ON COLUMN payment_status_summary.order_id IS
  'The unique identifier for the order';
```

### 3. Set RBAC Permissions

The database role used to run the MCP Server must:

* Have `USAGE` privileges on the database and schema the view is in.
* Have `SELECT` privileges on the view.
* Have `USAGE` privileges on the cluster where the index is installed.

```mzsql
GRANT USAGE on DATABASE materialize TO mcp_server_role;
GRANT USAGE on SCHEMA materialize.public TO mcp_server_role;
GRANT SELECT ON payment_status_summary TO mcp_server_role;
GRANT USAGE ON CLUSTER mcp_cluster TO mcp_server_role;
```

## Related Pages

* [CREATE VIEW](/sql/create-view)
* [CREATE INDEX](/sql/create-index)
* [COMMENT ON](/sql/comment-on)
* [CREATE ROLE](/sql/create-role)
* [GRANT PRIVILEGE](/sql/grant-privilege)
