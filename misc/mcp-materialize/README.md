# Materialize MCP Server

**Instantly turn indexed views in Materialize into real-time context tools for LLM-powered applications.**

Materialize MCP Server exposes your Materialize views—when indexed and documented—as live, typed, callable tools. These tools behave like stable APIs for structured data, enabling models to act on fresh, consistent, and trustworthy information.
No pipelines, no glue code, no stale caches.

---

## ✨ What Are Operational Data Products?

Views + indexes + comments = **Operational Data Products**:
Self-contained, versioned services that model real-world concepts and provide **fast, reliable, and testable** access to dynamic data.

| Feature        | Benefit                                                               |
| -------------- | --------------------------------------------------------------------- |
| **Stable**     | Define once, use repeatedly across use cases.                         |
| **Typed**      | Input/output schemas inferred directly from indexes.                  |
| **Observable** | Tool usage is logged per client, revealing real cost and performance. |
| **Secure**     | If it’s not indexed and documented, it’s not exposed.                 |

---

## 🚀 Quickstart

```bash
uv run mcp-materialize
```

This launches the server with default settings and immediately exposes any indexed views as tools.

---

## ⚙️ Configuration

The server can be configured via CLI flags or environment variables:

| Argument          | Env Var             | Default                                               | Description                                |
| ----------------- | ------------------- | ----------------------------------------------------- | ------------------------------------------ |
| `--mz-dsn`        | `MZ_DSN`            | `postgresql://materialize@localhost:6875/materialize` | Materialize connection string              |
| `--transport`     | `MCP_TRANSPORT`     | `stdio`                                               | Communication transport (`stdio` or `sse`) |
| `--host`          | `MCP_HOST`          | `0.0.0.0`                                             | Host address                               |
| `--port`          | `MCP_PORT`          | `3001`                                                | Port number                                |
| `--pool-min-size` | `MCP_POOL_MIN_SIZE` | `1`                                                   | Minimum DB pool size                       |
| `--pool-max-size` | `MCP_POOL_MAX_SIZE` | `10`                                                  | Maximum DB pool size                       |
| `--log-level`     | `MCP_LOG_LEVEL`     | `INFO`                                                | Log verbosity                              |

---

## 🛠 Defining a Tool

1. **Write a view** that captures your business logic.
2. **Create an index** on its primary lookup key.
3. **Document it** with `COMMENT` statements.

```sql
CREATE VIEW order_status_summary AS
SELECT o.order_id, o.status, s.carrier, c.estimated_delivery, e.delay_reason
FROM orders o
LEFT JOIN shipments s            ON o.order_id = s.order_id
LEFT JOIN carrier_tracking c     ON s.shipment_id = c.shipment_id
LEFT JOIN delivery_exceptions e ON c.tracking_id = e.tracking_id;

CREATE INDEX ON order_status_summary (order_id);

COMMENT ON VIEW order_status_summary IS
  'Given an order ID, retrieve the current status, shipping carrier, estimated delivery date, and any delivery exceptions. Use this tool to show real-time order tracking information to users.';

COMMENT ON COLUMN order_status_summary.order_id IS
  'The unique id for an order';
```

Now, this tool appears in `/tools/list`:

```json
{
  "name": "order_status_summary",
  "description": "Given an order ID, retrieve the current status, shipping carrier, estimated delivery date, and any delivery exceptions. Use this tool to show real-time order tracking information to users.",
  "inputSchema": {
    "type": "object",
    "required": ["order_id"],
    "properties": {
      "order_id": {
        "type": "text",
        "description": "The unique id for an order"
      }
    }
  }
}
```
