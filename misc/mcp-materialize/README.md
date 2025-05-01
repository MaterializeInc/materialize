# Materialize MCP Server

Instantly turn your Materialize indexed views into live context providers for Retrieval‑Augmented Generation (RAG) pipelines and LLM-driven applications.
By defining views and indexes, you create typed, fast, and consistent data tools. No extra services or stale caches required.

These live, indexed views form operational data products.
Self-contained, discoverable services that deliver real-time context instantly.

## Benefits of Operational Data Products

* **Stable:** define once, used repeatedly, ensuring consistent business logic.
* **Typed:** input and output schemas are derived from the index.
* **Observable:** usage is logged per‑tool, making cost and performance explicit.
* **Secure:** if you don't create a view/index, it isn't callable.

## Quickstart

Run the server with default settings:

```bash
uv run materialize-mcp-server
```

## Configuration


| Argument | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `--mz-dsn` | `MZ_DSN` | `postgresql://materialize@localhost:6875/materialize` | Materialize DSN |
| `--transport` | `MCP_TRANSPORT` | `stdio` | Communication transport (`stdio` or `sse`) |
| `--host` | `MCP_HOST` | `0.0.0.0` | Server host |
| `--port` | `MCP_PORT` | `3001` | Server port |
| `--pool-min-size` | `MCP_POOL_MIN_SIZE` | `1` | Minimum connection pool size |
| `--pool-max-size` | `MCP_POOL_MAX_SIZE` | `10` | Maximum connection pool size |
| `--log-level` | `MCP_LOG_LEVEL` | `INFO` | Logging level |


## Defining a Tool

1. **Write a view** in SQL that captures your live data logic.
2. **Create an index** on the key columns for lightning-fast lookups.
3. **Add a COMMENT** to describe the tool for LLM discoverability.

```sql
CREATE VIEW order_status_summary AS
SELECT  o.order_id,
        o.status,
        s.carrier,
        c.estimated_delivery,
        e.delay_reason
FROM orders o
LEFT JOIN shipments           s ON o.order_id = s.order_id
LEFT JOIN carrier_tracking    c ON s.shipment_id = c.shipment_id
LEFT JOIN delivery_exceptions e ON c.tracking_id = e.tracking_id;

CREATE INDEX ON order_status_summary (order_id);

COMMENT ON VIEW order_status_summary IS
  'Given an order ID, retrieve the current status, shipping carrier, estimated delivery date, and any delivery exceptions. Use this tool to show real-time order tracking information to users.';

COMMENT ON COLUMN order_status_summary.order_id IS 'The unique id for an order';
```

Refresh the server and the tool now appears in `tools/list`:

```json
{
  "name": "order_status_summary",
  "description": "Given an order ID, retrieve teh current status, shipping carrier, estimated delivery date, and any delivery exceptions. Use this tool to show real-time order tracking information to users.",
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

## Example 1: Personalized Delivery Context

Imagine you run an e-commerce site where localized inventory data shifts constantly due to in-store purchases, online orders, and warehouse replenishments.
With Materialize, you can join live inventory updates from Kafka with real-time membership data from a Postgres database.
Your AI-driven chat assistant, armed with this consolidated view, can instantly tell a shopper how many items remain, delivery fees (if any), and estimated arrival times specific to their location and account status.

> User: How many Deluxe Espresso Machines can I get delivered to Brooklyn by tomorrow?
>
> Assistant: We have 7 in stock. As a Gold member, you qualify for free expedited shipping — your machine will arrive tomorrow!

```sql
CREATE VIEW personalized_delivery_context AS
WITH live_inventory AS (
  SELECT p.product_id, i.warehouse, i.quantity_available
  FROM products p
  JOIN inventory_events i
    ON p.product_id = i.product_id
  WHERE i.event_time +  INTERVAL '5 minutes' >= mz_now()
),
membership_info AS (
  SELECT c.customer_id, m.tier, c.region
  FROM customers c
  JOIN memberships m
    ON c.customer_id = m.customer_id
  WHERE m.expires_at > mz_now()
),
shipping_rates AS (
  SELECT region, standard_days, expedited_days, free_expedited_for_tier
  FROM shipping_policies
)
SELECT
  li.product_id,
  SUM(li.quantity_available) AS total_available,
  mi.tier              AS customer_tier,
  sr.standard_days,
  sr.expedited_days,
  (mi.tier = sr.free_expedited_for_tier) AS free_expedited
FROM live_inventory li
CROSS JOIN membership_info mi
JOIN shipping_rates sr
  ON mi.region = sr.region
GROUP BY li.product_id, mi.tier, sr.standard_days, sr.expedited_days, sr.free_expedited_for_tier;

CREATE INDEX ON personalized_delivery_context (product_id);
COMMENT ON VIEW personalized_delivery_context IS
  'For a given product and customer, combine live inventory, membership tier, and shipping policies to calculate availability, delivery speed, and eligibility for free expedited shipping. Use this tool to generate personalized delivery quotes instantly.';
```

## Example 2:

In finance, small delays or data inconsistency can be costly.
If an AI-based robo-advisor is using stale market data, or doesn’t know about a client’s latest trades or preferences, it may provide recommendations that no longer align with current market conditions or client priorities.
By continuously ingesting price feeds and users market allocations, Materialize ensures the system always sees each client’s current portfolio. When prices change, market conditions shift, or clients objectives evolve, the advisor recalculates portfolio allocations within seconds, so customers can act before opportunities vanish.

> User: Based on my portfolio and today’s market, any trade suggestions?
>
> Assistant: Your portfolio is valued at $50k, with TechCorp up 3% and GreenCo down 6%. Consider trimming GreenCo by 10% and reallocating into TechCorp for momentum, keeping some cash for safety.”

```sql
CREATE VIEW financrag_portfolio_context AS
WITH live_prices AS (
  SELECT symbol, price_usd, as_of
  FROM market_prices
  WHERE as_of >= NOW() - INTERVAL '1 minute'
),
portfolio_stats AS (
  SELECT p.client_id, p.symbol, p.shares,
         lp.price_usd * p.shares AS position_value
  FROM positions p
  JOIN live_prices lp
    ON p.symbol = lp.symbol
),
portfolio_agg AS (
  SELECT
    client_id,
    SUM(position_value) AS total_value,
    JSONB_AGG(
      JSONB_BUILD_OBJECT(
        'symbol', symbol,
        'shares', shares,
        'value', position_value,
        'price', lp.price_usd
      )
    ) AS holdings
  FROM portfolio_stats
  GROUP BY client_id
)
SELECT client_id, total_value, holdings
FROM portfolio_agg;

CREATE INDEX ON financrag_portfolio_context (client_id);
COMMENT ON VIEW financrag_portfolio_context IS
  'Given a client ID, return their live portfolio value, with real-time valuations for each holding based on the latest market prices. Use this tool to generate up-to-date investment summaries or personalized trading recommendations.';

```
