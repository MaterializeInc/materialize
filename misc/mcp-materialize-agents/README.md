# Materialize MCP Server

**The live data layer for apps and agents** - Create up-to-the-second views into your business, just using SQL.

A Model Context Protocol (MCP) server that exposes Materialize indexed views as discoverable, canonical business objects for AI agents and LLM applications. Transform your live data infrastructure into intelligent, context-aware tools without additional services or caching layers.

## What is MCP?

The [Model Context Protocol](https://modelcontextprotocol.io/) is an open standard that enables seamless integration between LLM applications and data sources. This server implements MCP to expose your Materialize views as typed, discoverable tools that AI agents can query directly.

## Why Materialize + MCP?

Traditional RAG pipelines rely on stale snapshots and complex caching. With Materialize's incremental computation engine, your AI agents get:

- **Live context**: Query continuously updated views without ETL delays
- **Canonical business objects**: Single source of truth for both applications and agents
- **Operational consistency**: Same data products power your entire stack
- **Zero-latency lookups**: Indexed views deliver instant results at any scale
- **Business-aligned tools**: Views encapsulate domain logic, making them perfect AI tools

## Vector Stores + Materialize: Better Together

**Vector stores excel at the "why"** - they provide your AI with policies, documentation, historical patterns, and general knowledge. They answer questions like "What's our return policy?" or "How do we handle shipping delays?"

**Materialize excels at the "what's happening now"** - it provides live operational context about current state. It answers questions like "What's the status of order #12345?" or "How many items are in stock right now?"

Together, they create complete AI agents:
- **Vector store**: Retrieves the rule that orders over $100 get free shipping
- **Materialize**: Provides the current order total, customer tier, and inventory levels
- **AI Agent**: Combines both to make intelligent decisions with current context

This complementary approach ensures your agents have both the knowledge to make good decisions and the live data to make them accurately.

## How It Works

1. **Define indexed views** in Materialize that represent your canonical business objects
2. **Add comments** to make views discoverable by AI agents
3. **Connect your AI** application via MCP to access live data products
4. **Query live data** - agents get fresh, up-to-the-second data with every request

The server automatically:
- Discovers all commented views and indexes as canonical business objects
- Generates typed schemas from your SQL definitions
- Exposes them as MCP tools for AI agents
- Handles query execution with connection pooling

## Benefits of Operational Data Products

* **Stable:** Define canonical business objects once, use everywhere
* **Typed:** Input and output schemas are derived from the index
* **Observable:** Usage is logged per‑tool, making cost and performance explicit
* **Secure:** If you don't create a view/index, it isn't callable

## Installation

```bash
pip install mcp-materialize-agents
```

## Quickstart

Run the server with default settings:

```bash
mcp-materialize-agents
```

Or if installed via uv:

```bash
uv run mcp-materialize-agents
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


## Defining Canonical Business Objects as Tools

Transform your business logic into discoverable, live data tools that AI agents can query:

1. **Write a view** that represents a canonical business object
2. **Create an index** on key columns for instant lookups
3. **Add comments** to make it discoverable and self-documenting

```sql
-- Create a canonical view of order status
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

-- Index for instant lookups
CREATE INDEX ON order_status_summary (order_id);

-- Make it discoverable to AI agents
COMMENT ON VIEW order_status_summary IS 'Look up the status, shipment, and delivery info for a given order.';
COMMENT ON COLUMN order_status_summary.order_id IS 'The unique id for an order';
```

The server automatically exposes this as a typed MCP tool:

Your AI agents can now query this live view to get up-to-the-second order information, while your vector store provides the policies for handling delays, returns, and customer communication.

## Example 1: Customer Support Assistant

Your support agent needs both policy knowledge and current context to help customers effectively.

**Vector Store provides the rules:**
- Return policy: 30 days for electronics, 60 days for furniture
- Shipping policy: Free shipping on orders over $100
- Gold members get free expedited shipping

**Materialize provides what's happening now:**
- Order #12345 was placed 27 days ago (still returnable)
- Current status: Out for delivery
- Customer is a Gold member with 3 items in cart totaling $97

> User: Can I return my laptop if it arrives damaged? Also, how much more do I need to add for free shipping on my current cart?
>
> Assistant: Yes, you can return your laptop within 30 days of purchase - you have 3 days left on order #12345 which is currently out for delivery. For your current cart, you need to add $3 more to qualify for free shipping, or as a Gold member, you already get free expedited shipping regardless of order total!

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
COMMENT ON personalized_delivery_context IS
  'Combine live inventory, membership status, and shipping rules for personalized delivery quotes';
```

## Example 2: Financial Advisory Assistant

Your robo-advisor combines investment strategies with live market conditions.

**Vector Store provides the strategies:**
- Risk profiles and allocation strategies
- Tax-loss harvesting rules
- Rebalancing thresholds and strategies
- Market analysis patterns and indicators

**Materialize provides what's happening now:**
- Current portfolio value: $50,000
- TechCorp position: Up 3% today, 15% of portfolio
- GreenCo position: Down 6% today, 8% of portfolio
- Cash position: $5,000 available
- 2 pending limit orders

> User: Should I rebalance my portfolio given today's market moves?
>
> Assistant: Based on your moderate risk profile, your portfolio is slightly overweight in TechCorp (15% vs 12% target). With TechCorp up 3% today, this is a good opportunity to take profits. I recommend selling $1,500 of TechCorp to bring it back to target allocation. GreenCo is down 6% but still within your 5-10% allocation range at 8%, so no action needed there unless you want to tax-loss harvest.

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
COMMENT ON financrag_portfolio_context IS
  'Provide live portfolio valuation and per‑position details for RAG';
```

## Getting Started

1. **Install the server**: `pip install mcp-materialize-agents`
2. **Configure your connection**: Set `MZ_DSN` environment variable or use `--mz-dsn` flag
3. **Create indexed views**: Define your canonical business objects in Materialize
4. **Add comments**: Make views discoverable with descriptive comments
5. **Connect your AI application**: Use any MCP-compatible client

## Learn More

- [Materialize Documentation](https://materialize.com/docs)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [MCP Integration Guide](https://modelcontextprotocol.io/integrations)
```
