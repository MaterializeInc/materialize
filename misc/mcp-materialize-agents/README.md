# Materialize MCP Server

**The live data layer for apps and agents**

Agents succeed when they can act in a loop with confidence.
They perceive the world, think about what to do, act to change it, and then observe the results. The loop repeats until the goal is reached.

Large language models are improving quickly at the thinking step. The real challenge is perception. Agents need a live, trustworthy picture of the world to base their decisions on. That picture must always be correct and always reflect the current state.

The Materialize MCP Server provides that picture. It exposes your canonical business objects as **live, database consistent data products**.

Consider a customer support agent that needs to understand a customer's complete context:
- Current subscription status and tier
- Recent support interactions and satisfaction scores
- Active orders and shipment tracking
- Account health indicators and churn risk

With Materialize, this complex business logic is precomputed and instantly available. Agents can query with SQL, observe the current state, and see the effects of their actions reflected immediately. Every observation reflects the same point in time, so every action can be taken with confidence.

---

## Why Materialize with MCP?

The [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) defines how agents connect to external tools and data. The Materialize MCP Server implements it for your operational data, giving agents:

* **Live context**: Always see what is happening now, not what happened minutes or hours ago.
* **Database consistency**: All data products reflect the same moment in time, so cross-system joins always line up.
* **Fast reactions**: Observe the effects of actions in under a second.
* **Simple SQL**: Define digital twins of your business objects with views and indexes.
* **Operational safety**: Explore without placing load on production databases.

The result is a shorter path from perception to confident action.

---

## Knowledge plus Live Context

Vector stores and Materialize play different roles that complement each other.

* **Vector stores** give agents the rules and history of the world: policies, documentation, patterns, and prior examples.
* **Materialize** gives agents the current state of the world: inventory, customer status, orders, and shipments.

Together, they give agents both the knowledge and the live context required to act intelligently.

---

## How It Works

1. **Define views** that represent your canonical business objects
2. **Create indexes** so results are always precomputed and instantly accessible
3. **Add comments** for discovery and semantic meaning
4. **Run the MCP server** to expose these live data products to your agents
5. **Query with SQL** from any MCP compatible framework such as LangChain or Strands

Materialize incrementally maintains these views. Instead of recalculating them from scratch, it updates them as source data changes. Queries return in milliseconds even for complex joins, aggregations, and recursive logic.

### Example: Customer 360 View

Here's how you'd create a comprehensive customer view that combines data from multiple systems:

```sql
CREATE VIEW customer_360 AS
SELECT
    c.customer_id,
    c.email,
    c.signup_date,
    -- Subscription and revenue metrics
    s.plan_tier,
    s.monthly_revenue,
    s.renewal_date,
    CASE
        WHEN s.renewal_date < NOW() + INTERVAL '30 days' THEN 'upcoming'
        ELSE 'active'
    END as renewal_status,
    -- Support experience
    COALESCE(sup.recent_tickets, 0) as recent_tickets_30d,
    COALESCE(sup.avg_satisfaction, 0) as avg_satisfaction_score,
    CASE
        WHEN sup.recent_tickets > 5 THEN 'high'
        WHEN sup.recent_tickets > 2 THEN 'medium'
        ELSE 'low'
    END as support_intensity,
    -- Order activity
    COALESCE(o.active_orders, 0) as active_orders,
    COALESCE(o.lifetime_value, 0) as lifetime_value,
    o.last_order_date,
    -- Churn risk calculation
    CASE
        WHEN s.renewal_date < NOW() + INTERVAL '30 days'
            AND sup.avg_satisfaction < 3 THEN 'high'
        WHEN sup.recent_tickets > 5
            AND sup.avg_satisfaction < 4 THEN 'medium'
        WHEN o.last_order_date < NOW() - INTERVAL '90 days' THEN 'medium'
        ELSE 'low'
    END as churn_risk
FROM customers c
LEFT JOIN subscriptions s ON c.customer_id = s.customer_id
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*) as recent_tickets,
        AVG(satisfaction_score) as avg_satisfaction
    FROM support_tickets
    WHERE created_at > MZ_NOW() - INTERVAL '30 days'
    GROUP BY customer_id
) sup ON c.customer_id = sup.customer_id
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(CASE WHEN status IN ('pending', 'processing') THEN 1 END) as active_orders,
        SUM(total_amount) as lifetime_value,
        MAX(order_date) as last_order_date
    FROM orders
    GROUP BY customer_id
) o ON c.customer_id = o.customer_id;

-- Index for instant lookups by customer
CREATE INDEX ON customer_360 (customer_id);

-- Documentation for agent discovery
COMMENT ON VIEW customer_360 IS 'Complete customer context including subscription, support, orders, and churn risk';
COMMENT ON COLUMN customer_360.churn_risk IS 'Risk level: high (immediate attention), medium (monitor), low (healthy)';
```

---

## Real-World Example: Customer Retention Agent

A customer retention agent monitors and acts on churn signals:

* **Perceive**: Query `customer_360` to identify high-risk customers with upcoming renewals
* **Think**: Apply retention strategies from vector store (discount policies, win-back campaigns)
* **Act**: Create personalized retention offer and update customer record
* **Observe**: Customer response reflected instantly in satisfaction scores and order activity
* **Repeat**: Adjust strategy based on real-time customer behavior

Every decision is based on live data that reflects the customer's current state across all systems—support, billing, orders—at the same moment in time.

---

## Installation

```bash
pip install mcp-materialize-agents
```

Run with defaults:

```bash
mcp-materialize-agents
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv run mcp-materialize-agents
```

---

## Configuration

| Argument          | Env Var             | Default                                               | Description                          |
| ----------------- | ------------------- | ----------------------------------------------------- | ------------------------------------ |
| `--mz-dsn`        | `MZ_DSN`            | `postgresql://materialize@localhost:6875/materialize` | Materialize DSN                      |
| `--transport`     | `MCP_TRANSPORT`     | `stdio`                                               | Communication transport (stdio, sse) |
| `--host`          | `MCP_HOST`          | `0.0.0.0`                                             | Server host                          |
| `--port`          | `MCP_PORT`          | `3001`                                                | Server port                          |
| `--pool-min-size` | `MCP_POOL_MIN_SIZE` | `1`                                                   | Minimum connection pool size         |
| `--pool-max-size` | `MCP_POOL_MAX_SIZE` | `10`                                                  | Maximum connection pool size         |
| `--log-level`     | `MCP_LOG_LEVEL`     | `INFO`                                                | Logging level                        |

---

## Using the Customer 360 View

Once defined, agents can leverage this canonical business object:

```sql
-- Find customers needing immediate attention
SELECT customer_id, email, churn_risk, renewal_date
FROM customer_360
WHERE churn_risk = 'high'
  AND renewal_date < NOW() + INTERVAL '7 days';

-- Understand a specific customer's complete context
SELECT * FROM customer_360 WHERE customer_id = 12345;

-- Monitor support experience trends
SELECT support_intensity, COUNT(*), AVG(lifetime_value)
FROM customer_360
GROUP BY support_intensity;
```

Queries return instantly, always reflect the current state, and maintain consistency across all the underlying data sources. Your agent never sees stale data or inconsistent states between systems.

---

## Learn More

* [Materialize Documentation](https://materialize.com/docs)
* [Model Context Protocol](https://modelcontextprotocol.io/)
* [MCP Integration Guide](https://modelcontextprotocol.io/integrations)
