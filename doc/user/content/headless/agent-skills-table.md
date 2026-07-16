---
headless: true
---
| Skill | What it provides | When to use |
|-------|------------------|-------------|
| `mcp-developer-analysis` | Exact catalog schemas, diagnostic workflows, remediation runbooks, and guardrails for known pitfalls (cluster-scoped queries, uint8 ID mismatches, etc.). | Operational introspection and troubleshooting via the `materialize-developer` server. Examples: *"why is my materialized view stale?"*, *"what can I optimize to save costs?"*, *"is my source healthy?"* |
| `materialize-docs` | Comprehensive Materialize documentation, including SQL syntax, idiomatic patterns, data ingestion, concepts, and best practices (400+ reference files). | Authoring view definitions, learning concepts, looking up patterns. Useful with either MCP server. Examples: *"show me how to deduplicate a stream"*, *"what's the idiomatic top-K pattern?"*, *"how do I create a Kafka source?"* |
| `materialize-dbt` | dbt-materialize adapter usage: materializations, profile configuration, index creation, blue/green deployments, and testing. | Managing Materialize pipelines with dbt. Examples: *"write a dbt model for a materialized view"*, *"how do I do a blue/green deployment with dbt?"* |
