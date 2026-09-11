// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * In-console AI agent (the `\ask` shell command).
 *
 * The browser orchestrates a tool-use loop: it asks the model (via the
 * dev-server `/api/ask` middleware, which injects the server-side Anthropic
 * key), and when the model calls `query_system_catalog` we run that query
 * ourselves through mcpClient — as the logged-in user, so RBAC is preserved.
 * Only the LLM completion goes through the proxy; catalog reads never leave
 * the user's authenticated session.
 *
 * The model is told to query the ontology (mz_internal.mz_ontology_*) FIRST so
 * it discovers correct table/column names instead of hallucinating them.
 */

import { mcpQuerySystemCatalog } from "~/api/materialize/mcpClient";

const MODEL = "claude-opus-5";
const MAX_TOOL_ROUNDS = 8;

const SYSTEM_PROMPT = `You are an operations assistant embedded in the Materialize console. You answer plain-English questions about the user's live Materialize environment by querying the system catalog.

You have one tool: query_system_catalog(sql_query) — runs a READ-ONLY query (SELECT/SHOW/EXPLAIN only) against Materialize's mz_* system catalog and returns JSON array-of-arrays rows.

Rules:
1. DISCOVER THE SCHEMA FIRST. Before querying data, query the ontology to find the correct tables, columns, and join paths — never guess mz_* names:
   - mz_internal.mz_ontology_entity_types(name, relation): entities and their backing mz_* table.
   - mz_internal.mz_ontology_link_types(name, source_entity, target_entity): typed relationships between entities.
   - mz_internal.mz_ontology_properties: column names/types per entity.
2. For freshness/"why is X behind": object -> measures_global_lag_of / measures_materialization_lag (mz_internal.mz_materialization_lag has slowest_local_input / slowest_global_input columns that name the bottleneck input) -> hydration_of (mz_internal.mz_hydration_statuses) -> for sources, status_of_source (mz_internal.mz_source_statuses) + source statistics.
3. Catalog gotchas: mz_source_statuses/mz_sink_statuses use last_status_change_at (not updated_at); join mz_cluster_replica_utilization via mz_cluster_replicas + mz_clusters to get names; do NOT query mz_introspection.mz_dataflow_arrangement_sizes.
4. Read-only only. Never attempt INSERT/UPDATE/DELETE/CREATE/ALTER/DROP.
5. Keep querying until you can answer, then STOP and reply with: a one-paragraph root-cause diagnosis naming the specific object, and (if relevant) a suggested SQL fix. Be terse, no preamble.`;

const TOOLS = [
  {
    name: "query_system_catalog",
    description:
      "Run a read-only SQL query (SELECT/SHOW/EXPLAIN only) against Materialize's mz_* system catalog and ontology. Returns JSON array-of-arrays rows.",
    input_schema: {
      type: "object",
      properties: {
        sql_query: {
          type: "string",
          description:
            "PostgreSQL-compatible read-only SQL referencing mz_* / pg_catalog / information_schema tables",
        },
      },
      required: ["sql_query"],
    },
  },
];

interface ToolUseBlock {
  type: "tool_use";
  id: string;
  name: string;
  input: { sql_query?: string };
}
interface TextBlock {
  type: "text";
  text: string;
}
type ContentBlock = ToolUseBlock | TextBlock | { type: string };

interface MessageResponse {
  stop_reason?: string;
  content?: ContentBlock[];
  error?: { message?: string };
}

interface AnthropicMessage {
  role: "user" | "assistant";
  content: string | ContentBlock[];
}

/** A single query the agent ran while investigating. */
export interface AgentStep {
  sql: string;
  error?: string;
}

export interface AgentResult {
  answer: string;
  steps: AgentStep[];
}

const isToolUse = (b: ContentBlock): b is ToolUseBlock => b.type === "tool_use";
const isText = (b: ContentBlock): b is TextBlock => b.type === "text";

/** One round-trip to the model through the dev-server proxy. */
async function callModel(
  messages: AnthropicMessage[],
): Promise<MessageResponse> {
  const response = await fetch("/api/ask", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: 16000,
      system: SYSTEM_PROMPT,
      tools: TOOLS,
      messages,
    }),
  });
  const raw = await response.text();
  if (!response.ok) {
    let message = raw || `HTTP ${response.status}`;
    try {
      const parsed = JSON.parse(raw);
      message = parsed.error?.message ?? parsed.error ?? message;
    } catch {
      // not JSON
    }
    throw new Error(message);
  }
  return JSON.parse(raw) as MessageResponse;
}

/**
 * Run the agent loop for a question: let the model query the catalog/ontology
 * via the tool until it produces a final answer. Returns the answer plus the
 * queries it ran (so the shell can show the investigation).
 */
export async function runConsoleAgent(question: string): Promise<AgentResult> {
  const messages: AnthropicMessage[] = [{ role: "user", content: question }];
  const steps: AgentStep[] = [];

  for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
    const response = await callModel(messages);
    const blocks = response.content ?? [];
    messages.push({ role: "assistant", content: blocks });

    const toolUses = blocks.filter(isToolUse);
    if (response.stop_reason !== "tool_use" || toolUses.length === 0) {
      const answer = blocks
        .filter(isText)
        .map((b) => b.text)
        .join("\n")
        .trim();
      return { answer: answer || "(no answer)", steps };
    }

    const toolResults = await Promise.all(
      toolUses.map(async (toolUse) => {
        const sql = toolUse.input.sql_query ?? "";
        try {
          const result = await mcpQuerySystemCatalog(sql);
          steps.push({ sql });
          return {
            type: "tool_result" as const,
            tool_use_id: toolUse.id,
            content: result,
          };
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          steps.push({ sql, error: message });
          return {
            type: "tool_result" as const,
            tool_use_id: toolUse.id,
            content: `Error: ${message}`,
            is_error: true,
          };
        }
      }),
    );
    messages.push({ role: "user", content: toolResults });
  }

  return {
    answer: "(stopped: reached the maximum number of investigation steps)",
    steps,
  };
}
