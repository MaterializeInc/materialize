# Skill Upgrade: Make Materialize User Docs Maximally Accessible to LLM Agents

## Objective
Upgrade the generated Markdown documentation in `.claude/skills/materialize-docs` (sourced from `doc/user/content` in the Materialize repository) to be maximally accessible, searchable, and semantically clear for LLM-based coding agents (e.g., Claude Code).

The goal is to:
- Improve retrieval accuracy
- Improve understanding when files are read in isolation
- Reduce hallucination by making intent, scope, and invariants explicit
- Enable fast routing between docs and code

You are authorized to:
- Modify generated Markdown files
- Add new Markdown files
- Add generated metadata
- Add machine-readable artifacts (e.g., JSON manifests)

You are **not** asked to change application code.

---

## Scope
Input:
- Markdown files generated from `doc/user/content`
- Repository context available under `materialize/`

Output:
- Enhanced Markdown docs
- New index / map files
- Optional manifest file

<!-- EDIT: Added priority tiers. Without prioritization, an LLM or human
     might treat all steps equally when some deliver far more value.
     This enables incremental improvement rather than all-or-nothing. -->

<!-- Based on AWS Prescriptive Guidance: Tables break left-to-right reading
     flow that RAG chunking relies on. Step 4's table-to-list transformation
     is critical for retrieval accuracy.
     https://docs.aws.amazon.com/prescriptive-guidance/latest/writing-best-practices-rag/best-practices.html -->

## Priority Tiers

Execute steps in priority order. If constrained, complete higher tiers first.

- **P0 (Critical)**: Steps 0, 1, 2, 4 — Shortcode resolution enables all else; metadata and preambles enable routing; table-to-list transformation is highest RAG impact
- **P1 (High)**: Steps 2b, 3, 4b, 10 — Section summaries, structure enforcement, document splitting, and validation ensure retrievability and consistency
- **P2 (Medium)**: Steps 5, 6, 7, 8 — Code blocks, links, search hints, and routing artifacts (SKILL.md integration, vocabulary)
- **P3 (Polish)**: Step 9 — Manifest is useful but not required for basic functionality

---

## Step 0: Resolve Hugo Shortcodes (PREREQUISITE)

<!-- Based on analysis of materialize-docs: Hugo templates in the source docs
     don't render in plain Markdown. These must be resolved before any other
     processing can begin. -->

### Action
Before processing any documentation file, resolve all Hugo shortcodes to plain Markdown equivalents.

### Resolution Rules

- **`{{< include-md file="..." >}}`**: Inline the referenced file content directly
- **`{{< diagram "..." >}}`**: Convert to `[See diagram: name]` with link to diagram file
- **`{{< tabs >}}...{{< /tabs >}}`**: Flatten to sequential H4 sections (one per tab)
- **`{{< warning >}}`**: Convert to `> **Warning:** ...`
- **`{{< note >}}`**: Convert to `> **Note:** ...`
- **`{{< tip >}}`**: Convert to `> **Tip:** ...`
- **`{{< private-preview >}}`**: Convert to `> **Private Preview:** This feature is in private preview.`
- **`{{< public-preview >}}`**: Convert to `> **Public Preview:** This feature is in public preview.`

### Validation
After resolution, grep for `{{<` and `{{%` patterns. No Hugo shortcodes should remain.

---

## Step 1: Normalize Metadata for Every Page

### Action
Ensure **every Markdown file** begins with a machine-readable metadata block.

Prefer YAML front matter. If unavailable, insert a synthetic `Metadata` section at the very top.

### Required Fields
Add or normalize the following fields:

<!-- Based on Bluestream metadata strategy: Complexity levels help LLMs filter
     by user expertise. Typed relationships explain WHY docs are related.
     https://bluestream.com/blog/how-to-build-a-metadata-strategy-for-ai-ready-documentation/ -->

```yaml
---
title: "<page title>"
description: "<1–2 sentence summary>"
doc_type: "<overview | concept | howto | reference | troubleshooting | tutorial>"
product_area: "<SQL | Sources | Sinks | Security | Deployment | Clusters | Indexes | Views | Connections | ...>"
audience: "<developer | dba | operator | data-engineer>"
status: "<stable | beta | deprecated | experimental>"
complexity: "<beginner | intermediate | advanced>"
keywords:
  - "<primary term>"
  - "<synonym>"
  - "<grep-friendly token>"
prerequisites:
  - path: "<relative path>"
    reason: "<why this must be read/done first>"
related_concepts:
  - path: "<relative path>"
    relationship: "<explains-theory | provides-context | deep-dive>"
alternatives:
  - path: "<relative path>"
    when: "<condition when alternative is preferred>"
canonical_url: "<original docs.materialize.com URL>"
---
```

### Complexity Guidelines
- **beginner**: No prior Materialize knowledge required; basic SQL familiarity
- **intermediate**: Assumes understanding of sources, views, and clusters
- **advanced**: Requires knowledge of dataflows, arrangements, or operational concerns

<!-- EDIT: Removed `repo_paths` field. Asking an LLM to "best-guess" code paths
     invites hallucination—the very problem we're trying to prevent. If code
     mapping is needed, it should be done separately by grepping the actual
     codebase, not guessing. -->

<!-- EDIT: Expanded audience and doc_type enums. The original "dev | ops | user"
     conflates roles. "developer" and "data-engineer" are distinct audiences
     with different needs. Added "tutorial" as a doc_type since it differs
     from "howto" (tutorials are learning-focused, howtos are task-focused). -->

<!-- EDIT: Added "experimental" status. Materialize has experimental features
     that aren't just beta—they may change or disappear entirely. -->

---

## Step 2: Add a Progressive-Disclosure Preamble

<!-- Based on Nielsen Norman Group: >2 levels of progressive disclosure
     negatively affects user experience. Limit preamble to 2 sections.
     https://www.nngroup.com/articles/progressive-disclosure/ -->

<!-- Based on AWS RAG research: Session starters (contextual transitions)
     create high semantic matching for RAG retrieval.
     https://docs.aws.amazon.com/prescriptive-guidance/latest/writing-best-practices-rag/best-practices.html -->

Immediately after the title, insert a short orienting section with **exactly 2 sections** plus a session starter.

### Required Sections (2 max)

```markdown
## Purpose
<1–2 sentences describing what this page explains>

<Session starter: A contextual sentence that helps users confirm they're in the right place>
Example: "If you are looking to ingest data from Kafka, PostgreSQL, or webhooks, this command is your starting point."

## When to use
- <specific situation where this applies>
- <another situation>
- **Not for**: <common misapplication — merged from "Do not use this when">
```

### Key Takeaways (Move to End)
Move "Key takeaways" to the **end** of the document, after all technical content. This prevents front-loading cognitive load while preserving the summary for quick reference.

```markdown
## Key takeaways
- <invariant or rule>
- <important semantic guarantee>
- <important limitation>
```

<!-- EDIT: Removed "Related code" section from preamble. Same rationale as
     removing repo_paths: guessing code paths invites hallucination. If an
     agent needs to find related code, it should grep for SQL keywords or
     function names mentioned in the doc, not rely on pre-guessed paths. -->

---

## Step 2b: Add Section Summaries

<!-- Based on AWS RAG research: Section summaries after headings significantly
     improve semantic search matching and retrieval accuracy.
     https://docs.aws.amazon.com/prescriptive-guidance/latest/writing-best-practices-rag/best-practices.html -->

After every H2 heading, add a 1-2 sentence summary explaining what the section covers. This improves RAG chunking by ensuring each chunk has self-contained context.

### Example

```markdown
## Syntax
This section defines the SQL syntax for CREATE SOURCE, including all required and optional clauses.

` ` `sql
CREATE SOURCE name
FROM KAFKA CONNECTION conn_name (TOPIC 'topic')
...
` ` `
```

### Validation
Every H2 section should have descriptive text within the first 3 lines after the heading (before any code blocks, tables, or sub-headings).

---

## Step 3: Enforce Structure by Document Type

Reorder or lightly refactor content so it follows a consistent structure.

### Reference Docs
1. Purpose
2. Syntax / Definition
3. Parameters / Options
4. Semantics & edge cases
5. Examples
6. See also

### How-To Docs
1. Goal
2. Preconditions
3. Steps (numbered)
4. Verification
5. Rollback / Cleanup
6. Troubleshooting

### Concept / Overview Docs
1. Purpose
2. Mental model (textual description)
3. Key terms
4. System interactions
5. Pitfalls
6. See also

---

## Step 4: Convert Tables to Bulleted Lists

<!-- Based on AWS Prescriptive Guidance: Tables break RAG chunking because
     rows get separated from headers during retrieval. Bulleted lists maintain
     context within each item.
     https://docs.aws.amazon.com/prescriptive-guidance/latest/writing-best-practices-rag/best-practices.html -->

### Why This Matters
Tables break left-to-right reading flow that RAG chunking relies on. When a table is chunked, rows often get separated from headers, making them incomprehensible.

### Transformation Rules

**Before (problematic):**
```markdown
| Field | Type | Description |
|-------|------|-------------|
| id | text | The unique identifier |
| name | text | The display name |
```

**After (RAG-friendly):**
```markdown
### Fields
- **`id`** (`text`): The unique identifier
- **`name`** (`text`): The display name
```

### Exceptions
Tables with ≤5 rows may be kept if they serve a clear comparative purpose (e.g., feature comparison matrix). In this case:
- Add a 1-2 sentence explanation **before** the table
- Ensure headers are descriptive

### Admonitions
Convert all Hugo shortcodes or HTML admonitions into plain Markdown blockquotes:

```markdown
> **Warning:** ...
> **Note:** ...
> **Invariant:** ...
> **Pitfall:** ...
```

---

## Step 4b: Document Splitting by Size

<!-- Based on LLM chunking research: Documents >10,000 tokens should be split
     to avoid context dilution and improve retrieval precision. -->

Large documents dilute retrieval precision. Use these guidelines:

### Size Thresholds

- **< 2,000 tokens**: Keep as single file (no action needed)
- **2,000-5,000 tokens**: Add section anchors for deep linking
- **5,000-10,000 tokens**: Consider splitting into logical sub-documents
- **> 10,000 tokens**: **Must split** into multiple files

### How to Split

1. Identify natural boundaries (major H2 sections)
2. Create a parent index file with links to sub-documents
3. Each sub-document should be self-contained with its own metadata
4. Update cross-references in related documents

### Token Estimation
Rough guide: ~4 characters per token in English technical text. A 10,000-token document is approximately 40,000 characters or ~250 lines of dense content.

---

## Step 5: Normalize Code Blocks

### Rules
- Every code block MUST specify a language.
- Do not mix multiple files in one block without labeling.

---

## Step 6: Rewrite Links for Local Use

For every internal link:
- Convert to a relative path within `materialize-docs`
- Preserve canonical URL in parentheses if useful

---

## Step 7: Add Explicit Search Hints for Ambiguous Terms

When a page discusses overloaded or ambiguous terms, add:

```markdown
**Search terms:** source, mz_source, ingestion, CREATE SOURCE, rehydration
```

---

## Step 8: Generate Routing Artifacts

Create the following files at the root of `materialize-docs`:

<!-- Note: SKILL.md already serves as the primary index/entry point.
     Do NOT create _INDEX.md as it would duplicate SKILL.md functionality. -->

### Integrate with SKILL.md
The existing `SKILL.md` file serves as the primary entry point and index. Ensure it:
- Contains accurate links to all major documentation sections
- Groups content by product_area and task type
- Stays in sync with new/removed documents

### `_GLOSSARY.md`
Definitions of Materialize-specific terms with links to detailed docs:
```markdown
# Glossary

**Arrangement**: An indexed, incrementally maintained collection. See [Arrangements](concepts/arrangements.md).

**Cluster**: A pool of compute resources. See [Clusters](sql/clusters.md).

**Materialized View**: A view whose results are persisted and incrementally updated. See [CREATE MATERIALIZED VIEW](sql/create-materialized-view.md).
```

### `_VOCABULARY.yaml`

<!-- Based on Bluestream research: Controlled vocabularies reduce ambiguity
     and enable consistent term usage across documents.
     https://bluestream.com/blog/how-to-build-a-metadata-strategy-for-ai-ready-documentation/ -->

Machine-readable controlled vocabulary for Materialize-specific terms:
```yaml
# _VOCABULARY.yaml
terms:
  source:
    canonical: "source"
    aliases: ["data source", "ingestion source"]
    definition: "Connection to external system streaming data into Materialize"
    not_to_be_confused_with: ["table", "view"]

  materialized_view:
    canonical: "materialized view"
    aliases: ["matview", "MV"]
    definition: "A view whose results are persisted and incrementally updated"
    not_to_be_confused_with: ["view", "index"]

  cluster:
    canonical: "cluster"
    aliases: []
    definition: "A pool of compute resources that runs dataflows"
    not_to_be_confused_with: ["replica", "database cluster"]

  arrangement:
    canonical: "arrangement"
    aliases: []
    definition: "An indexed, incrementally maintained collection in memory"

  dataflow:
    canonical: "dataflow"
    aliases: ["data flow"]
    definition: "A computation graph that processes streaming data"
```

### `_SQL_COMMANDS.md`
Quick reference of all SQL commands with one-line descriptions (use bulleted list format):
```markdown
# SQL Command Reference

- **`CREATE SOURCE`**: Define an external data source — [docs](sql/create-source.md)
- **`CREATE MATERIALIZED VIEW`**: Create an incrementally maintained view — [docs](sql/create-materialized-view.md)
- **`CREATE INDEX`**: Create an index on a view for faster queries — [docs](sql/create-index.md)
```

<!-- EDIT: Removed _CODE_MAP.md. Mapping docs to code requires actually
     analyzing the codebase, not guessing. This is better done as a separate
     task that greps for relevant identifiers. Keeping it here invites
     fabrication. -->

<!-- EDIT: Added concrete examples for each routing artifact. The original
     spec just listed filenames without explaining content or format.
     An LLM executing this would have to guess the structure. -->

---

## Step 9: (Optional) Emit a Manifest

Generate `manifest.json` with per-file metadata for routing and filtering.

---

## Step 10: Validate LLM-Readiness

Ensure:
- No Markdown file lacks metadata
- No code block lacks a language
- No page starts without a Purpose section
- Index / glossary files exist

---

## Success Criteria

<!-- EDIT: Made success criteria testable. The original criteria were
     subjective ("avoid semantic confusion") or unmeasurable ("within 5-10
     lines"). These concrete checks can be validated programmatically or
     by inspection. -->

### Structural Requirements (Automated Checks)
- [ ] No Hugo shortcodes remain (grep for `{{<` and `{{%` returns empty)
- [ ] Every `.md` file has YAML front matter with all required fields including `complexity`
- [ ] Every `.md` file has a `## Purpose` section within the first 20 lines
- [ ] Every H2 section has summary text within 3 lines (before code/tables/sub-headings)
- [ ] Every howto document has a session starter in the Purpose section
- [ ] No files exceed 10,000 tokens (~40,000 characters)
- [ ] No tables with > 5 rows (convert to bulleted lists)
- [ ] Every code block specifies a language
- [ ] `_GLOSSARY.md`, `_VOCABULARY.yaml`, and `_SQL_COMMANDS.md` exist
- [ ] `_VOCABULARY.yaml` covers all terms defined in `_GLOSSARY.md`
- [ ] SKILL.md is in sync with actual documentation structure
- [ ] No broken internal links (relative paths resolve to existing files)

### Semantic Requirements (Manual Spot-Check)
An LLM agent reading a random doc should be able to:
- Determine from metadata alone whether the doc is relevant to a query
- Understand the doc's purpose without reading past the preamble
- Find related docs via `see_also` links
- Locate the doc via `_INDEX.md` or keyword search

---

## Instruction to the Executing LLM

Execute deterministically. Prefer explicitness over elegance.

<!-- EDIT: Added concrete guidance for the executing LLM. The original
     instruction was too terse to be actionable. -->

### Execution Guidelines

1. **Process incrementally**: Transform one file at a time. Verify each file compiles (valid Markdown) before moving on.

2. **Preserve original content**: Add metadata and structure around existing content. Do not rephrase technical content unless it's ambiguous or incorrect.

3. **When uncertain about metadata values**:
   - `doc_type`: Default to "reference" if unclear
   - `product_area`: Use the parent directory name as a hint
   - `status`: Default to "stable" unless the doc mentions beta/experimental
   - `keywords`: Extract from the title, headings, and first paragraph

4. **Handle edge cases**:
   - Empty files: Skip with a warning
   - Non-documentation Markdown (e.g., changelogs): Skip
   - Files already with front matter: Merge, don't duplicate

5. **Report progress**: After processing, output a summary:
   - Files processed
   - Files skipped (with reasons)
   - Validation failures

---

<!-- EDIT: Added a concrete before/after example. Specifications without
     examples are ambiguous. This shows exactly what transformation is
     expected, reducing interpretation variance between different LLMs
     or execution runs. -->

## Appendix: Before/After Example

### Before (typical generated doc)
```markdown
# CREATE SOURCE

Creates a new source in Materialize.

## Syntax

CREATE SOURCE name
FROM KAFKA CONNECTION conn_name (TOPIC 'topic')
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn;

## Parameters

| Parameter | Description |
|-----------|-------------|
| name | The name of the source |
| conn_name | A Kafka connection |

## Examples

CREATE SOURCE my_source
FROM KAFKA CONNECTION kafka_conn (TOPIC 'events')
FORMAT JSON;
```

### After (LLM-optimized)
```markdown
---
title: "CREATE SOURCE"
description: "Creates a source that ingests data from an external system into Materialize."
doc_type: reference
product_area: Sources
audience: developer
status: stable
complexity: beginner
keywords:
  - CREATE SOURCE
  - source
  - ingestion
  - Kafka
  - PostgreSQL
  - webhook
prerequisites:
  - path: "sql/create-connection.md"
    reason: "A CONNECTION must exist before creating a source"
related_concepts:
  - path: "concepts/sources.md"
    relationship: "explains-theory"
alternatives:
  - path: "sql/create-source/webhook.md"
    when: "For HTTP push-based ingestion instead of pull-based"
canonical_url: "https://materialize.com/docs/sql/create-source/"
---

# CREATE SOURCE

## Purpose
Defines an external data source that Materialize continuously ingests from. Sources are the primary way to get data into Materialize.

If you are looking to ingest data from Kafka, PostgreSQL, MySQL, or webhooks, this command is your starting point.

## When to use
- Ingesting streaming data from Kafka, Redpanda, or other message brokers
- Replicating data from PostgreSQL or MySQL via CDC
- Receiving data via webhooks
- **Not for**: Querying data without persistence (use SELECT on existing objects) or transforming before ingestion (create source first, then a view)

**Search terms:** CREATE SOURCE, source, ingestion, Kafka, PostgreSQL, webhook, mz_sources

## Syntax
This section defines the SQL syntax for CREATE SOURCE, including connection and format options.

` ` `sql
CREATE SOURCE name
FROM KAFKA CONNECTION conn_name (TOPIC 'topic')
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn;
` ` `

## Parameters
The following parameters configure the CREATE SOURCE command.

- **`name`** (identifier): The name of the source to create
- **`conn_name`** (identifier): A Kafka connection created with CREATE CONNECTION
- **`TOPIC`** (string): The Kafka topic to consume from
- **`FORMAT`** (clause): Data format specification (AVRO, JSON, PROTOBUF, etc.)

## Examples
Common patterns for creating sources from different external systems.

` ` `sql
-- Basic Kafka source with JSON format
CREATE SOURCE my_source
FROM KAFKA CONNECTION kafka_conn (TOPIC 'events')
FORMAT JSON;
` ` `

## Key takeaways
- Sources are append-only by default; updates require upsert semantics
- Each source requires a CONNECTION object to be created first
- Sources consume cluster resources continuously, even when not queried
```
