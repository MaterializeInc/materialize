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
## Priority Tiers

Execute steps in priority order. If constrained, complete higher tiers first.

| Tier | Steps | Rationale |
|------|-------|-----------|
| **P0 (Critical)** | 1, 2, 8 | Metadata and preambles enable routing; index files enable discovery |
| **P1 (High)** | 3, 5, 10 | Structure and validation ensure consistency |
| **P2 (Medium)** | 4, 6, 7 | Polish for edge cases |
| **P3 (Optional)** | 9 | Manifest is useful but not required for basic functionality |

---

## Step 1: Normalize Metadata for Every Page

### Action
Ensure **every Markdown file** begins with a machine-readable metadata block.

Prefer YAML front matter. If unavailable, insert a synthetic `Metadata` section at the very top.

### Required Fields
Add or normalize the following fields:

```yaml
---
title: "<page title>"
description: "<1–2 sentence summary>"
doc_type: "<overview | concept | howto | reference | troubleshooting | tutorial>"
product_area: "<SQL | Sources | Sinks | Security | Deployment | Clusters | Indexes | Views | Connections | ...>"
audience: "<developer | dba | operator | data-engineer>"
status: "<stable | beta | deprecated | experimental>"
keywords:
  - "<primary term>"
  - "<synonym>"
  - "<grep-friendly token>"
see_also:
  - "<relative path to related doc>"
canonical_url: "<original docs.materialize.com URL>"
---
```

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

Immediately after the title, insert a short orienting section.

### Required Sections
Add the following headings (even if brief):

```markdown
## Purpose
<1–2 sentences describing what this page explains>

## Use this when
- <specific situation>

## Do not use this when
- <common misapplication>

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

## Step 4: Normalize Admonitions and Tables

### Admonitions
Convert all Hugo shortcodes or HTML admonitions into plain Markdown:

```markdown
> **Warning:** ...
> **Note:** ...
> **Invariant:** ...
> **Pitfall:** ...
```

### Tables
For every table:
- Add a 1–2 sentence explanation **before** the table explaining what it is for.
- Avoid tables without surrounding prose.

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

Create the following new files at the root of `materialize-docs`:

### `_INDEX.md`
A structured table of contents grouped by product_area and doc_type:
```markdown
# Materialize Documentation Index

## By Product Area
### SQL
- [CREATE SOURCE](sql/create-source.md) - reference
- [SELECT](sql/select.md) - reference
...

## By Task
### Getting Started
- [Quickstart](getting-started/quickstart.md) - tutorial
...
```

### `_GLOSSARY.md`
Definitions of Materialize-specific terms with links to detailed docs:
```markdown
# Glossary

**Arrangement**: An indexed, incrementally maintained collection. See [Arrangements](concepts/arrangements.md).

**Cluster**: A pool of compute resources. See [Clusters](sql/clusters.md).

**Materialized View**: A view whose results are persisted and incrementally updated. See [CREATE MATERIALIZED VIEW](sql/create-materialized-view.md).
```

### `_SQL_COMMANDS.md`
Quick reference of all SQL commands with one-line descriptions:
```markdown
# SQL Command Reference

| Command | Description | Link |
|---------|-------------|------|
| `CREATE SOURCE` | Define an external data source | [docs](sql/create-source.md) |
| `CREATE MATERIALIZED VIEW` | Create an incrementally maintained view | [docs](sql/create-materialized-view.md) |
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
- [ ] Every `.md` file has YAML front matter with all required fields
- [ ] Every `.md` file has a `## Purpose` section within the first 20 lines
- [ ] Every code block specifies a language
- [ ] `_INDEX.md`, `_GLOSSARY.md`, and `_SQL_COMMANDS.md` exist
- [ ] No broken internal links (relative paths resolve to existing files)
- [ ] No Hugo shortcodes remain (e.g., `{{< ... >}}`)

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
keywords:
  - CREATE SOURCE
  - source
  - ingestion
  - Kafka
  - PostgreSQL
  - webhook
see_also:
  - sql/create-connection.md
  - sql/create-materialized-view.md
  - concepts/sources.md
canonical_url: "https://materialize.com/docs/sql/create-source/"
---

# CREATE SOURCE

## Purpose
Defines an external data source that Materialize continuously ingests from. Sources are the primary way to get data into Materialize.

## Use this when
- Ingesting streaming data from Kafka, Redpanda, or other message brokers
- Replicating data from PostgreSQL via logical replication
- Receiving data via webhooks

## Do not use this when
- You want to query data without persisting it (use a direct SELECT instead, if supported)
- You need to transform data before ingestion (create a source first, then a view)

## Key takeaways
- Sources are append-only by default; updates require upsert semantics
- Each source requires a CONNECTION object to be created first
- Sources consume cluster resources continuously, even when not queried

**Search terms:** CREATE SOURCE, source, ingestion, Kafka, PostgreSQL, webhook, mz_sources

## Syntax

` ` `sql
CREATE SOURCE name
FROM KAFKA CONNECTION conn_name (TOPIC 'topic')
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn;
` ` `

## Parameters

The following table describes the parameters for CREATE SOURCE:

| Parameter | Description |
|-----------|-------------|
| `name` | The name of the source to create |
| `conn_name` | A Kafka connection created with CREATE CONNECTION |

## Examples

` ` `sql
CREATE SOURCE my_source
FROM KAFKA CONNECTION kafka_conn (TOPIC 'events')
FORMAT JSON;
` ` `

## See also
- [CREATE CONNECTION](create-connection.md)
- [Sources concept guide](../concepts/sources.md)
```
