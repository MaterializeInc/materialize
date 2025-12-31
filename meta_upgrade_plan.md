# Meta Upgrade Plan: Improving skill_upgrade.md

# Produced by going into Plan mode with Claude Opus after one round of changes and asking:
# > OK. Plan to do the same again. Think ultrahard. Reference best practices from the web please. Take as long as you need.

This document describes research-backed improvements to `skill_upgrade.md` based on industry best practices for LLM-friendly documentation.

## Research Sources

| Source | Key Finding |
|--------|-------------|
| [AWS RAG Best Practices](https://docs.aws.amazon.com/prescriptive-guidance/latest/writing-best-practices-rag/best-practices.html) | Tables break RAG; use lists instead. Add section summaries. |
| [Bluestream Metadata Strategy](https://bluestream.com/blog/how-to-build-a-metadata-strategy-for-ai-ready-documentation/) | Controlled vocabularies, typed relationships, complexity levels |
| [Nielsen Norman Group](https://www.nngroup.com/articles/progressive-disclosure/) | Max 2 levels of progressive disclosure |
| [Write the Docs](https://www.writethedocs.org/guide/docs-as-code/) | Consistency, style guides, version control |

## Current State Analysis

**Location**: `.claude/skills/materialize-docs/`
**Files**: 389 markdown files across 183 directories
**Front matter**: None on individual files
**Existing index**: SKILL.md (160 lines, serves as entry point)
**Hugo shortcodes**: Present (e.g., `{{< include-md >}}`)

---

## Proposed Changes

### 1. ADD: Step 0 - Hugo Shortcode Resolution

The docs contain unresolved Hugo templates. Add prerequisite step:

```markdown
## Step 0: Resolve Hugo Shortcodes (PREREQUISITE)

| Shortcode | Resolution |
|-----------|------------|
| `{{< include-md file="..." >}}` | Inline the file content |
| `{{< diagram "..." >}}` | Convert to `[See diagram: name]` link |
| `{{< tabs >}}...{{< /tabs >}}` | Flatten to sequential H4 sections |
| `{{< warning >}}` | Convert to `> **Warning:** ...` |
```

### 2. CRITICAL: Replace Tables with Bulleted Lists

**Why**: AWS explicitly states tables break RAG chunking (rows split from headers).

**Transform**:
```markdown
# Before (problematic)
| Field | Type | Description |
|-------|------|-------------|
| id | text | The ID |

# After (RAG-friendly)
### Fields
- **`id`** (`text`): The ID
```

### 3. ADD: Section Summaries

After every H2, add 1-2 sentence summary:

```markdown
## Syntax
This section defines the SQL syntax for CREATE SOURCE.

```sql
CREATE SOURCE ...
```
```

### 4. REVISE: Preamble to 2 Sections Max

**Why**: Nielsen Norman research shows >2 levels hurts UX.

**Current** (4 sections):
- Purpose
- Use this when
- Do not use this when
- Key takeaways

**Revised** (2 sections):
- Purpose (with session starter)
- When to use (merged positive/negative)

Move "Key takeaways" to document end.

### 5. ADD: Session Starters

**Why**: AWS research shows contextual transitions create high semantic matching for RAG.

```markdown
## Purpose
CREATE SOURCE connects Materialize to external data.

If you are looking to ingest data from Kafka, PostgreSQL, or webhooks,
this command is your starting point.
```

### 6. ADD: Document Splitting Guidance

| Size | Action |
|------|--------|
| < 2,000 tokens | Keep as single file |
| 2,000-5,000 | Add section anchors |
| 5,000-10,000 | Consider splitting |
| > 10,000 | **Must split** |

### 7. REVISE: Integrate with SKILL.md

- **DO NOT** create `_INDEX.md` (duplicates SKILL.md)
- **ADD** `_VOCABULARY.yaml` for controlled vocabulary
- **KEEP** `_GLOSSARY.md` and `_MANIFEST.json`

### 8. ADD: Controlled Vocabulary

**Why**: Bluestream research emphasizes controlled vocabularies reduce ambiguity.

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

### 9. ENHANCE: Typed Relationships

**Why**: Flat `see_also` doesn't tell an LLM WHY docs are related.

Replace flat `see_also` with:

```yaml
prerequisites:
  - path: "sql/create-connection.md"
    reason: "Connection must exist first"
related_concepts:
  - path: "concepts/sources.md"
    relationship: "explains-theory"
alternatives:
  - path: "sql/create-source/webhook.md"
    when: "For HTTP push-based ingestion"
```

### 10. ADD: Complexity Level

**Why**: Bluestream research - helps LLMs filter by user expertise.

```yaml
complexity: "<beginner | intermediate | advanced>"
```

---

## Updated Success Criteria

### Automated Checks
- [ ] No files > 10,000 tokens
- [ ] No tables > 5 rows (convert to lists)
- [ ] Every H2 has summary within 3 lines
- [ ] Every howto has session starter
- [ ] `_VOCABULARY.yaml` covers all `_GLOSSARY.md` terms
- [ ] `complexity` field in all front matter
- [ ] No Hugo shortcodes remain

---

## Updated Priority Tiers

| Tier | Steps | Rationale |
|------|-------|-----------|
| **P0 (Critical)** | 0, 1, 2, 4 | Shortcode resolution enables all else; tables→lists is highest RAG impact |
| **P1 (High)** | 3, 5, 6 | Section summaries, session starters, chunking ensure retrievability |
| **P2 (Medium)** | 7, 8 | SKILL.md integration, controlled vocabulary |
| **P3 (Polish)** | 9, 10 | Typed relationships, complexity level |

---

## Justification Comments Template

Use these in skill_upgrade.md to cite sources:

```markdown
<!-- Based on AWS Prescriptive Guidance: Tables break left-to-right reading
     flow that RAG chunking relies on. Replace with bulleted lists.
     https://docs.aws.amazon.com/prescriptive-guidance/latest/writing-best-practices-rag/best-practices.html -->

<!-- Based on Nielsen Norman Group: >2 levels of progressive disclosure
     negatively affects user experience. Limit preamble to 2 sections.
     https://www.nngroup.com/articles/progressive-disclosure/ -->

<!-- Based on Bluestream metadata strategy: Controlled vocabularies reduce
     ambiguity and enable consistent term usage across documents.
     https://bluestream.com/blog/how-to-build-a-metadata-strategy-for-ai-ready-documentation/ -->

<!-- Based on AWS RAG research: Section summaries after headings significantly
     improve semantic search matching and retrieval accuracy.
     https://docs.aws.amazon.com/prescriptive-guidance/latest/writing-best-practices-rag/best-practices.html -->

<!-- Based on LLM chunking research: Documents >10,000 tokens should be split
     to avoid context dilution and improve retrieval precision. -->
```

---

## Implementation Checklist

When editing `skill_upgrade.md`, make these changes:

- [ ] Add Step 0: Hugo Shortcode Resolution before Step 1
- [ ] Update Priority Tiers (P0 now includes Step 0 and table transformation)
- [ ] Enhance Step 1 metadata with `complexity` and typed relationships
- [ ] Revise Step 2 to 2 sections max + add session starter requirement
- [ ] Add Step 2b: Section Summaries requirement
- [ ] **REPLACE** Step 4 entirely with table-to-list transformation
- [ ] Add Step 4b: Document Splitting guidance
- [ ] Revise Step 8 to integrate with SKILL.md (remove `_INDEX.md`, add `_VOCABULARY.yaml`)
- [ ] Update Success Criteria with new automated checks
- [ ] Update Before/After example to show all new patterns
