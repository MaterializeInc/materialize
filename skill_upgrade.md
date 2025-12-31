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
doc_type: "<overview | concept | howto | reference | troubleshooting>"
product_area: "<SQL | Sources | Sinks | Security | Deployment | ...>"
audience: "<dev | ops | user>"
status: "<stable | beta | deprecated>"
keywords:
  - "<primary term>"
  - "<synonym>"
  - "<grep-friendly token>"
repo_paths:
  - "<best-guess code path or glob>"
see_also:
  - "<relative path to related doc>"
canonical_url: "<original docs.materialize.com URL>"
---
```

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

## Related code
- <path or glob>
```

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

- `_INDEX.md`
- `_GLOSSARY.md`
- `_SQL_COMMANDS.md`
- `_CODE_MAP.md`

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
An LLM agent can:
- Identify doc intent within 5–10 lines
- Route from code to docs deterministically
- Avoid semantic confusion
- Apply docs without reading everything

---

## Instruction to the Executing LLM
Execute deterministically. Prefer explicitness over elegance.
