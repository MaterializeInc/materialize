{{- /* Home page template - generates SKILL.md for Claude */ -}}
---
name: materialize-docs
description: Materialize documentation for SQL syntax, data ingestion, concepts, and best practices. Use when users ask about Materialize queries, sources, sinks, views, or clusters.
---

# Materialize Documentation

This skill provides comprehensive documentation for Materialize, a streaming database for real-time analytics.

## How to Use This Skill

When a user asks about Materialize:

1. **For SQL syntax/commands**: Read files in the `sql/` directory
2. **For core concepts**: Read files in the `concepts/` directory
3. **For data ingestion**: Read files in the `ingest-data/` directory
4. **For transformations**: Read files in the `transform-data/` directory

## Documentation Sections
{{- range .Site.Sections }}
{{- $sectionName := .Section }}
{{- if not (in (slice "releases" "self-managed" "about" "get-started") $sectionName) }}

### {{ .Title }}
{{- with .Description }}
{{ . }}
{{- end }}
{{ range .Pages | first 10 }}
- **{{ .Title }}**: `{{ strings.TrimPrefix "/" .RelPermalink }}`
{{- end }}
{{- if gt (len .Pages) 10 }}
- _(and {{ sub (len .Pages) 10 }} more files in this section)_
{{- end }}
{{- end }}
{{- end }}

## Quick Reference

### Common SQL Commands

| Command | Description |
|---------|-------------|
| `CREATE SOURCE` | Connect to external data sources (Kafka, PostgreSQL, MySQL) |
| `CREATE MATERIALIZED VIEW` | Create incrementally maintained views |
| `CREATE INDEX` | Create indexes on views for faster queries |
| `CREATE SINK` | Export data to external systems |
| `SELECT` | Query data from sources, views, and tables |

### Key Concepts

- **Sources**: Connections to external data systems that stream data into Materialize
- **Materialized Views**: Views that are incrementally maintained as source data changes
- **Indexes**: Arrangements of data in memory for fast point lookups
- **Clusters**: Isolated compute resources for running dataflows
- **Sinks**: Connections that export data from Materialize to external systems
