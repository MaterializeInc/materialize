# Repository Guidelines

## Project Structure

This directory is the root of Materialize's Hugo user documentation site.

- `content/` contains user-facing Markdown pages, organized by product area.
- `data/` contains YAML and JSON inputs used to populate reference pages and
  examples.
- `layouts/` contains Hugo templates and shortcodes; `assets/` contains SCSS
  and JavaScript sources.
- `archetypes/` provides metadata templates, `static/` holds copied assets,
  and `sql-grammar/` contains BNF files and generated railroad diagrams.
- `resources/` and `public/` are generated build output. Avoid hand-editing
  them.

## Build and Development Commands

Run commands from `doc/user` unless noted otherwise:

- `hugo server -D` starts a local preview at `http://localhost:1313`.
- `hugo server --disableFastRender --ignoreCache` starts a cache-busting
  preview when incremental rendering is misleading.
- `../../ci/test/lint-docs.sh` builds the site, runs `htmltest`, and checks
  documentation catalogs. Run it from the repository root.
- `../../bin/format-docs` trims trailing whitespace and ensures Markdown files
  end with a newline.
- `sql-grammar/generate.sh` regenerates railroad diagrams after BNF changes.

## Writing and Naming Conventions

Use clear Markdown headings, short paragraphs, fenced code blocks with an
appropriate language (`sql` for SQL and `nofmt` for expected output), and
relative links to repository files. Match nearby front matter and URL naming
patterns, generally using lowercase, hyphenated paths. Keep examples runnable
and update related `data/` definitions when changing generated reference
content. Use `warn-if-unreleased` or `version-added` for features not yet
deployed.

Use imperative sidebar labels where possible, for example `Update materialized
views`. Use `Getting started`, not `Get started`. Use sentence case for sidebar
labels. Capitalize only proper nouns, product names, and official command,
protocol, or service names.

## Testing Guidelines

There is no unit-test suite for prose. Preview changed pages with Hugo, then
run `../../ci/test/lint-docs.sh` to catch broken links, HTML errors, and
catalog inconsistencies. Review generated diagrams and rendered examples when
changing shortcodes, layouts, or SQL grammar.

## Commits and Pull Requests

Recent commits use a concise `<component>: <imperative summary>` format, often
with the GitHub PR number, for example `docs: clarify source configuration
(#12345)`. Keep commits focused. PRs should explain the user impact, identify
affected pages or data files, link relevant issues, and include screenshots or
preview details for visual changes. Coordinate substantial feature or API
documentation with a technical writer and add release notes when required.
