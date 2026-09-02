---
source: src/mz-deploy/src/lsp/code_action.rs
revision: d50441fcbe
---

# mz-deploy::lsp::code_action

LSP code-action support.

Owns four concerns that serve the `textDocument/codeAction` flow: the `QuickFixData`/`SuggestionData`/`ReplacementData` JSON payload attached to `Diagnostic.data`, the `build_code_actions` builder that turns a `CodeActionParams` request back into one `CodeAction` per alternative, the `Candidates` struct and `harvest_candidates` helper that build per-kind name pools from the project cache, and the `fuzzy_suggestions` function that matches against those pools for `UnknownItem`, `UnknownSchema`, `UnknownDatabase`, and `UnknownCluster` errors. Fuzzy matching delegates to `crate::suggest::did_you_mean`.
