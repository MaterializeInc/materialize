---
source: src/mz-deploy/src/suggest.rs
revision: d50441fcbe
---

# mz-deploy::suggest

Nearest-name suggestions for names that did not resolve.

`did_you_mean` returns up to `MAX_DID_YOU_MEAN` (3) closest names from a candidate set to a given needle, sorted by Damerau-Levenshtein distance ascending. Candidates whose distance exceeds `max(2, needle.len() / 3)` are filtered out so unrelated names do not surface as suggestions. Allocations happen only for surviving candidates.

This is shared between the LSP quick-fix layer (which suggests corrections in the IDE) and the deployment validation error path (which surfaces them in CLI output), so a misspelling reads the same however the user encountered it.
