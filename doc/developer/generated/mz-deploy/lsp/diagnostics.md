---
source: src/mz-deploy/src/lsp/diagnostics.rs
revision: 24424b9efb
---

# mz-deploy::lsp::diagnostics

LSP-specific diagnostic emission.
The internal `position_to_offset` helper converts an LSP `Position` (line, character) to a byte offset in the rope, returning `None` if the position's line lies past the end of the document to prevent a panic on out-of-range access.
