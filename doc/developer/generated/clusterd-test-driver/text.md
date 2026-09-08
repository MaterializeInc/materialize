---
source: src/clusterd-test-driver/src/text.rs
revision: cb4e73fb79
---

# mz-clusterd-test-driver::text

The text script format: a hand-writable, `datadriven`-style command file format and its parser/rewriter.

A script is a sequence of stanzas. Each stanza is a command (a directive line plus an optional indentation-structured body) followed by a `----` separator and the expected output. The expected output block is the assertion; setting `REWRITE=1` regenerates it in place. A `#` at column 0 is a comment; blank lines and comments are preserved on rewrite.

Expected output that itself contains blank lines (such as the multi-object `explain` plan render, which separates objects with a blank line) uses the doubled-separator form: the directive line, then `----`, then `----`, then the expected output, closed by a matching `----`/`----` pair. `REWRITE` emits this form automatically when the actual output contains a blank line, and the parser handles it transparently.

`parse_file` parses a script into `Item`s: each `Item::Stanza` holds the raw input block, the expected output string, and the parsed `Command`; `Item::Verbatim` holds a blank line or column-0 comment. `rewrite` reproduces the file with each stanza's expected block replaced by the actual output, choosing the doubled-`----` form when the output contains a blank line.

Command bodies are indentation-structured. `define-schema` and `write-rows` carry column declarations and row tokens respectively. `create-dataflow` and `explain` carry `import`/`build`/`export` sub-commands, with each `build`'s MIR as a deeper-indented sub-body. `write-rows` payloads are typed against the schema token-by-token via `cell_from_token` (reusing `mz_repr::strconv`).

The `explain` verb accepts either a full `create-dataflow` body (inline form) or `ref=<name>` (reference form, no body). The `create-instance` command accepts an optional body of `name type value` rows (the same format as `update-configuration`) to supply an `initial_config` snapshot delivered to the replica before create-time setup.
