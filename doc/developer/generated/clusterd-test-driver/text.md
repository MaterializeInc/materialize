---
source: src/clusterd-test-driver/src/text.rs
revision: 6d4c0fbb2b
---

# mz-clusterd-test-driver::text

The text script format: a hand-writable, `datadriven`-style command file format and its executor.

A script is a sequence of stanzas. Each stanza is a command (a directive line plus an optional indentation-structured body) followed by a `----` separator and the expected output. The expected output block is the assertion; setting `REWRITE=1` regenerates it in place. A `#` at column 0 is a comment; blank lines and comments are preserved on rewrite.

`run(path, driver)` parses the script file and executes each command against the `Driver`, comparing actual output to the expected block. A command failure renders as `error: <message>`, so expected failures are verified by their golden block. Assertions use level-triggered waits on monotonic frontiers, making a single sequential script deterministic.

The `define` command carries arbitrary MIR (as pretty-form specs parsed by `mz-expr-parser`), including index imports, over the full `DataflowBuilder` surface. `define_index` is sugar for the common single-index shape. `write-rows` payloads are typed against the schema token-by-token via `cell_from_token` (reusing `mz_repr::strconv`).
