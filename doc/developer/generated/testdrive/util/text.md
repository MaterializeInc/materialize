---
source: src/testdrive/src/util/text.rs
revision: 5c7a88f770
---

# testdrive::util::text

Provides two small text utilities used when comparing query output.
`trim_trailing_space` strips trailing whitespace from each line and removes trailing empty lines.
`print_diff` renders a colorized unified diff of two strings to stdout, using green for insertions and red for deletions.
