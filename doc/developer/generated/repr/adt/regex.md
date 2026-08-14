---
source: src/repr/src/adt/regex.rs
revision: 07d08ad9c
---

# mz-repr::adt::regex

Defines a `Regex` newtype wrapping `regex::Regex` with `Serialize`/`Deserialize`, `PartialOrd`, `Hash`, and `Arbitrary` implementations needed to use compiled regexes as `Datum` values.
The struct stores `case_insensitive` and `dot_matches_new_line` flags alongside the compiled `regex::Regex`. Equality, ordering, and hashing are based on the pattern string plus these flags rather than language equivalence. Serialization is handled by a manual `Serialize`/`Deserialize` implementation that stores the pattern and flags as struct fields and reconstructs the compiled regex on deserialization, avoiding reliance on the underlying `regex::Regex` serialization.

Compilation enforces three limits to prevent envd OOMs:

* `MAX_REGEX_SIZE_BEFORE_COMPILATION` (1 MiB): patterns exceeding this byte length are rejected before any parsing occurs.
* `MAX_REGEX_CHARACTER_CLASSES` (2000): patterns containing more than this many character-class nodes (Unicode/Perl/POSIX class nodes and bracketed-class range items, counted by `CharacterClassCounter`) are rejected before the regex crate's NFA compiler runs. These are the node kinds whose AST-to-HIR translation is not bounded by the pattern's byte length.
* `MAX_REGEX_SIZE_AFTER_COMPILATION` (10 MiB): passed to `RegexBuilder::size_limit` to cap the compiled NFA.

`RegexCompilationError` has three variants: `RegexError` (wrapping the `regex` crate error), `PatternTooLarge` (byte length exceeded before compilation), and `TooManyCharacterClasses` (character class count exceeded before compilation).

`Regex::new` defaults to `dot_matches_new_line: true` matching PostgreSQL's newline-sensitive matching rules. `Regex::new_dot_matches_new_line` allows explicit control.
