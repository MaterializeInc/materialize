---
source: src/ore/src/str.rs
revision: 537b22fcd2
---

# mz-ore::str

Provides string formatting and manipulation utilities used throughout Materialize.
`StrExt` adds `quoted` (wraps in double quotes, escaping inner quotes) and `escaped` (additionally escapes `\n`, `\r`, `\t`) to `str`; the corresponding `QuotedStr` and `EscapedStr` newtypes implement `Display`.
`separated` produces a `Display` value that joins an iterator with a separator string; `bracketed` wraps any `Display` in open/close delimiters; `closure_to_display` adapts a closure to `Display`.
`Indent` tracks indentation levels (with mark/reset support) and implements `AddAssign`/`SubAssign<usize>`; the `IndentLike` trait provides `indented` and `indented_if` convenience methods for any type that can yield a `&mut Indent`.
`MaxLenString<const MAX: usize>` is a newtype `String` whose byte length is enforced at construction time.
`redact` returns a `Debug` wrapper that replaces alphanumeric characters with placeholders unless soft assertions are enabled or the alternate format flag is set.
