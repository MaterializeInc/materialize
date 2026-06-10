---
source: src/ore/src/option.rs
revision: 48bde993a2
---

# mz-ore::option

Extends `Option<T>` with two traits: `OptionExt` and `FallibleMapExt`.
`OptionExt` adds `owned` (converts `Option<&T>` to `Option<T::Owned>` via `ToOwned`), `display_or` (returns a `Display`-able `Either<T, D>` with a fallback value), and `display_or_else` (lazy variant that only computes the default when the option is `None`).
`FallibleMapExt` adds `try_map`, a fallible analog of `Option::map` that propagates errors.
