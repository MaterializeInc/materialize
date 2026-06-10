---
source: src/sql/src/session/vars/polyfill.rs
revision: 7c8928a3c4
---

# mz-sql::session::vars::polyfill

Provides the `LazyValueFn` trait and the `lazy_value!`/`value!` macros, which work around Rust's lack of const-generic function pointers by wrapping `LazyLock`-backed statics into types that `VarDefinition::new` can accept.
