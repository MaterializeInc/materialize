---
source: src/expr/src/scalar/func/format.rs
revision: 4267863081
---

# mz-expr::scalar::func::format

Implements PostgreSQL-compatible date/time formatting and parsing (`to_char`, `to_timestamp`, `to_date`) using a token-based format-string parser.
Defines the `DateTimeToken` enum representing all recognized format patterns (e.g., `YYYY`, `MM`, `HH24`, `TZ`) and the `DateTimeFormat` compiled representation that drives efficient formatting via Aho-Corasick pattern matching.
