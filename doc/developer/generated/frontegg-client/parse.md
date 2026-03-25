---
source: src/frontegg-client/src/parse.rs
revision: 79dbbcdb89
---

# frontegg-client::parse

Provides `Paginated<T>` and `PaginatedMetadata` for deserializing paginated API responses, and `Empty` for consuming empty response bodies in DELETE requests.
This is a crate-internal module used by the `role` and `user` submodules to iterate over pages.
