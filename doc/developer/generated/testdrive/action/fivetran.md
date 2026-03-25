---
source: src/testdrive/src/action/fivetran.rs
revision: 96f48dd2ed
---

# testdrive::action::fivetran

Implements the `fivetran-destination` builtin command, which sends gRPC requests to a running Fivetran Destination service and verifies the response.
Embeds generated protobuf types for the Fivetran SDK v2 and Google well-known types; the `action` argument selects among describe table, create table, alter table, truncate, and write operations.
