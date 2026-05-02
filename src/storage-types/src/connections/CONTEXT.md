# storage-types::connections

All external-system connection types and the runtime context for instantiating them.

## Files (LOC ≈ 2,945 across this directory)

| File | What it owns |
|---|---|
| `connections.rs` (parent) | `Connection` enum (Kafka, CSR, Postgres, SSH, AWS, AwsPrivateLink, MySQL, SqlServer, IcebergCatalog); `ConnectionContext` (SSH tunnel managers, secrets reader, cloud-resource reader); per-connection `connect()` impls; `KafkaTopicOptions`; `IcebergCatalogConnection` with `IcebergCatalogImpl` (Rest / S3TablesRest) |
| `aws.rs` | `AwsConnection`, `AwsAuth`, `AwsConnectionReference`, `AwsConnectionValidationError`; assume-role credential chains for Iceberg FileIO/OpenDAL via `AwsSdkCredentialLoader` |
| `inline.rs` | `ReferencedConnection` / `InlinedConnection` polymorphism; `ConnectionResolver` + `ConnectionAccess` traits; `IntoInlineConnection` for resolving catalog references into inline values at rendering time |
| `string_or_secret.rs` | `StringOrSecret` — a value that is either a literal string or a reference to a secrets-controller secret |

## Key concepts

- **`Connection` enum** — the exhaustive set of connection backends. Each variant is a struct carrying enough configuration to open a live connection to the external system.
- **`ConnectionContext`** — global runtime state injected at process start; holds `SshTunnelManager`, `SecretsReader`, `AwsExternalIdPrefix`, and similar singletons. Passed through to `connect()` calls at rendering time.
- **`InlinedConnection` / `ReferencedConnection`** — two-phase design: catalog stores `ReferencedConnection` (holds `CatalogItemId`); rendering resolves to `InlinedConnection` (holds the full config). Prevents secret material from living in the catalog.
- **`AlterCompatible`** — all connection types implement this to declare which fields may change across `ALTER CONNECTION`. Violations return `AlterError` to the adapter.

## Cross-references

- Parent `connections.rs` (one level up) is the primary module file.
- `mz-secrets::SecretsReader`, `mz-ssh-util::tunnel_manager::SshTunnelManager`, `mz-cloud-resources` — runtime dependencies injected via `ConnectionContext`.
- Generated developer docs: `doc/developer/generated/storage-types/connections/`.
