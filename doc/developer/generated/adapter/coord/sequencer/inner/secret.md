---
source: src/adapter/src/coord/sequencer/inner/secret.rs
revision: a29f0a64ed
---

# adapter::coord::sequencer::inner::secret

Implements the staged sequencing pipeline for secret and SSH key operations via the `SecretStage` enum and its `Staged` trait implementation.
`sequence_create_secret` validates, writes the secret value to the secrets controller, then persists the catalog entry; `sequence_alter_secret` updates an existing secret's value; `sequence_rotate_keys` reads the current SSH key pair, rotates it, writes the new keys to the secrets controller, and updates the owning connection's `create_sql` in the catalog.
Cancellation is disabled for all secret stages because they call external services and transact the catalog separately.
