---
source: src/adapter/src/coord/sequencer/inner/secret.rs
revision: 277b33e9c0
---

# adapter::coord::sequencer::inner::secret

Implements the staged sequencing pipeline for secret and SSH key operations via the `SecretStage` enum and its `Staged` trait implementation.
`sequence_create_secret` validates, writes the secret value to the secrets controller, then persists the catalog entry; `sequence_alter_secret` updates an existing secret's value and additionally calls `check_secret_content_guards_of_dependents` to validate the proposed new contents against every connection that uses the secret before persisting; `sequence_rotate_keys` reads the current SSH key pair, rotates it, writes the new keys to the secrets controller, and updates the owning connection's `create_sql` in the catalog.
Cancellation is disabled for all secret stages because they call external services and transact the catalog separately.
