---
source: src/adapter/src/coord/sequencer/inner/secret.rs
revision: 5b9fb22e87
---

# adapter::coord::sequencer::inner::secret

Implements `sequence_create_secret` and `sequence_alter_secret`, which write the secret value to the secrets controller and then persist the catalog entry; also implements `sequence_drop_secrets` which removes secret values from the secrets controller during DROP.
