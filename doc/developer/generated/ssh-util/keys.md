---
source: src/ssh-util/src/keys.rs
revision: 3f18f464ef
---

# mz-ssh-util::keys

Defines `SshKeyPair` (a single Ed25519 key pair in OpenSSH format) and `SshKeyPairSet` (a primary/secondary pair supporting rotation).
`SshKeyPair` generates keys via OpenSSL, exposes them as OpenSSH-formatted strings, and implements custom serde serialization for backwards-compatible on-disk encoding.
`SshKeyPairSet::rotate` promotes the secondary key to primary and generates a fresh secondary, enabling zero-downtime key rotation.
