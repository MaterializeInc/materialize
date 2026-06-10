---
source: src/ssh-util/src/keys.rs
revision: ed294863cf
---

# mz-ssh-util::keys

Defines `SshKeyPair` (a single Ed25519 key pair in OpenSSH format) and `SshKeyPairSet` (a primary/secondary pair supporting rotation).
`SshKeyPair` generates keys via `aws-lc-rs`, exposes them as OpenSSH-formatted strings, wraps intermediate private-key bytes in `mz_ore::secure::Zeroizing` to ensure they are erased on drop, and implements custom serde serialization for backwards-compatible on-disk encoding.
`SshKeyPairSet::rotate` promotes the secondary key to primary and generates a fresh secondary, enabling zero-downtime key rotation.
