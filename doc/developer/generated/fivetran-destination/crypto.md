---
source: src/fivetran-destination/src/crypto.rs
revision: 82d92a7fad
---

# mz-fivetran-destination::crypto

Provides `AsyncAesDecrypter`, an `AsyncRead` wrapper that transparently decrypts AES-256-CBC encrypted streams on the fly.
Used by the DML module to decrypt Fivetran-provided batch files before parsing them as CSV.
