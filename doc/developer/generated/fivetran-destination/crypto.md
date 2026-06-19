---
source: src/fivetran-destination/src/crypto.rs
revision: 849327076c
---

# mz-fivetran-destination::crypto

`AsyncAesDecrypter<R>` is an `AsyncRead` adapter that AES-256-CBC-decrypts an inner `AsyncRead` stream on the fly using `openssl::symm::Crypter`. Constructed with a key and IV; maintains an internal 4096-byte work buffer (padded by `BLOCK_SIZE` = 16 bytes) and a `done` flag to track when the inner stream is exhausted.
