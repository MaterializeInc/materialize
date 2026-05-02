---
source: src/ore/src/secure.rs
revision: b69d9bb412
---

# ore::secure

Utilities for handling sensitive data that must be zeroed from memory on drop.

## Overview

This module provides wrapper types and re-exports for managing secrets (passwords, keys, tokens, credentials) so they are automatically zeroed when dropped and redacted in logs.

## Re-exports from the `zeroize` crate

| Symbol | Description |
|--------|-------------|
| `Zeroize` | Trait for types that can be zeroed in place |
| `ZeroizeOnDrop` | Marker trait indicating `Drop` zeroes the value |
| `Zeroizing<T>` | Generic wrapper that zeroes `T` on drop; derefs to `T` |

Downstream crates should import these from `mz_ore::secure` rather than adding a direct `zeroize` dependency. This keeps the zeroize version consistent across the workspace.

## Types

### `SecureString`

A `String` wrapper that:
- Is zeroed from memory when dropped (`ZeroizeOnDrop`)
- Redacts its contents in `Debug` and `Display` output
- Does **not** implement `Clone` (prevents untracked copies of secrets)

Use `unsecure()` to access the inner `&str` when the plaintext is needed.

### `SecureVec`

A `Vec<u8>` wrapper with the same properties as `SecureString`.

Use `unsecure()` to access the inner `&[u8]`.

## Usage

```rust
use mz_ore::secure::{SecureString, SecureVec, Zeroizing};

// Wrap a password — zeroed on drop, redacted in logs
let password = SecureString::from("hunter2");
assert_eq!(password.unsecure(), "hunter2");
assert_eq!(format!("{:?}", password), "SecureString(<redacted>)");

// Wrap raw key bytes
let key = SecureVec::from(vec![0xDE, 0xAD, 0xBE, 0xEF]);
assert_eq!(key.unsecure(), &[0xDE, 0xAD, 0xBE, 0xEF]);

// Use Zeroizing<T> for temporary stack buffers
let buf = Zeroizing::new([0u8; 32]);
// buf is zeroed when it goes out of scope
```

## When to use

| Scenario | Type |
|----------|------|
| Passwords, tokens, credentials as strings | `SecureString` |
| Raw key material, secret bytes | `SecureVec` |
| Temporary buffers holding derived keys, nonces, HMACs | `Zeroizing<[u8; N]>` or `Zeroizing<Vec<u8>>` |
| Struct fields that hold secrets | Derive `Zeroize` + `ZeroizeOnDrop` on the struct |

## Related

- `ore::url::SensitiveUrl` — URL wrapper that redacts passwords in `Display`/`Debug`
- `ore::str::redact()` — debug-output redaction for arbitrary values
- `mz-ssh-util` — uses `Zeroizing<String>` and `Zeroizing<Vec<u8>>` for SSH private keys (reference implementation)
