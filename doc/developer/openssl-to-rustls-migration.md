# OpenSSL to rustls Migration Plan

## Background

The Materialize codebase currently uses OpenSSL extensively: **137 findings across
28 crates**, covering TLS connections, password hashing, SSH key generation, AES
decryption, and certificate handling. The `deny.toml` actively bans rustls.

This document tracks the plan to replace all OpenSSL usage with rustls (for TLS)
and pure-Rust crypto crates like `ring`, `sha2`, `hmac`, and `pbkdf2` (for
non-TLS cryptography).

**Linear project:** SEC team — "Replace OpenSSL with rustls"

## Tracking Tool

An optional linter identifies all OpenSSL usage:

```bash
bin/lint-openssl            # Human-readable report
bin/lint-openssl --strict   # Exits non-zero if any findings remain
bin/lint-openssl --json     # Machine-readable output
```

Raw output snapshots are in this directory:
- [`openssl-lint-output.txt`](openssl-lint-output.txt) — human-readable
- [`openssl-lint-output.json`](openssl-lint-output.json) — JSON

## Migration Tiers

### Prerequisite: Unblock rustls (SEC-176)

Remove the rustls ban in `deny.toml` (lines 209-212). All other work is blocked
on this.

### Tier 1: Feature-flag swaps only

No source code changes — just swap a Cargo.toml feature.

| Crate | Change | Issue |
|-------|--------|-------|
| mz-cloud-resources | `kube: openssl-tls` -> `rustls-tls` | SEC-177 |
| mz-npm | `reqwest: native-tls-vendored` -> `rustls-tls` | SEC-178 |
| mz-segment | `segment: native-tls-vendored` -> rustls | SEC-179 |
| mz-sql-server-util, mz-storage, mz-storage-types | `tiberius: native-tls` -> `rustls` | SEC-180 |
| mz-testdrive | `duckdb: native-tls`, `reqwest: native-tls-vendored` | SEC-181 |

### Tier 2: Dependency swap, no source changes

| Crate | Change | Issue |
|-------|--------|-------|
| mz (CLI) | Remove `openssl-probe`, `reqwest: default-tls` -> `rustls-tls` | SEC-182 |
| mz-aws-util, mz-dyncfg-launchdarkly | `hyper-tls` -> `hyper-rustls` | SEC-183 |
| mz-persist | Remove `openssl`/`openssl-sys`/`postgres-openssl`, reqwest -> rustls | SEC-184 |

### Tier 3: Light source changes (1-3 lines)

| Crate | What to change | Issue |
|-------|----------------|-------|
| mz-adapter | 1x `openssl::rand::rand_bytes` -> `getrandom` | SEC-185 |
| mz-catalog | `rand_bytes` + `sha256` -> `ring`/`sha2` | SEC-186 |
| mz-ssh-util | Ed25519 keygen via `openssl::pkey` -> `ed25519-dalek` | SEC-187 |
| mz-debug, mz-postgres-util | `postgres-openssl` -> `postgres-rustls` | SEC-188 |
| mz-frontegg-mock, mz-oidc-mock | `openssl::rsa::Rsa` -> `rsa` crate | SEC-189 |

### Tier 4: Moderate source changes

| Crate | What to change | Issue |
|-------|----------------|-------|
| mz-ccsr | `native_tls::Certificate` + openssl identity -> rustls certs | SEC-190 |
| mz-storage-types | `native_tls::Error` + `openssl::error::ErrorStack` variants | SEC-191 |

### Tier 5: Core infrastructure (heavy changes)

These are the critical-path items and should be migrated in dependency order:

| Order | Crate | Scope | Issue |
|-------|-------|-------|-------|
| 1 | **mz-tls-util** | Central TLS abstraction (PKCS12, SslConnector, MakeTlsConnector) | SEC-192 |
| 2 | **mz-ore** | Foundational: openssl + tokio-openssl + native-tls + hyper-tls | SEC-193 |
| 3 | **mz-server-core** | SslAcceptor/SslContext/TlsConfig infrastructure | SEC-194 |
| 4 | **mz-pgwire** + mz-pgwire-common | pgwire protocol TLS (depends on server-core) | SEC-195 |
| 5 | **mz-balancerd** | SSL termination/proxying with SNI (depends on server-core) | SEC-196 |
| 6 | **mz-environmentd** | HTTPS server + 16 source hits in tests (depends on server-core + tls-util) | SEC-197 |
| 7 | **mz-auth** | SCRAM-SHA256 crypto: PBKDF2, HMAC, SHA256, constant-time compare (standalone, not TLS) | SEC-198 |
| 8 | **mz-fivetran-destination** | AES decryption + postgres TLS config | SEC-200 |

### Final: CI enforcement (SEC-199)

Once all crates are migrated:
1. Add `ci/test/lint-main/checks/check-openssl.sh` calling `bin/lint-openssl --strict`
2. Add `openssl` to `deny.toml` bans
3. Verify `bin/lint-openssl --strict` exits 0

## Dependency Graph

```
SEC-176 (deny.toml)
  |
  +---> SEC-192 (mz-tls-util) ---+
  |                               |
  +---> SEC-193 (mz-ore)         +---> SEC-197 (mz-environmentd)
  |                               |
  +---> SEC-194 (mz-server-core) -+
          |
          +---> SEC-195 (mz-pgwire)
          +---> SEC-196 (mz-balancerd)

All Tier 1-3 issues are blocked by SEC-176 but have no other dependencies.
SEC-198 (mz-auth) and SEC-200 (mz-fivetran-destination) are independent.
SEC-199 (CI enforcement) is the final issue after everything else completes.
```

## Replacement Crate Mapping

| OpenSSL usage | Replacement |
|--------------|-------------|
| `openssl::ssl::*` (TLS connections) | `rustls` + `tokio-rustls` |
| `openssl::pkcs12::*` | `rustls-pemfile` (direct PEM loading) |
| `openssl::x509::*` | `rustls` certificate types, `rcgen` for test cert generation |
| `postgres-openssl` | `postgres-rustls` |
| `hyper-openssl` / `hyper-tls` | `hyper-rustls` |
| `tokio-openssl` | `tokio-rustls` |
| `native-tls` / `tokio-native-tls` | Remove (rustls replaces) |
| `openssl::sha::sha256` | `sha2::Sha256` |
| `openssl::sign::Signer` (HMAC) | `hmac::Hmac<Sha256>` |
| `openssl::pkcs5::pbkdf2_hmac` | `pbkdf2` crate |
| `openssl::rand::rand_bytes` | `getrandom` or `ring::rand` |
| `openssl::memcmp::eq` | `subtle::ConstantTimeEq` |
| `openssl::rsa::Rsa` | `rsa` crate |
| `openssl::pkey::PKey` (Ed25519) | `ed25519-dalek` or `ring::signature` |
| `openssl::symm::{Cipher, Crypter}` (AES) | `aes` + `cbc` crates or `ring::aead` |
| `openssl-probe` | `webpki-roots` (bundled Mozilla root certs) |
