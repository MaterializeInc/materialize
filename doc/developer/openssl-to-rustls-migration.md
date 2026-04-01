# OpenSSL to rustls Migration Plan

## Background

The Materialize codebase currently uses OpenSSL extensively: **137 findings across
28 crates**, covering TLS connections, password hashing, SSH key generation, AES
decryption, and certificate handling. The `deny.toml` actively bans rustls.

This project replaces all OpenSSL usage with **rustls** (for TLS) and
**aws-lc-rs** (for all cryptographic primitives), with an optional **FIPS 140-3
compliance mode**.

**Linear project:** SEC team — "Replace OpenSSL with rustls"

## FIPS 140-3 Strategy

All cryptographic operations use **`aws-lc-rs`** as the single crypto backend:

- **TLS:** `rustls` configured with `aws-lc-rs` as its `CryptoProvider`
- **Non-TLS crypto:** `aws-lc-rs` directly (digest, HMAC, PBKDF2, AES, RSA, Ed25519, CSPRNG)
- **FIPS mode:** A workspace-level `fips` feature flag enables `aws-lc-rs/fips`,
  which uses `aws-lc-fips-sys` (the FIPS 140-3 validated module, CMVP certified)
- **Standard builds:** `cargo build` (uses `aws-lc-rs` without FIPS)
- **FIPS builds:** `cargo build --features fips`

**Why aws-lc-rs (not ring, sha2, hmac, etc.):**
- AWS-LC has FIPS 140-3 validation; `ring`, `sha2`, `hmac`, `pbkdf2`, `subtle`,
  `rsa`, `ed25519-dalek`, and `aes`+`cbc` do **not**
- It is the default crypto backend for rustls since 0.22+
- Single dependency covers all crypto primitives needed
- Consistent backend simplifies FIPS audit scope

**Caveat:** FIPS 140-3 validation is tied to a specific binary build. Compiling
`aws-lc-fips-sys` from source produces the same code as the validated module, but
the resulting binary is not itself "validated" without a vendor assertion or
independent validation. Discuss with your compliance team.

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

### Prerequisites

**Unblock rustls (SEC-176):** Remove the rustls ban in `deny.toml` (lines
209-212). All other work is blocked on this.

**Add workspace fips feature flag (SEC-201):** Establish `aws-lc-rs` as the
crypto backend with a `fips` feature toggle. Should be done early so all migrated
crates use the right backend from the start.

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
| mz-adapter | 1x `openssl::rand::rand_bytes` -> `aws_lc_rs::rand` | SEC-185 |
| mz-catalog | `rand_bytes` + `sha256` -> `aws_lc_rs::{rand, digest}` | SEC-186 |
| mz-ssh-util | Ed25519 keygen -> `aws_lc_rs::signature::Ed25519KeyPair` | SEC-187 |
| mz-debug, mz-postgres-util | `postgres-openssl` -> `postgres-rustls` | SEC-188 |
| mz-frontegg-mock, mz-oidc-mock | `openssl::rsa::Rsa` -> `aws_lc_rs::rsa` | SEC-189 |

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
| 7 | **mz-auth** | SCRAM-SHA256 crypto: PBKDF2, HMAC, SHA256, constant-time compare -> `aws-lc-rs` | SEC-198 |
| 8 | **mz-fivetran-destination** | AES decryption + postgres TLS config -> `aws-lc-rs` + rustls | SEC-200 |

### Final: CI enforcement (SEC-199)

Once all crates are migrated:
1. Add `ci/test/lint-main/checks/check-openssl.sh` calling `bin/lint-openssl --strict`
2. Add `openssl` to `deny.toml` bans
3. Verify `bin/lint-openssl --strict` exits 0

## Dependency Graph

```
SEC-176 (deny.toml)
  |
  +---> SEC-201 (fips feature flag)
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

| OpenSSL usage | FIPS-compatible replacement |
|--------------|----------------------------|
| `openssl::ssl::*` (TLS connections) | `rustls` + `tokio-rustls` (with `aws-lc-rs` CryptoProvider) |
| `openssl::pkcs12::*` | `rustls-pemfile` (direct PEM loading) |
| `openssl::x509::*` | `rustls` certificate types, `rcgen` for test cert generation |
| `postgres-openssl` | `postgres-rustls` |
| `hyper-openssl` / `hyper-tls` | `hyper-rustls` |
| `tokio-openssl` | `tokio-rustls` |
| `native-tls` / `tokio-native-tls` | Remove (rustls replaces) |
| `openssl::sha::sha256` | `aws_lc_rs::digest` (SHA256) |
| `openssl::sign::Signer` (HMAC) | `aws_lc_rs::hmac` |
| `openssl::pkcs5::pbkdf2_hmac` | `aws_lc_rs::pbkdf2` |
| `openssl::rand::rand_bytes` | `aws_lc_rs::rand` (or `getrandom` for OS entropy) |
| `openssl::memcmp::eq` | `aws_lc_rs::constant_time::verify_slices_are_equal` |
| `openssl::rsa::Rsa` | `aws_lc_rs::rsa` |
| `openssl::pkey::PKey` (Ed25519) | `aws_lc_rs::signature::Ed25519KeyPair` |
| `openssl::symm::{Cipher, Crypter}` (AES) | `aws_lc_rs::cipher` (AES-CBC) |
| `openssl-probe` | `webpki-roots` (bundled Mozilla root certs) |
