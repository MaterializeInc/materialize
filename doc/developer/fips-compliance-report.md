# FIPS 140-3 Compliance Report

**Date:** 2026-04-01
**Scope:** Materialize codebase, container images, Kubernetes deployment
**Linear project:** SEC team — "Replace OpenSSL with rustls"

## Executive Summary

Materialize does not currently meet FIPS 140-3 requirements. Cryptographic
operations span three layers — Rust binaries, container images, and Kubernetes
deployment — each with distinct compliance gaps. This report inventories all
gaps, proposes a unified remediation strategy centered on `aws-lc-rs` (a
FIPS-validated crypto library), and outlines an incremental path to offering an
optional FIPS compliance mode.

**Key numbers:**
- **137** openssl findings across **28** Rust crates
- **8** FIPS gaps in production container images
- **4** deployment-layer issues (Helm/orchestratord)
- **38** Linear issues tracking remediation (SEC-176 through SEC-213)

## 1. Rust Binary Layer

### Current State

All cryptographic operations use **OpenSSL** (vendored, v0.10.76). OpenSSL
itself supports a FIPS provider module, but the vendored build used here is
**not** the FIPS-validated binary — it is compiled from source without FIPS
module activation. This means no Rust-level cryptography is currently
FIPS-compliant.

Additionally, several crates use **non-FIPS pure-Rust crypto** libraries
(`sha2`, `hmac`, `subtle`) for hashing and comparison operations.

### Findings by Category

| Category | Crates | Findings | Description |
|----------|--------|----------|-------------|
| Direct openssl dependency | 22 | 47 | `openssl`, `openssl-sys`, `native-tls`, `postgres-openssl`, `tokio-openssl`, `hyper-openssl`, `hyper-tls` in Cargo.toml |
| OpenSSL feature flags | 13 | 18 | `native-tls-vendored`, `openssl-tls`, `default-tls` on third-party deps |
| OpenSSL source imports | 18 | 72 | `use openssl::`, `native_tls::`, `tokio_openssl::` in .rs files |
| Non-FIPS crypto (sha2) | 10 | 10 | Direct `sha2` dependency in workspace crates |
| Non-FIPS crypto (hmac) | 1 | 1 | Direct `hmac` dependency (mz-expr) |
| Non-FIPS crypto (subtle) | 1 | 1 | Direct `subtle` dependency (mz-expr) |

### OpenSSL Usage by Function

| Function | Crates | FIPS Replacement |
|----------|--------|-----------------|
| TLS connections (SSL/TLS) | mz-tls-util, mz-server-core, mz-pgwire, mz-balancerd, mz-environmentd, mz-ccsr, mz-fivetran-destination | `rustls` + `tokio-rustls` with `aws-lc-rs` CryptoProvider |
| PostgreSQL TLS | mz-tls-util, mz-postgres-util, mz-debug, mz-persist, mz-environmentd, mz-fivetran-destination | `postgres-rustls` |
| HTTP/S client | mz-ore, mz-aws-util, mz-dyncfg-launchdarkly, mz-adapter, mz-environmentd | `hyper-rustls`, `reqwest` with `rustls-tls` |
| SCRAM-SHA256 auth | mz-auth | `aws_lc_rs::{hmac, digest, pbkdf2, rand, constant_time}` |
| SHA-256 hashing | mz-catalog, mz-adapter, mz-expr, mz-avro, mz-npm, +5 others | `aws_lc_rs::digest` |
| HMAC-SHA256 | mz-auth, mz-expr | `aws_lc_rs::hmac` |
| PBKDF2 | mz-auth | `aws_lc_rs::pbkdf2` |
| Ed25519 key generation | mz-ssh-util | `aws_lc_rs::signature::Ed25519KeyPair` |
| RSA key generation | mz-oidc-mock, mz-frontegg-mock | `aws_lc_rs::rsa` |
| AES-CBC decryption | mz-fivetran-destination | `aws_lc_rs::cipher` |
| Random number generation | mz-auth, mz-adapter, mz-catalog | `aws_lc_rs::rand` |
| Constant-time comparison | mz-auth, mz-expr | `aws_lc_rs::constant_time::verify_slices_are_equal` |

### Remediation Strategy

**Single crypto backend:** All cryptographic operations will use **`aws-lc-rs`**
(AWS LibCrypto). AWS-LC has FIPS 140-3 validation via the Cryptographic Module
Validation Program (CMVP).

**Dual mode via feature flag:**
- Standard build: `cargo build` — uses `aws-lc-rs` (non-FIPS mode, faster compilation)
- FIPS build: `cargo build --features fips` — uses `aws-lc-rs/fips` (links `aws-lc-fips-sys`, the validated module)

**Migration tiers:**

| Tier | Crates | Effort | Description |
|------|--------|--------|-------------|
| Prerequisite | 2 | Low | Remove rustls ban from deny.toml, add workspace `fips` feature flag |
| 1 — Feature-flag swaps | 6 | Low | Change Cargo.toml features (e.g., `native-tls` → `rustls-tls`) |
| 2 — Dependency swaps | 3 | Low | Replace deps, no source changes |
| 3 — Light source changes | 5 | Low-Medium | 1-3 lines of code per crate |
| 4 — Moderate changes | 2 | Medium | Error types, certificate handling |
| 5 — Core infrastructure | 8 | High | TLS abstractions, server config, auth crypto |
| Cross-cutting | 1 | Medium | Migrate existing sha2/hmac/subtle in 10+ crates |
| CI enforcement | 1 | Low | Enable `lint-openssl --strict` in CI |

### Preventive Controls (Implemented)

The following `deny.toml` bans are already in place to prevent introduction of
new non-FIPS crypto dependencies:

- `ring` — banned (wrapper: `aws-config`)
- `sha2` — banned (wrappers for 10 workspace + 10 third-party crates, to be removed as migrated)
- `hmac` — banned (wrappers for 1 workspace + 5 third-party crates)
- `subtle` — banned (wrappers for 1 workspace + 2 third-party crates)
- `pbkdf2`, `ed25519-dalek`, `aes`, `cbc`, `rsa` — banned (no current usage)

### Tracking

- **Linter:** `bin/lint-openssl` — run to see current openssl findings
- **Linter:** `bin/lint-fips` (planned, SEC-203) — will detect non-FIPS crypto API usage in source
- **Linear issues:** SEC-176 through SEC-206

---

## 2. Container Image Layer

### Current State

Production containers are built from 4 base images, none of which use
FIPS-validated system crypto libraries. System packages (curl, openssh, nginx,
PostgreSQL) link against Ubuntu's default OpenSSL, which is not in FIPS mode.

### Production Base Images

| Image | Base | FIPS Status | Crypto Packages |
|-------|------|-------------|-----------------|
| `ubuntu-base` | `ubuntu:noble-20260210.1` | Not FIPS | `eatmydata` only |
| `prod-base` | Inherits ubuntu-base | Not FIPS | `ca-certificates`, `curl`, `openssh-client` |
| `materialized-base` | Inherits ubuntu-base | Not FIPS | `curl`, `openssh-client`, `nginx`, `postgresql-16` |
| `distroless-prod-base` | `gcr.io/distroless/cc-debian13:nonroot` | Not FIPS | Minimal C runtime |

### Production Application Images

| Image | Base | Binary | FIPS Risk |
|-------|------|--------|-----------|
| `environmentd` | prod-base | environmentd | High — main SQL engine, handles client TLS |
| `clusterd` | prod-base | clusterd | High — compute worker |
| `materialized` | materialized-base | materialized | High — all-in-one, includes PostgreSQL + nginx |
| `balancerd` | distroless-prod-base | balancerd | High — TLS termination |
| `orchestratord` | distroless-prod-base | orchestratord | Medium — operator, makes HTTPS calls |
| `fivetran-destination` | distroless-prod-base | mz-fivetran-destination | Medium — handles encrypted data |
| `mz` (CLI) | prod-base | mz | Low — client tool |

### Remediation Strategy

**FIPS base images:** Create FIPS variants of the 4 base images:

1. **ubuntu-base** → Ubuntu Pro FIPS (`ubuntu-pro-fips:noble`) or custom base with
   `pro enable fips-updates`. This provides FIPS-validated OpenSSL for all system
   packages.
2. **prod-base** → Inherits from FIPS ubuntu-base. `curl` and `openssh-client`
   automatically use the FIPS OpenSSL.
3. **materialized-base** → Inherits from FIPS ubuntu-base. `nginx` and
   `postgresql-16` automatically use the FIPS OpenSSL.
4. **distroless-prod-base** → No Google FIPS variant exists. Options: build
   custom distroless with FIPS libs, or switch to Red Hat UBI minimal for
   balancerd/orchestratord/fivetran-destination.

**Dual mode:** Build FIPS and non-FIPS container variants in CI. FIPS images get
a `-fips` tag suffix (e.g., `materialize/environmentd:v26.19.0-fips`).

### Test Certificate Generation

The `test/test-certs/create-certs.sh` script uses:
- RSA 4096-bit keys — FIPS-compliant
- SHA-256 signatures — FIPS-compliant
- No weak algorithms detected

No remediation needed for test cert generation.

### Tracking

- **Linter:** `bin/lint-fips-containers` — run to see current container findings
- **Linear issues:** SEC-207 (base images), SEC-208 (linter), SEC-209 (system packages)

---

## 3. Kubernetes/Helm Deployment Layer

### Current State

The orchestratord operator and Helm charts create Kubernetes resources
(StatefulSets, Deployments, Pods) with no FIPS validation or enforcement.

### Findings

#### 3.1 Container Image References — No FIPS Validation

The orchestratord creates pods from user-specified image references in the
Materialize CRD. There is no validation that these images are FIPS-compiled.

| CRD Field | Pod Created | Risk |
|-----------|-------------|------|
| `spec.environmentdImageRef` | environmentd StatefulSet | Critical |
| (derived from above) | clusterd pods | Critical |
| `spec.balancerdImageRef` | balancerd Deployment | High |
| `spec.consoleImageRef` | console Deployment | Medium |

#### 3.2 Ed25519 Certificate Algorithm — Not FIPS-Approved

The orchestratord's TLS certificate creation (`src/orchestratord/src/tls.rs`)
accepts Ed25519 as a key algorithm for cert-manager certificates. Ed25519 is
**not** FIPS 140-3 approved for digital signatures. RSA (2048+) and ECDSA
(P-256/P-384/P-521) are the approved alternatives.

#### 3.3 External Service Connections

The orchestratord injects configuration for outbound connections that all use
the binary's TLS stack:

| Service | Config | Protocol | FIPS Concern |
|---------|--------|----------|-------------|
| Segment Analytics | `--segment-api-key` | HTTPS | Must use FIPS TLS client |
| OpenTelemetry | `--opentelemetry-endpoint` | gRPC/HTTPS | Must use FIPS TLS client |
| Sentry | `--sentry-dsn` | HTTPS | Must use FIPS TLS client |
| AWS Secrets Manager | `secretsController: aws-secrets-manager` | HTTPS | AWS SDK must use FIPS endpoint |
| environmentd API | Internal HTTP calls | HTTP/HTTPS | orchestratord uses `reqwest` |

After the Rust binary migration to `rustls` + `aws-lc-rs`, all these
connections will use the FIPS crypto backend. No separate remediation needed
beyond the binary migration — but this should be verified (SEC-212).

#### 3.4 User-Controllable FIPS Escape Hatches

| CRD Field | Risk | Mitigation |
|-----------|------|------------|
| `environmentd_extra_env` | Can inject env vars affecting crypto | Validate in FIPS mode |
| `environmentd_extra_args` | Can pass arbitrary CLI flags | Validate in FIPS mode |
| Certificate `algorithm: Ed25519` | Not FIPS-approved | Block in FIPS mode |

#### 3.5 Prometheus Metrics — Plaintext HTTP

Metrics endpoints default to `prometheus.io/scheme: "http"`. While not a FIPS
violation, unencrypted metrics could leak sensitive information. In FIPS mode,
consider requiring HTTPS for scrape endpoints.

### Remediation Strategy

Add a `fips.enabled` toggle to the Helm chart (`values.yaml`) that, when
activated:

1. Selects FIPS-compiled container images (e.g., `-fips` tag suffix)
2. Blocks Ed25519 certificate algorithms
3. Enforces `--tls-mode=require` (no plaintext)
4. Passes `--fips` to Rust binaries to activate `aws-lc-rs` FIPS mode
5. Validates user-supplied env vars and args
6. Optionally sets Prometheus scrape scheme to `https`

### Tracking

- **Linear issues:** SEC-210 (Ed25519), SEC-211 (image validation), SEC-212
  (external services), SEC-213 (Helm FIPS toggle)

---

## 4. FIPS Validation Caveat

FIPS 140-3 validation is tied to a **specific binary build** of a cryptographic
module. Using `aws-lc-fips-sys` (compiled from source) produces the **same
code** as the CMVP-validated module, but the compiled binary is not itself
listed on the CMVP certificate.

For formal FIPS compliance, one of the following is typically required:
- **Vendor assertion:** AWS provides documentation that builds from the
  validated source code produce equivalent modules
- **Independent validation:** Submit the compiled module for CMVP testing
- **Operational environment:** Deploy on an AWS platform where the validated
  module is pre-installed

This is a compliance/legal question, not a technical one. Discuss with your
compliance team early in the process.

---

## 5. Compliance Linters

Three linters have been created (or planned) to continuously track FIPS
compliance:

| Linter | Status | What it checks | Findings |
|--------|--------|----------------|----------|
| `bin/lint-openssl` | Implemented | OpenSSL deps, features, imports in Rust code | 137 |
| `bin/lint-fips-containers` | Implemented | Non-FIPS base images, crypto packages in Dockerfiles | 38 (8 production) |
| `bin/lint-fips` | Planned (SEC-203) | Non-FIPS crypto API usage (sha2, hmac, ring, etc.) in Rust source | TBD |

**Runtime tests (planned):**
- SEC-204: FIPS CryptoProvider verification test (aws-lc-rs active, FIPS indicator set)
- SEC-205: TLS protocol and cipher suite audit (min TLS 1.2, FIPS cipher suites, key sizes)

---

## 6. Issue Inventory

### Prerequisites
| Issue | Title | Priority |
|-------|-------|----------|
| SEC-176 | Remove rustls ban from deny.toml | High |
| SEC-201 | Add workspace-level fips feature flag with aws-lc-rs | High |
| SEC-202 | Add deny.toml bans for non-FIPS crypto crates | High (done) |

### Rust Binary Migration (28 crates)
| Issue | Crate(s) | Tier | Priority |
|-------|----------|------|----------|
| SEC-177 | mz-cloud-resources | 1 | Low |
| SEC-178 | mz-npm | 1 | Low |
| SEC-179 | mz-segment | 1 | Low |
| SEC-180 | mz-sql-server-util, mz-storage, mz-storage-types | 1 | Low |
| SEC-181 | mz-testdrive | 1 | Low |
| SEC-182 | mz (CLI) | 2 | Low |
| SEC-183 | mz-aws-util, mz-dyncfg-launchdarkly | 2 | Low |
| SEC-184 | mz-persist | 2 | Low |
| SEC-185 | mz-adapter | 3 | Low |
| SEC-186 | mz-catalog | 3 | Low |
| SEC-187 | mz-ssh-util | 3 | Medium |
| SEC-188 | mz-debug, mz-postgres-util | 3 | Low |
| SEC-189 | mz-frontegg-mock, mz-oidc-mock | 3 | Low |
| SEC-190 | mz-ccsr | 4 | Medium |
| SEC-191 | mz-storage-types | 4 | Medium |
| SEC-192 | mz-tls-util (central abstraction) | 5 | High |
| SEC-193 | mz-ore (foundational) | 5 | High |
| SEC-194 | mz-server-core | 5 | High |
| SEC-195 | mz-pgwire, mz-pgwire-common | 5 | High |
| SEC-196 | mz-balancerd | 5 | High |
| SEC-197 | mz-environmentd | 5 | High |
| SEC-198 | mz-auth (SCRAM-SHA256) | 5 | High |
| SEC-200 | mz-fivetran-destination | 5 | Medium |
| SEC-206 | mz-expr, mz-avro + 8 others (sha2/hmac/subtle) | Cross-cutting | Medium |

### Linters and Tests
| Issue | Title | Priority |
|-------|-------|----------|
| SEC-199 | Enable lint-openssl --strict in CI | Low |
| SEC-203 | Add bin/lint-fips source-level scanner | Medium |
| SEC-204 | Add FIPS CryptoProvider verification test | Medium |
| SEC-205 | Add TLS protocol and cipher suite audit test | Medium |
| SEC-208 | Add bin/lint-fips-containers (done) | High |

### Container Images
| Issue | Title | Priority |
|-------|-------|----------|
| SEC-207 | Create FIPS-validated base container images | High |
| SEC-209 | Evaluate system crypto packages in prod containers | Medium |

### Kubernetes/Helm Deployment
| Issue | Title | Priority |
|-------|-------|----------|
| SEC-210 | Block Ed25519 certificate algorithm in FIPS mode | High |
| SEC-211 | Validate container images are FIPS-built | Medium |
| SEC-212 | Audit external service connections for FIPS TLS | Medium |
| SEC-213 | Add Helm chart FIPS mode configuration | Medium |

---

## 7. Recommended Execution Order

**Phase 1 — Foundation (unblocks everything):**
1. SEC-176: Remove rustls ban
2. SEC-201: Add workspace `fips` feature flag
3. SEC-202: deny.toml bans (done)
4. SEC-208: Container FIPS linter (done)

**Phase 2 — Quick wins (Tier 1-2, 9 crates):**
5. SEC-177–181: Feature-flag swaps
6. SEC-182–184: Dependency swaps

**Phase 3 — Core infrastructure (highest risk, most dependencies):**
7. SEC-192: mz-tls-util (central abstraction, unblocks downstream)
8. SEC-193: mz-ore (foundational)
9. SEC-194: mz-server-core (TLS server config)

**Phase 4 — TLS server chain:**
10. SEC-195: mz-pgwire
11. SEC-196: mz-balancerd
12. SEC-197: mz-environmentd

**Phase 5 — Crypto and remaining crates:**
13. SEC-198: mz-auth (SCRAM-SHA256 — security-critical)
14. SEC-200: mz-fivetran-destination (AES + TLS)
15. SEC-185–191: Tier 3-4 crates
16. SEC-206: sha2/hmac/subtle migration across 10+ crates

**Phase 6 — Container and deployment hardening:**
17. SEC-207: FIPS base container images
18. SEC-209: Audit system crypto packages
19. SEC-210: Block Ed25519 in FIPS mode
20. SEC-213: Helm FIPS toggle
21. SEC-211: Image FIPS validation
22. SEC-212: External service audit

**Phase 7 — Verification and enforcement:**
23. SEC-203: lint-fips source scanner
24. SEC-204: CryptoProvider verification test
25. SEC-205: TLS protocol audit test
26. SEC-199: Enable lint-openssl --strict in CI
