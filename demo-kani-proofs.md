# Kani Model Checking Proofs for Persist Envelope Encryption

*2026-03-07T00:02:54Z by Showboat 0.6.1*
<!-- showboat-id: 9a14c131-9c25-4098-9745-0ae08c4b6c37 -->

## Overview

The persist layer's `crypto.rs` implements KMS envelope encryption with two formats:
- **V1** (single-key): MZ-managed KMS wraps the DEK
- **V2** (two-party): DEK is double-wrapped with both MZ and customer KMS keys

The AES-256-GCM primitives come from `aws-lc-rs` (formally verified by AWS), but our
custom envelope logic — format construction, parsing, version dispatch, bounds checking —
needs verification. We use two complementary approaches:

1. **Unit tests** (19 tests) — check specific inputs and known edge cases
2. **Property-based tests** (4 proptest properties) — fuzz the full encrypt→decrypt pipeline with random inputs
3. **Kani bounded model checking** (9 proofs) — exhaustively prove properties hold for *all* inputs within bounds

This document demonstrates both passing.

### Architecture of the proofs

The parsing logic was factored into `validate_envelope_header` (pure arithmetic, returns
`Option<(u8, usize)>`) which `parse_envelope` delegates to. Similarly, the header-writing
logic was factored into `write_envelope_header`, and the decrypt length check into
`validate_decrypt_input`. The Kani proofs target these pure inner functions, avoiding
`anyhow` and allocation overhead that would make the SAT solver intractable.

## Part 1: Existing Unit Tests

All 23 crypto tests verify specific behaviors: 19 unit tests (roundtrips, tamper detection,
wrong-key rejection, version validation, truncated-data rejection, two-party encryption,
EncryptedBlob/EncryptedConsensus wrappers, mixed V1/V2 scenarios) plus 4 proptest
property-based tests that fuzz the full encrypt→decrypt pipeline.

```bash
cargo test -p mz-persist -- crypto 2>&1
```

```output
    Finished `test` profile [unoptimized + debuginfo] target(s) in 5.08s
     Running unittests src/lib.rs (target/debug/deps/mz_persist-76484e00c0355782)

running 23 tests
test crypto::tests::truncated_data_rejected ... ok
test crypto::tests::two_party_customer_key_revocation ... ok
test crypto::tests::tamper_detection ... ok
test crypto::tests::roundtrip_empty_plaintext ... ok
test crypto::tests::roundtrip ... ok
test crypto::tests::envelope_format_parsing ... ok
test crypto::tests::two_party_envelope_format ... ok
test crypto::tests::two_party_encrypted_consensus_roundtrip ... ok
test crypto::tests::encrypted_consensus_data_is_actually_encrypted ... ok
test crypto::tests::encrypted_consensus_roundtrip ... ok
test crypto::tests::two_party_roundtrip ... ok
test crypto::tests::two_party_v1_backward_compat ... ok
test crypto::tests::two_party_encrypted_blob_roundtrip ... ok
test crypto::tests::version_byte_validation ... ok
test crypto::tests::two_party_v2_requires_customer_key ... ok
test crypto::tests::wrong_key_fails ... ok
test crypto::tests::two_party_mixed_versions ... ok
test crypto::tests::encrypted_blob_roundtrip ... ok
test crypto::tests::encrypted_consensus_impl_test ... ok
test crypto::tests::proptest_parse_envelope_never_panics ... ok
test crypto::tests::proptest_decrypt_with_key_never_panics ... ok
test crypto::tests::proptest_tampered_ciphertext_detected ... ok
test crypto::tests::proptest_encrypt_decrypt_roundtrip ... ok

test result: ok. 23 passed; 0 failed; 0 ignored; 0 measured; 19 filtered out; finished in 0.12s

   Doc-tests mz_persist

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

```

All 23 tests pass. The 19 unit tests verify specific behaviors, while the 4 proptest
properties fuzz the pipeline with random keys, plaintexts (0–1024 bytes), wrapped DEKs
(0–256 bytes), and both V1/V2 versions:

- **`proptest_encrypt_decrypt_roundtrip`** — random encrypt→parse→decrypt always recovers the original plaintext
- **`proptest_parse_envelope_never_panics`** — random byte vectors never cause a panic in `parse_envelope`
- **`proptest_decrypt_with_key_never_panics`** — random data never causes a panic in `decrypt_with_key`
- **`proptest_tampered_ciphertext_detected`** — flipping any byte in nonce+ciphertext is always caught by AEAD

## Part 2: Normal Build Is Unaffected

The Kani harnesses live inside `#[cfg(kani)]` — completely invisible to normal compilation.

```bash
cargo check -p mz-persist 2>&1
```

```output
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.50s
```

Clean — no warnings, no errors.

## Part 3: Kani Bounded Model Checking Proofs

Each harness uses `kani::any()` to generate *all possible* inputs within bounds (up to
36 bytes for the symbolic array proofs). Kani then exhaustively verifies that every
assertion holds and no panic is reachable — for every possible input, not just specific
test cases.

### Proof 1: `validate_envelope_header_no_panic`

Proves the parsing function never panics on **any** byte slice up to 36 bytes. This
covers all 2^(36×8) ≈ 10^86 possible inputs — every version byte, every wrapped-DEK
length encoding, every truncation pattern.

```bash
cargo kani -p mz-persist --harness validate_envelope_header_no_panic 2>&1 | grep -E '(Checking harness|SUMMARY|failed|VERIFICATION|Verification Time|Complete)'
```

```output
Checking harness crypto::kani_proofs::validate_envelope_header_no_panic...
SUMMARY:
 ** 0 of 39 failed
VERIFICATION:- SUCCESSFUL
Verification Time: 0.16620792s
Complete - 1 successfully verified harnesses, 0 failures, 1 total.
```

**VERIFIED** — 0 of 39 checks failed. No panic is reachable for any input.

### Proof 2: `validate_envelope_header_version_valid`

If the parser returns `Some`, the version byte is always `ENVELOPE_VERSION_V1` (0x01) or
`ENVELOPE_VERSION_V2` (0x02). No other version byte can sneak through.

```bash
cargo kani -p mz-persist --harness validate_envelope_header_version_valid 2>&1 | grep -E '(Checking harness|SUMMARY|failed|VERIFICATION|Verification Time|Complete)'
```

```output
Checking harness crypto::kani_proofs::validate_envelope_header_version_valid...
	 - Description: "assertion failed: version == ENVELOPE_VERSION_V1 || version == ENVELOPE_VERSION_V2"
SUMMARY:
 ** 0 of 41 failed
VERIFICATION:- SUCCESSFUL
Verification Time: 0.17219087s
Complete - 1 successfully verified harnesses, 0 failures, 1 total.
```

**VERIFIED** — 0 of 41 checks failed. The assertion `version == V1 || version == V2` holds universally.

### Proof 3: `validate_envelope_header_slice_bounds`

If the parser returns `Some((version, wrapped_end))`, then:
- `wrapped_end` is within the input bounds
- The remaining bytes (nonce + ciphertext + tag) are at least `NONCE_LEN + GCM_TAG_LEN` (28) bytes
- The header, wrapped DEK, and payload exactly partition the input with no gaps or overlaps

```bash
cargo kani -p mz-persist --harness validate_envelope_header_slice_bounds 2>&1 | grep -E '(Checking harness|SUMMARY|failed|VERIFICATION|Verification Time|Complete)'
```

```output
Checking harness crypto::kani_proofs::validate_envelope_header_slice_bounds...
	 - Description: "assertion failed: wrapped_end <= len"
	 - Description: "assertion failed: wrapped_end >= min_header"
	 - Description: "assertion failed: nonce_ct_len >= NONCE_LEN + GCM_TAG_LEN"
	 - Description: "assertion failed: min_header + wrapped_dek_len + nonce_ct_len == len"
SUMMARY:
 ** 0 of 50 failed
VERIFICATION:- SUCCESSFUL
Verification Time: 0.26660234s
Complete - 1 successfully verified harnesses, 0 failures, 1 total.
```

**VERIFIED** — 0 of 50 checks failed. All 4 assertions hold for every possible input:
bounds, minimum size, and exact partitioning.

### Proof 4: `envelope_header_roundtrip`

Constructs an envelope with a symbolic version byte (`V1` or `V2`), a symbolic wrapped
DEK (0–4 bytes), and a fixed nonce+ciphertext region, then verifies that
`validate_envelope_header` recovers the original version and correct `wrapped_end` offset.
This proves the format is self-consistent: what you write is what you read back.

```bash
cargo kani -p mz-persist --harness envelope_header_roundtrip 2>&1 | grep -E '(Checking harness|SUMMARY|failed|VERIFICATION|Verification Time|Complete)'
```

```output
Checking harness crypto::kani_proofs::envelope_header_roundtrip...
aborting path on assume(false) at file /Users/runner/.rustup/toolchains/nightly-2025-11-21-aarch64-apple-darwin/lib/rustlib/src/rust/library/core/src/option.rs line 2175 column 5 function std::option::unwrap_failed thread 0
Check 6: std::option::unwrap_failed.assertion.1
	 - Location: ../../../../runner/.rustup/toolchains/nightly-2025-11-21-aarch64-apple-darwin/lib/rustlib/src/rust/library/core/src/option.rs:2175:5 in function std::option::unwrap_failed
	 - Description: "assertion failed: parsed_ver == version"
	 - Description: "assertion failed: parsed_wrapped_end == min_header + wrapped_len"
	 - Description: "assertion failed: parsed_wrapped.len() == wrapped_len"
SUMMARY:
 ** 0 of 62 failed
VERIFICATION:- SUCCESSFUL
Verification Time: 0.2979168s
Complete - 1 successfully verified harnesses, 0 failures, 1 total.
```

**VERIFIED** — 0 of 62 checks failed. The roundtrip property holds: constructing and
parsing an envelope always recovers the original version, wrapped DEK offset, and
wrapped DEK content.

### Proof 5: `version_byte_written_correctly`

Verifies that writing a version byte (V1 or V2) as `buf[0]` and then calling
`validate_envelope_header` on the resulting envelope recovers that same version byte.

```bash
cargo kani -p mz-persist --harness version_byte_written_correctly 2>&1 | grep -E '(Checking harness|SUMMARY|failed|VERIFICATION|Verification Time|Complete)'
```

```output
Checking harness crypto::kani_proofs::version_byte_written_correctly...
aborting path on assume(false) at file /Users/runner/.rustup/toolchains/nightly-2025-11-21-aarch64-apple-darwin/lib/rustlib/src/rust/library/core/src/option.rs line 2175 column 5 function std::option::unwrap_failed thread 0
Check 5: std::option::unwrap_failed.assertion.1
	 - Location: ../../../../runner/.rustup/toolchains/nightly-2025-11-21-aarch64-apple-darwin/lib/rustlib/src/rust/library/core/src/option.rs:2175:5 in function std::option::unwrap_failed
	 - Description: "assertion failed: buf[0] == version"
	 - Description: "assertion failed: parsed_ver == version"
SUMMARY:
 ** 0 of 56 failed
VERIFICATION:- SUCCESSFUL
Verification Time: 0.14042233s
Complete - 1 successfully verified harnesses, 0 failures, 1 total.
```

**VERIFIED** — 0 of 56 checks failed.

### Proof 6: `parse_envelope_error_path_no_panic`

Proves that the error-path indexing in `parse_envelope`'s `None` branch — which re-checks
`data[0]` for diagnostic error messages — never panics on any input. This closes the gap
between the proven `validate_envelope_header` and the `anyhow`-wrapping `parse_envelope`.

```bash
cargo kani -p mz-persist --harness parse_envelope_error_path_no_panic 2>&1 | grep -E '(Checking harness|SUMMARY|failed|VERIFICATION|Verification Time|Complete)'
```

```output
Checking harness crypto::kani_proofs::parse_envelope_error_path_no_panic...
SUMMARY:
 ** 0 of 55 failed
VERIFICATION:- SUCCESSFUL
Verification Time: 0.19498241s
Complete - 1 successfully verified harnesses, 0 failures, 1 total.
```

**VERIFIED** — 0 of 55 checks failed. The `parse_envelope` error path is panic-free for all inputs.

### Proof 7: `write_header_matches_parse_header`

Proves that the header layout produced by `write_envelope_header` (the function used by
`encrypt_with_dek_versioned`) is correctly parsed back by `validate_envelope_header`.
This closes the format-consistency gap between the encrypt and parse paths without
needing to model AEAD.

```bash
cargo kani -p mz-persist --harness write_header_matches_parse_header 2>&1 | grep -E '(Checking harness|SUMMARY|failed|VERIFICATION|Verification Time|Complete)'
```

```output
Checking harness crypto::kani_proofs::write_header_matches_parse_header...
	 - Description: "assertion failed: result.is_some()"
	 - Description: "assertion failed: parsed_ver == version"
	 - Description: "assertion failed: parsed_wrapped_end == min_header + wrapped_len"
	 - Description: "assertion failed: buf[min_header + j] == wrapped_dek[j]"
SUMMARY:
 ** 0 of 75 failed
VERIFICATION:- SUCCESSFUL
Verification Time: 0.31899443s
Complete - 1 successfully verified harnesses, 0 failures, 1 total.
```

**VERIFIED** — 0 of 75 checks failed. The write→parse roundtrip holds: headers written by
`write_envelope_header` are always correctly recovered by `validate_envelope_header`.

### Proof 8: `validate_decrypt_input_no_panic`

Proves the decrypt length-check function (`validate_decrypt_input`, the pure-arithmetic
core of `decrypt_with_key`) never panics on any input up to 36 bytes.

```bash
cargo kani -p mz-persist --harness validate_decrypt_input_no_panic 2>&1 | grep -E '(Checking harness|SUMMARY|failed|VERIFICATION|Verification Time|Complete)'
```

```output
Checking harness crypto::kani_proofs::validate_decrypt_input_no_panic...
SUMMARY:
 ** 0 of 15 failed
VERIFICATION:- SUCCESSFUL
Verification Time: 0.112957165s
Complete - 1 successfully verified harnesses, 0 failures, 1 total.
```

**VERIFIED** — 0 of 15 checks failed.

### Proof 9: `validate_decrypt_input_bounds_sound`

If `validate_decrypt_input` returns `Some(split)`, then: (a) `split ≤ len` (safe for
`split_at`), (b) `split == NONCE_LEN` (exactly 12 bytes for the nonce), and
(c) the remaining ciphertext+tag region is at least `GCM_TAG_LEN` (16) bytes.

```bash
cargo kani -p mz-persist --harness validate_decrypt_input_bounds_sound 2>&1 | grep -E '(Checking harness|SUMMARY|failed|VERIFICATION|Verification Time|Complete)'
```

```output
Checking harness crypto::kani_proofs::validate_decrypt_input_bounds_sound...
	 - Description: "assertion failed: split <= len"
	 - Description: "assertion failed: split == NONCE_LEN"
	 - Description: "assertion failed: ct_len >= GCM_TAG_LEN"
SUMMARY:
 ** 0 of 20 failed
VERIFICATION:- SUCCESSFUL
Verification Time: 0.11346517s
Complete - 1 successfully verified harnesses, 0 failures, 1 total.
```

**VERIFIED** — 0 of 20 checks failed. The decrypt bounds check is provably sufficient.

## Summary

| Proof | Property | Checks | Result | Time |
|-------|----------|--------|--------|------|
| 1. `validate_envelope_header_no_panic` | No panics on any input ≤ 36 bytes | 39 | PASS | 0.17s |
| 2. `validate_envelope_header_version_valid` | Ok ⟹ version ∈ {V1, V2} | 41 | PASS | 0.17s |
| 3. `validate_envelope_header_slice_bounds` | Ok ⟹ offsets are sound & partition input | 50 | PASS | 0.27s |
| 4. `envelope_header_roundtrip` | construct → parse recovers original fields | 62 | PASS | 0.30s |
| 5. `version_byte_written_correctly` | version byte roundtrips correctly | 56 | PASS | 0.14s |
| 6. `parse_envelope_error_path_no_panic` | error-path indexing never panics | 55 | PASS | 0.19s |
| 7. `write_header_matches_parse_header` | write_envelope_header → validate roundtrip | 75 | PASS | 0.32s |
| 8. `validate_decrypt_input_no_panic` | decrypt length check never panics | 15 | PASS | 0.11s |
| 9. `validate_decrypt_input_bounds_sound` | Ok ⟹ split valid, nonce + tag regions sound | 20 | PASS | 0.11s |

**Total: 413 CBMC checks, 0 failures, 9/9 proofs verified.**

Combined with the 19 unit tests and 4 proptest properties (which fuzz the full
encrypt/decrypt path including `aws-lc-rs` AEAD with random inputs), this provides
high confidence that:
- The envelope format is self-consistent (roundtrip — proofs 4, 7, proptest)
- Parsing never panics on any malformed input (proofs 1, 6, proptest)
- Only valid version bytes are accepted (proof 2)
- Slice boundaries are always sound for both parse and decrypt (proofs 3, 8, 9)
- The header layout written by `encrypt` matches what `parse` expects (proof 7)
- The decrypt bounds check is provably sufficient (proofs 8, 9)
- Tampered ciphertext is always detected by AEAD (proptest)
- The `anyhow` error paths in `parse_envelope` are panic-free (proof 6)
