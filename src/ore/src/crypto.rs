// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! FIPS-aware cryptographic provider helpers.
//!
//! This module provides a [`fips_crypto_provider`] function that returns the
//! correct [`rustls::crypto::CryptoProvider`] for the build configuration:
//!
//! - When the `fips` feature is enabled, the provider is backed by
//!   `aws_lc_rs` compiled against the FIPS-validated module.
//! - Otherwise, the default `aws_lc_rs` provider is used.

use std::sync::Arc;

/// Auto-install the crypto provider when any binary links mz-ore with the
/// `crypto` feature. This ensures reqwest (with `rustls-tls-*-no-provider`)
/// can build TLS clients in any context — main binaries, test binaries, and
/// build scripts — without requiring explicit `fips_crypto_provider()` calls.
///
/// In FIPS mode, uses the FIPS-validated aws-lc module. Otherwise, uses the
/// standard aws-lc-rs provider. The two paths link different C libraries
/// (aws-lc-fips-sys vs aws-lc-sys) and must not both be active.
#[ctor::ctor]
fn auto_install_crypto_provider() {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let _ = provider.install_default();
}

/// Returns the [`rustls::crypto::CryptoProvider`] appropriate for the current
/// build.
///
/// - With the `fips` feature: uses the FIPS 140-3 validated aws-lc module.
/// - Without `fips`: uses the standard aws-lc-rs provider.
///
/// On the first call, this also installs the provider as the process-wide
/// default so that any rustls usage (including transitive dependencies like
/// `hyper-rustls` or `tokio-postgres-rustls`) picks it up automatically.
///
/// The returned provider is cached in an `Arc` so cloning is cheap.
pub fn fips_crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    // Both paths use aws_lc_rs::default_provider(), but with the `fips`
    // feature enabled, aws-lc-rs links against aws-lc-fips-sys instead of
    // aws-lc-sys, providing the FIPS-validated cryptographic module.
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let _ = provider.clone().install_default();
    Arc::new(provider)
}
