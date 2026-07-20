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

//! Rustls crypto provider selection.
//!
//! Materialize standardizes on the aws-lc-rs crypto provider for all rustls
//! usage. This module installs it as the process-wide default so that rustls
//! configurations built anywhere in the process, including in transitive
//! dependencies, pick it up without selecting a provider explicitly.

use std::sync::Arc;

// Install the provider at startup of any binary that links mz-ore with the
// `crypto` feature. This covers main binaries, test binaries, and build
// scripts, which are separate processes and do not inherit each other's
// provider.
#[ctor::ctor]
fn auto_install_crypto_provider() {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let _ = provider.install_default();
}

/// Returns the aws-lc-rs [`rustls::crypto::CryptoProvider`], for rustls
/// configurations that select a provider explicitly.
pub fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}
