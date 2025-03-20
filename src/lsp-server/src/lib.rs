// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `lsp` is Materialize Language Server Protocol (LSP) implementation
#![warn(missing_docs)]

use mz_build_info::{build_info, BuildInfo};
use std::sync::LazyLock;

/// Build information about the LSP server.
pub const BUILD_INFO: BuildInfo = build_info!();
/// Variable holding the version of LSP server.
pub static PKG_VERSION: LazyLock<String> =
    LazyLock::new(|| BUILD_INFO.semver_version().to_string());
/// Variable holding the name of LSP server package.
pub static PKG_NAME: LazyLock<String> = LazyLock::new(|| env!("CARGO_PKG_NAME").to_string());

/// Contains the structure and implementation of the Language Server Protocol.
pub mod backend;
