// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use guppy::platform::{Platform, PlatformSpec, TargetFeatures, Triple};
use std::sync::LazyLock;

use crate::ToBazelDefinition;

static AARCH64_MAC_OS: LazyLock<PlatformSpec> = LazyLock::new(|| {
    let triple = Triple::new("aarch64-apple-darwin").unwrap();
    PlatformSpec::Platform(Arc::new(Platform::from_triple(triple, TargetFeatures::All)))
});
static X86_64_MAC_OS: LazyLock<PlatformSpec> = LazyLock::new(|| {
    let triple = Triple::new("x86_64-apple-darwin").unwrap();
    PlatformSpec::Platform(Arc::new(Platform::from_triple(triple, TargetFeatures::All)))
});
static AARCH64_LINUX_GNU: LazyLock<PlatformSpec> = LazyLock::new(|| {
    let triple = Triple::new("aarch64-unknown-linux-gnu").unwrap();
    PlatformSpec::Platform(Arc::new(Platform::from_triple(triple, TargetFeatures::All)))
});
static X86_64_LINUX_GNU: LazyLock<PlatformSpec> = LazyLock::new(|| {
    let triple = Triple::new("x86_64-unknown-linux-gnu").unwrap();
    PlatformSpec::Platform(Arc::new(Platform::from_triple(triple, TargetFeatures::All)))
});

/// Defines the various platforms we support building for.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PlatformVariant {
    /// `aarch64-apple-darwin`
    Aarch64MacOS,
    /// `x86_64-apple-darwin`
    X86_64MacOS,
    /// `aarch64-unknown-linux-gnu`
    Aarch64Linux,
    /// `x86_64-unknown-linux-gnu`
    X86_64Linux,
}

impl PlatformVariant {
    const ALL: &'static [PlatformVariant] = &[
        PlatformVariant::Aarch64MacOS,
        PlatformVariant::X86_64MacOS,
        PlatformVariant::Aarch64Linux,
        PlatformVariant::X86_64Linux,
    ];

    pub fn spec(&self) -> &PlatformSpec {
        match self {
            PlatformVariant::Aarch64MacOS => &*AARCH64_MAC_OS,
            PlatformVariant::X86_64MacOS => &*X86_64_MAC_OS,
            PlatformVariant::Aarch64Linux => &*AARCH64_LINUX_GNU,
            PlatformVariant::X86_64Linux => &*X86_64_LINUX_GNU,
        }
    }

    pub fn all() -> &'static [PlatformVariant] {
        Self::ALL
    }
}

impl ToBazelDefinition for PlatformVariant {
    fn format(&self, writer: &mut dyn std::fmt::Write) -> Result<(), std::fmt::Error> {
        use PlatformVariant::*;

        // These names are defined in our own platform spec in `/misc/bazel/platforms`.
        let s = match self {
            Aarch64MacOS => "@//misc/bazel/platforms:macos_arm",
            X86_64MacOS => "@//misc/bazel/platforms:macos_x86_64",
            Aarch64Linux => "@//misc/bazel/platforms:linux_arm",
            X86_64Linux => "@//misc/bazel/platforms:linux_x86_64",
        };

        write!(writer, "\"{s}\"")
    }
}
