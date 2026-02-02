// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

pub mod controller;
pub mod k8s;
pub mod metrics;
pub mod tls;
pub mod webhook;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    Anyhow(#[from] anyhow::Error),
    Kube(#[from] kube::Error),
    Reqwest(#[from] reqwest::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Anyhow(e) => write!(f, "{e}"),
            Self::Kube(e) => write!(f, "{e}"),
            Self::Reqwest(e) => write!(f, "{e}"),
        }
    }
}

/// Extracts the tag from an OCI image reference, correctly ignoring
/// registry-host ports (`gcr.io:443/...`) and `@sha256:` digests.
pub fn parse_image_tag(image_ref: &str) -> Option<&str> {
    let before_digest = image_ref.split('@').next().unwrap_or(image_ref);
    let name_part = before_digest
        .rsplit_once('/')
        .map_or(before_digest, |(_, n)| n);
    name_part.rsplit_once(':').map(|(_, tag)| tag)
}

pub fn matching_image_from_environmentd_image_ref(
    environmentd_image_ref: &str,
    image_name: &str,
    image_tag: Option<&str>,
) -> String {
    let namespace = environmentd_image_ref
        .rsplit_once('/')
        .unwrap_or(("materialize", ""))
        .0;
    let tag = image_tag
        .or_else(|| parse_image_tag(environmentd_image_ref))
        .unwrap_or("latest");
    format!("{namespace}/{image_name}:{tag}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_parse_image_tag() {
        for (input, expected) in [
            ("materialize/environmentd:v0.27.0", Some("v0.27.0")),
            ("materialize/environmentd", None),
            (
                "gcr.io:443/materialize/environmentd:v0.27.0",
                Some("v0.27.0"),
            ),
            ("gcr.io:443/materialize/environmentd", None),
            ("pkg.dev/proj/repo/env@sha256:deadbeef", None),
            ("pkg.dev/proj/repo/env:v1.0@sha256:deadbeef", Some("v1.0")),
            ("environmentd:latest", Some("latest")),
            ("environmentd", None),
        ] {
            assert_eq!(parse_image_tag(input), expected, "input: {input}");
        }
    }

    #[mz_ore::test]
    fn test_matching_image() {
        let f = matching_image_from_environmentd_image_ref;
        assert_eq!(
            f("materialize/environmentd:v0.27.0", "console", None),
            "materialize/console:v0.27.0"
        );
        assert_eq!(
            f(
                "materialize/environmentd:v0.27.0",
                "console",
                Some("custom")
            ),
            "materialize/console:custom"
        );
        assert_eq!(
            f("gcr.io:443/materialize/environmentd", "clusterd", None),
            "gcr.io:443/materialize/clusterd:latest"
        );
        assert_eq!(
            f("pkg.dev/proj/repo/env@sha256:deadbeef", "console", None),
            "pkg.dev/proj/repo/console:latest"
        );
        assert_eq!(
            f("environmentd", "console", None),
            "materialize/console:latest"
        );
    }
}
