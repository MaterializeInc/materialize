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

pub fn matching_image_from_environmentd_image_ref(
    environmentd_image_ref: &str,
    image_name: &str,
    image_tag: Option<&str>,
) -> String {
    let namespace = environmentd_image_ref
        .rsplit_once('/')
        .unwrap_or(("materialize", ""))
        .0;
    let tag = image_tag.unwrap_or_else(|| {
        environmentd_image_ref
            .rsplit_once(':')
            .unwrap_or(("", "unstable"))
            .1
    });
    format!("{namespace}/{image_name}:{tag}")
}
