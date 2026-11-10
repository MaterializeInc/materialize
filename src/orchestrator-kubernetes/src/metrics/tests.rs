// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io;

use anyhow::Context;
use kube::error::Error as K8sError;

use super::error_kind;

fn service_error(kind: io::ErrorKind) -> K8sError {
    K8sError::Service(Box::new(io::Error::from(kind)))
}

#[mz_ore::test]
fn error_kind_finds_nested_timeout() {
    assert_eq!(
        error_kind(&service_error(io::ErrorKind::TimedOut)),
        "timeout"
    );

    let wrapped: anyhow::Result<()> =
        Err(service_error(io::ErrorKind::TimedOut)).context("failed to get service");
    assert_eq!(error_kind(wrapped.unwrap_err().as_ref()), "timeout");
}

#[mz_ore::test]
fn error_kind_classifies_kube_errors() {
    assert_eq!(
        error_kind(&service_error(io::ErrorKind::ConnectionReset)),
        "transport"
    );

    let serde_err = serde_json::from_str::<u64>("x").unwrap_err();
    assert_eq!(error_kind(&K8sError::SerdeError(serde_err)), "decode");

    let wrapped: anyhow::Result<()> =
        Err(service_error(io::ErrorKind::ConnectionReset)).context("failed to get service");
    assert_eq!(error_kind(wrapped.unwrap_err().as_ref()), "transport");
}

#[mz_ore::test]
fn error_kind_defaults_to_other() {
    let err = anyhow::anyhow!("internal-http port missing in service spec");
    assert_eq!(error_kind(err.as_ref()), "other");
}
