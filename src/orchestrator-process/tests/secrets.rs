// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for file system backed secrets management.

use mz_orchestrator_process::{ProcessOrchestrator, ProcessOrchestratorConfig};
use mz_repr::CatalogItemId;
use mz_secrets::SecretsController;

async fn test_orchestrator(secrets_dir: std::path::PathBuf) -> ProcessOrchestrator {
    ProcessOrchestrator::new(ProcessOrchestratorConfig {
        image_dir: secrets_dir.clone(),
        suppress_output: true,
        environment_id: "process-secrets-test".into(),
        scratch_directory: secrets_dir.join("scratch"),
        secrets_dir,
        command_wrapper: vec![],
        propagate_crashes: false,
        tcp_proxy: None,
    })
    .await
    .expect("creating orchestrator")
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // uses the file system
async fn test_secrets_roundtrip_and_listing() {
    let tempdir = tempfile::tempdir().expect("creating tempdir");
    let orchestrator = test_orchestrator(tempdir.path().join("secrets")).await;

    let user_id = CatalogItemId::User(1);
    orchestrator
        .ensure(user_id, b"user contents")
        .await
        .unwrap();
    orchestrator
        .ensure_internal("ctp-ca", b"internal contents")
        .await
        .unwrap();

    // Internal secrets neither appear in nor break the user secret listing.
    assert_eq!(orchestrator.list().await.unwrap(), vec![user_id]);

    // Both kinds of secrets are readable through the reader.
    let reader = orchestrator.reader();
    assert_eq!(reader.read(user_id).await.unwrap(), b"user contents");
    assert_eq!(
        reader.read_internal("ctp-ca").await.unwrap(),
        b"internal contents"
    );

    // Updates overwrite.
    orchestrator
        .ensure_internal("ctp-ca", b"rotated")
        .await
        .unwrap();
    assert_eq!(reader.read_internal("ctp-ca").await.unwrap(), b"rotated");

    // Deletion is idempotent, and a deleted secret is unreadable.
    orchestrator.delete_internal("ctp-ca").await.unwrap();
    orchestrator.delete_internal("ctp-ca").await.unwrap();
    assert!(reader.read_internal("ctp-ca").await.is_err());

    // Invalid internal names are rejected, in particular names that could escape the secrets
    // directory.
    for invalid in ["", "Bad_Name", "../escape", "a/b"] {
        assert!(
            orchestrator.ensure_internal(invalid, b"x").await.is_err(),
            "name {invalid:?} must be rejected"
        );
    }
}
