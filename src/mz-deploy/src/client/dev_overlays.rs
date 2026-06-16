// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Read/write helpers for the `_mz_deploy.tables.dev_overlays` manifest.
//!
//! These are called by `cli::commands::dev` to drop-and-rebuild
//! per-developer overlay databases.

use crate::client::connection::DevOverlaysClient;
use crate::client::errors::ConnectionError;

impl DevOverlaysClient<'_> {
    /// List overlay databases recorded for the given profile + project.
    pub async fn list_overlays(
        &self,
        profile: &str,
        project: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        let rows = self
            .client
            .query(
                "SELECT overlay_db FROM _mz_deploy.tables.dev_overlays \
                 WHERE profile = $1 AND project = $2 \
                 ORDER BY overlay_db",
                &[&profile, &project],
            )
            .await?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    /// Record that an overlay database was created.
    pub async fn insert_overlay(
        &self,
        profile: &str,
        project: &str,
        overlay_db: &str,
    ) -> Result<(), ConnectionError> {
        self.client
            .execute(
                "INSERT INTO _mz_deploy.tables.dev_overlays \
                 (profile, project, overlay_db, created_at) \
                 VALUES ($1, $2, $3, now())",
                &[&profile, &project, &overlay_db],
            )
            .await?;
        Ok(())
    }

    /// Remove all overlay records for a profile + project pair.
    pub async fn delete_overlays(
        &self,
        profile: &str,
        project: &str,
    ) -> Result<(), ConnectionError> {
        self.client
            .execute(
                "DELETE FROM _mz_deploy.tables.dev_overlays \
                 WHERE profile = $1 AND project = $2",
                &[&profile, &project],
            )
            .await?;
        Ok(())
    }
}
