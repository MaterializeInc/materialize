// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_sql_parser::ast::{ExternalReferences, UnresolvedItemName};

use crate::plan::PlanError;
use crate::pure::{PurifiedSourceExport, RetrievedSourceReferences, SourceReferencePolicy};

// BTreeMap<UnresolvedItemName, PurifiedSourceExport>

pub(super) async fn purify_source_exports(
    client: &mut mz_sql_server_util::Client,
    retrieved_references: &RetrievedSourceReferences,
    requested_references: &Option<ExternalReferences>,
    text_columns: &[UnresolvedItemName],
    exclude_columns: &[UnresolvedItemName],
    reference_policy: &SourceReferencePolicy,
) -> Result<BTreeMap<UnresolvedItemName, PurifiedSourceExport>, PlanError> {
    Ok(BTreeMap::default())
}
