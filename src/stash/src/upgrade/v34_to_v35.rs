// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::upgrade::objects_v35;
use crate::{StashError, Transaction, TypedCollection};

/// Migration to initialize the comments collection (e.g. `COMMENT ON`).
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const COMMENTS_COLLECTION: TypedCollection<objects_v35::CommentKey, objects_v35::CommentValue> =
        TypedCollection::new("comments");

    COMMENTS_COLLECTION.initialize(tx, vec![]).await?;

    Ok(())
}
