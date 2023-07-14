// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use mz_ore::str::{separated, StrExt};
use mz_repr::ColumnName;

use crate::catalog::ObjectType;

/// Notices that can occur in the adapter layer.
///
/// These are diagnostic warnings or informational messages that are not
/// severe enough to warrant failing a query entirely.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanNotice {
    ObjectDoesNotExist {
        name: String,
        object_type: ObjectType,
    },
    UpsertSinkKeyNotEnforced {
        key: Vec<ColumnName>,
        name: String,
    },
}

impl PlanNotice {
    /// Reports additional details about the notice, if any are available.
    pub fn detail(&self) -> Option<String> {
        match self {
            PlanNotice::UpsertSinkKeyNotEnforced { key, name } => {
                let details = format!(
                    "Materialize did not validate that the specified upsert envelope key ({}) \
                    was a unique key of the underlying relation {}. If this key is not unique, \
                    the sink might produce multiple updates for the same key at the same time, \
                    which may confuse downstream consumers.",
                    separated(", ", key.iter().map(|c| c.as_str().quoted())),
                    name.quoted()
                );
                Some(details)
            }
            _ => None,
        }
    }

    /// Reports a hint for the user about how the notice could be addressed.
    pub fn hint(&self) -> Option<String> {
        match self {
            PlanNotice::UpsertSinkKeyNotEnforced { .. } => {
                Some("See: https://materialize.com/s/sink-key-selection".into())
            }
            _ => None,
        }
    }
}

impl fmt::Display for PlanNotice {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PlanNotice::ObjectDoesNotExist { name, object_type } => {
                write!(
                    f,
                    "{} {} does not exist, skipping",
                    object_type,
                    name.quoted()
                )
            }
            PlanNotice::UpsertSinkKeyNotEnforced { .. } => {
                write!(f, "upsert key not validated to be unique")
            }
        }
    }
}
