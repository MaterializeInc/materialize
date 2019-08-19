// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Types and data structures used to glue the various components of
//! Materialize together.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Various metadata that gets attached to commands at all stages.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandMeta {
    /// The pgwire connection on which this command originated.
    pub connection_uuid: Uuid,
}

impl CommandMeta {
    pub fn nil() -> CommandMeta {
        CommandMeta {
            connection_uuid: Uuid::nil(),
        }
    }
}
