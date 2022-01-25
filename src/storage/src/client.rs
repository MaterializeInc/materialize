// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// A client to the storage service.
pub struct Client {

}

impl Client {
    /// Creates a source.
    ///
    /// Returns an identifier for the resulting definite collection.
    pub fn create_source(def: SourceDesc) -> Id {
        todo!()
    }

    /// Subscribes to a stream of updates for the specified collection...
    pub fn subscribe(id: Id) {
        todo!()
    }
}

/// Describes a source.
pub struct SourceDesc {

}

/// Identifies a definite collection.
///
/// An `Id` is unique for the lifetime of a [`Server`](crate::server::Server).
pub struct Id {

}
