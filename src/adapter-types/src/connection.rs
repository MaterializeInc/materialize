// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::id_gen::{IdAllocatorInnerBitSet, IdHandle};

/// Inner type of a [`ConnectionId`], `u32` for postgres compatibility.
///
/// Note: Generally you should not use this type directly, and instead use [`ConnectionId`].
pub type ConnectionIdType = u32;

/// An abstraction allowing us to name different connections.
pub type ConnectionId = IdHandle<ConnectionIdType, IdAllocatorInnerBitSet>;
