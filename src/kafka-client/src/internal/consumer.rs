// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The consumer engine: fetcher, partition queues, offsets and group
//! membership.

/// The state of one consumer, shared by its facade handles.
pub(crate) struct ConsumerInner {}

/// The state of one split partition queue.
pub(crate) struct PartitionQueueInner {}
