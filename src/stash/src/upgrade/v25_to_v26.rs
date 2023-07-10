// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// No-op migration for adding `disk` field to Cluster / Replica configurations.
/// All new values are `boolean`s, which default to `false` in protobuf.
pub fn upgrade() {}
