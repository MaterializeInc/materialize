// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cluster state shared by all engines of one client: bootstrap, broker
//! connections, the metadata cache and refresh, leader, coordinator and
//! controller lookup.

/// The state of one client's view of the cluster.
pub(crate) struct Cluster {}
