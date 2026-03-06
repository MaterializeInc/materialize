// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! State tracked for active standing queries.
//!
//! Each standing query has:
//! - A long-lived SUBSCRIBE on the standing query's dataflow.
//! - A batch buffer for pending EXECUTE requests.
//! - A request map from request_id to the channel for returning results.
//! - An in-flight set tracking which request_ids were written at which timestamp.

use std::collections::BTreeMap;

use mz_controller_types::ClusterId;
use mz_repr::{CatalogItemId, GlobalId, Row, Timestamp};
use uuid::Uuid;

use crate::ExecuteContext;

/// State for a single active standing query.
#[derive(Debug)]
pub(crate) struct ActiveStandingQuery {
    /// The CatalogItemId of this standing query.
    pub item_id: CatalogItemId,
    /// The GlobalId of the standing query's output collection.
    pub output_id: GlobalId,
    /// The GlobalId of the parameter storage collection.
    pub param_collection_id: GlobalId,
    /// The cluster on which the standing query's dataflow runs.
    pub cluster_id: ClusterId,
    /// The GlobalId of the SUBSCRIBE sink for this standing query.
    pub subscribe_sink_id: GlobalId,
    /// Pending requests waiting to be flushed (inserted into the param collection).
    pub batch_buffer: Vec<PendingRequest>,
    /// Open requests: request_id → ExecuteContext waiting for results.
    pub request_map: BTreeMap<Uuid, ExecuteContext>,
    /// In-flight requests: write timestamp → list of request_ids written at that timestamp.
    pub in_flight: BTreeMap<Timestamp, Vec<Uuid>>,
    /// Buffered results: request_id → accumulated result rows.
    pub result_buffer: BTreeMap<Uuid, Vec<Row>>,
    /// Param rows for in-flight requests: request_id → param row (for retraction after delivery).
    pub param_rows: BTreeMap<Uuid, Row>,
    /// Param rows for delivered requests, pending retraction from the param collection.
    pub pending_retractions: Vec<Row>,
}

/// A pending EXECUTE request waiting to be batched and flushed.
#[derive(Debug)]
pub(crate) struct PendingRequest {
    /// Unique identifier for this request.
    pub request_id: Uuid,
    /// A single row containing (request_id, param_1, param_2, ...) for insertion
    /// into the parameter collection.
    pub param_row: Row,
}
