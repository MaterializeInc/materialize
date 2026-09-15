// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Request-owned execution of transient compute sinks.

use std::sync::Arc;

use mz_compute_client::protocol::response::{CopyToResponse, SubscribeBatch, SubscribeResponse};
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::LirRelationExpr;
use mz_compute_types::sinks::ComputeSinkConnection;
use mz_controller_types::ReplicaId;
use timely::progress::Antichain;
use tracing::Instrument;

use crate::active_compute_sink::ActiveComputeSink;
use crate::coord::{Coordinator, Message};
use crate::query_client::compute::DataflowResponse;
use crate::{AdapterError, ReadHolds};

impl Coordinator {
    /// Starts execution for a dataflow with one registered SUBSCRIBE or COPY sink.
    ///
    /// Registers task ownership before returning, without waiting for compute
    /// admission. The caller removes the active sink if validation fails. Creation
    /// holds pass to the query client, which retains them through each replica ACK.
    pub(crate) fn start_query_sink(
        &mut self,
        dataflow: DataflowDescription<LirRelationExpr>,
        cluster: ComputeInstanceId,
        target: Option<ReplicaId>,
        creation_holds: ReadHolds,
    ) -> Result<(), AdapterError> {
        let invalid = |message: &str| AdapterError::Internal(message.into());
        let client = Arc::clone(
            self.query_client
                .as_ref()
                .ok_or_else(|| invalid("query sink requires a query client"))?,
        );
        if dataflow.sink_exports.len() != 1 || !dataflow.index_exports.is_empty() {
            return Err(invalid("query sink requires exactly one sink export"));
        }
        let (&id, desc) = dataflow.sink_exports.iter().next().expect("one sink");
        let subscribe = match &desc.connection {
            ComputeSinkConnection::Subscribe(_) => true,
            ComputeSinkConnection::CopyToS3Oneshot(_) => false,
            _ => return Err(invalid("query sink requires SUBSCRIBE or COPY TO")),
        };
        let mut lower = dataflow
            .as_of
            .clone()
            .ok_or_else(|| invalid("query sink requires as_of"))?;
        let sink = self
            .active_compute_sinks
            .get_mut(&id)
            .ok_or_else(|| invalid("query sink must be registered before execution"))?;
        if sink.cluster_id() != cluster
            || matches!(sink, ActiveComputeSink::Subscribe(_)) != subscribe
        {
            return Err(invalid("query sink does not match registered sink"));
        }
        let execution = sink.query_execution_mut();
        if execution.is_some() {
            return Err(invalid("query sink execution already started"));
        }
        let catalog = Arc::clone(&self.catalog);
        let tx = self.internal_cmd_tx.clone();
        *execution = Some(
            mz_ore::task::spawn(
                || "query sink",
                async move {
                    let result: Result<(), AdapterError> = async {
                        let mut dataflows = client
                            .create_dataflow(catalog, cluster, target, dataflow, creation_holds)
                            .await?;
                        while let Some(response) = dataflows.recv().await {
                            let response = response?;
                            if let DataflowResponse::Subscribe(_, SubscribeResponse::Batch(batch)) =
                                &response
                            {
                                lower = batch.upper.clone();
                            }
                            if tx.send(Message::QueryDataflowResponse(response)).is_err() {
                                return Ok(());
                            }
                        }
                        Ok(())
                    }
                    .await;
                    if let Err(error) = result {
                        // Only creation failure or loss of all alternatives reaches
                        // here. Native terminal responses preserve sink bookkeeping
                        // and client error delivery without failing healthy siblings.
                        let response = if subscribe {
                            DataflowResponse::Subscribe(
                                id,
                                SubscribeResponse::Batch(SubscribeBatch {
                                    lower,
                                    upper: Antichain::new(),
                                    updates: Err(subscribe_error(error, target)),
                                }),
                            )
                        } else {
                            DataflowResponse::CopyTo(id, CopyToResponse::Error(error.to_string()))
                        };
                        let _ = tx.send(Message::QueryDataflowResponse(response));
                    }
                }
                .instrument(tracing::Span::current()),
            )
            .abort_on_drop(),
        );
        Ok(())
    }
}

fn subscribe_error(error: AdapterError, target: Option<ReplicaId>) -> String {
    if crate::query_client::is_target_replica_failure(&error, target) {
        mz_compute_client::controller::error::ERROR_TARGET_REPLICA_FAILED.into()
    } else {
        error.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_client::compute::QueryError;

    #[mz_ore::test]
    fn targeted_subscribe_only_relabels_connection_failures() {
        let target = Some(ReplicaId::User(1));
        let disconnected =
            || AdapterError::Unstructured(QueryError::Disconnected("removed".into()).into());
        assert_eq!(
            subscribe_error(disconnected(), target),
            mz_compute_client::controller::error::ERROR_TARGET_REPLICA_FAILED
        );
        assert_eq!(
            subscribe_error(disconnected(), None),
            disconnected().to_string()
        );
        for error in [
            AdapterError::Unstructured(QueryError::Rejected("admission failed".into()).into()),
            AdapterError::Unstructured(anyhow::anyhow!("execution failed")),
            AdapterError::CollectionUnreadable { id: "u1".into() },
        ] {
            let expected = error.to_string();
            assert_eq!(subscribe_error(error, target), expected);
        }
    }
}
