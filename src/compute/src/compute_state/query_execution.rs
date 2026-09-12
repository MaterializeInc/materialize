// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Connection-owned execution, borrowing catalog imports from the worker runtime.

use super::*;
use mz_compute_client::protocol::response::SubscribeBatch;

#[cfg(test)]
mod tests;

/// Source admission is independent of snapshot completion and scheduling.
pub(crate) struct Admission {
    pending_sources: BTreeSet<GlobalId>,
    error: Option<String>,
    imports: Vec<Rc<RefCell<Admission>>>,
}

impl Admission {
    pub(crate) fn observe(&mut self, source: GlobalId, event: Result<(), String>) {
        match event {
            Ok(()) => {
                self.pending_sources.remove(&source);
            }
            Err(error) => {
                self.error.get_or_insert(error);
            }
        }
    }

    fn error(&self) -> Option<String> {
        self.error
            .clone()
            .or_else(|| self.imports.iter().find_map(|i| i.borrow().error()))
    }

    fn ready(&self) -> bool {
        self.pending_sources.is_empty()
            && self.error().is_none()
            && self.imports.iter().all(|import| import.borrow().ready())
    }
}

struct QueryDataflow {
    admission: Rc<RefCell<Admission>>,
    exports: BTreeSet<GlobalId>,
    subscribes: BTreeSet<GlobalId>,
    copies: BTreeSet<GlobalId>,
    acknowledged: bool,
    failed: bool,
}

#[derive(Default)]
pub(super) struct QueryState {
    collections: BTreeMap<GlobalId, CollectionState>,
    traces: BTreeMap<GlobalId, TraceBundle>,
    suspended: BTreeMap<GlobalId, Rc<dyn Any>>,
    subscribes: Rc<RefCell<Vec<(GlobalId, SubscribeResponse)>>>,
    copies: Rc<RefCell<Vec<(GlobalId, CopyToResponse)>>>,
    queued: VecDeque<IndexPeek>,
    pending: VecDeque<PendingPeek>,
    flows: BTreeMap<Uuid, QueryDataflow>,
    max_result_size: u64,
}

/// Exchange only transient entries. Catalog collections and their import probes remain
/// worker-owned. No public ID remapping or allocator coordination is involved.
fn exchange_transients<T>(shared: &mut BTreeMap<GlobalId, T>, local: &mut BTreeMap<GlobalId, T>) {
    let ids: Vec<_> = shared
        .keys()
        .filter(|id| id.is_transient())
        .copied()
        .collect();
    let saved = ids
        .into_iter()
        .map(|id| (id, shared.remove(&id).unwrap()))
        .collect();
    shared.append(local);
    *local = saved;
}

impl QueryState {
    fn exchange(&mut self, state: &mut ComputeState) {
        exchange_transients(&mut state.collections, &mut self.collections);
        exchange_transients(&mut state.traces.traces, &mut self.traces);
        exchange_transients(&mut state.suspended_collections, &mut self.suspended);
        std::mem::swap(&mut state.subscribe_response_buffer, &mut self.subscribes);
        std::mem::swap(&mut state.copy_to_response_buffer, &mut self.copies);
        std::mem::swap(&mut state.queued_peeks, &mut self.queued);
        std::mem::swap(&mut state.pending_peeks, &mut self.pending);
    }

    fn admission(&self, id: GlobalId) -> Option<Rc<RefCell<Admission>>> {
        self.flows
            .values()
            .find(|f| f.exports.contains(&id))
            .map(|f| Rc::clone(&f.admission))
    }
}

impl ComputeState {
    /// `None` releases this connection's execution state without touching catalog state.
    pub(crate) fn handle_query_command(
        &mut self,
        worker: &mut TimelyWorker,
        command: Option<ComputeCommand>,
        nonce: Uuid,
        response_sender: &mut ResponseSender,
    ) {
        if let Some(ComputeCommand::HelloQuery { nonce: hello_nonce }) = &command {
            if *hello_nonce != nonce || self.queries.contains_key(&nonce) {
                return;
            }
            self.queries.insert(nonce, QueryState::default());
            let _ = response_sender.send_query(nonce, ComputeResponse::QueryReady);
            self.report_query_catalog_frontiers(nonce, response_sender);
            return;
        }
        let Some(mut query) = self.queries.remove(&nonce) else {
            return;
        };
        let disconnected = command.is_none();
        self.in_query(
            worker,
            response_sender,
            nonce,
            &mut query,
            |active, query| {
                match command {
                    None => {
                        active.compute_state.queued_peeks.clear();
                        active.compute_state.pending_peeks.clear();
                        let ids: Vec<_> = active
                            .compute_state
                            .collections
                            .keys()
                            .filter(|id| id.is_transient())
                            .copied()
                            .collect();
                        for id in ids {
                            active.drop_collection(id);
                        }
                    }
                    Some(ComputeCommand::SetQueryMaxResultSize { max_result_size }) => {
                        query.max_result_size = max_result_size;
                    }
                    Some(ComputeCommand::CreateQueryDataflow {
                        request_id,
                        dataflow,
                    }) => {
                        active.create_query_dataflow(query, request_id, *dataflow);
                    }
                    Some(ComputeCommand::Peek(peek)) => {
                        let error = match &peek.target {
                            PeekTarget::Index { id } => active.validate_query_read(
                                query,
                                *id,
                                &Antichain::from_elem(peek.timestamp),
                            ),
                            PeekTarget::Persist { .. } => Ok(()),
                        };
                        if let Err(error) = error {
                            active.send_compute_response(ComputeResponse::PeekResponse(
                                peek.uuid,
                                PeekResponse::Error(PeekError::unstructured(error)),
                                peek.otel_ctx.clone(),
                            ));
                        } else if !active
                            .compute_state
                            .queued_peeks
                            .iter()
                            .any(|p| p.peek.uuid == peek.uuid)
                            && !active
                                .compute_state
                                .pending_peeks
                                .iter()
                                .any(|p| p.peek().uuid == peek.uuid)
                        {
                            active.handle_peek(*peek);
                        }
                    }
                    Some(ComputeCommand::CancelPeek { uuid }) => active.handle_cancel_peek(uuid),
                    Some(ComputeCommand::Schedule(id)) if id.is_transient() => {
                        if query.admission(id).is_some_and(|a| a.borrow().ready()) {
                            active.handle_schedule(id);
                        }
                    }
                    Some(ComputeCommand::AllowCompaction { id, frontier }) if id.is_transient() => {
                        if frontier.is_empty() {
                            for flow in query.flows.values_mut() {
                                if !flow.acknowledged && flow.exports.contains(&id) {
                                    // Dropping an export can cancel the source's reader acquisition,
                                    // so its callback cannot be relied on to resolve creation.
                                    flow.admission.borrow_mut().error.get_or_insert_with(|| {
                                        format!("query export {id} dropped before admission")
                                    });
                                }
                            }
                        }
                        if active.compute_state.collections.contains_key(&id) {
                            active.handle_allow_compaction(id, frontier);
                        }
                        active.poll_query_dataflows(query);
                    }
                    // Query connections never configure the worker or authorize external writes.
                    Some(_) => (),
                }
            },
        );
        if !disconnected {
            self.queries.insert(nonce, query);
        }
    }

    fn in_query(
        &mut self,
        worker: &mut TimelyWorker,
        response_sender: &mut ResponseSender,
        nonce: Uuid,
        query: &mut QueryState,
        action: impl FnOnce(&mut ActiveComputeState<'_>, &mut QueryState),
    ) {
        query.exchange(self);
        // Collection logs have no connection namespace. Keep query-private IDs out of
        // lifecycle logging, while retaining the worker's Timely logger and CPU budget.
        let logger = self.compute_logger.take();
        let max_result_size = self.max_result_size;
        self.max_result_size = max_result_size.min(query.max_result_size);
        self.active_query = Some(nonce);
        let passed_over = self.peek_passed_over;
        action(
            &mut ActiveComputeState {
                timely_worker: worker,
                compute_state: self,
                response_tx: response_sender,
            },
            query,
        );
        self.peek_passed_over |= passed_over;
        self.active_query = None;
        self.max_result_size = max_result_size;
        self.compute_logger = logger;
        query.exchange(self);
    }

    /// Poll after the lifecycle peek sweep, which starts the shared worker CPU budget.
    pub(crate) fn poll_query_commands(
        &mut self,
        worker: &mut TimelyWorker,
        response_sender: &mut ResponseSender,
    ) {
        let mut nonces: Vec<_> = self.queries.keys().copied().collect();
        if let Some(last) = self.last_query_served {
            let next = nonces.partition_point(|nonce| *nonce <= last);
            nonces.rotate_left(next);
        }
        // Only queued peeks draw worker CPU budget. Prefer them over idle scopes,
        // retaining round-robin order within each group and one budget for the sweep.
        nonces.sort_by_key(|nonce| self.queries[nonce].queued.is_empty());
        if let Some(first) = nonces
            .first()
            .filter(|n| !self.queries[n].queued.is_empty())
        {
            self.last_query_served = Some(*first);
        }
        for nonce in nonces {
            let mut query = self.queries.remove(&nonce).unwrap();
            let mut upper = Antichain::new();
            for trace in query.traces.values_mut() {
                trace.oks_mut().read_upper(&mut upper);
                trace.oks_mut().set_physical_compaction(upper.borrow());
                trace.errs_mut().read_upper(&mut upper);
                trace.errs_mut().set_physical_compaction(upper.borrow());
            }
            self.in_query(
                worker,
                response_sender,
                nonce,
                &mut query,
                |active, query| {
                    active.poll_query_dataflows(query);
                    let failed: BTreeSet<_> = query
                        .flows
                        .values()
                        .filter(|flow| flow.failed)
                        .flat_map(|flow| flow.exports.iter().copied())
                        .collect();
                    active
                        .compute_state
                        .subscribe_response_buffer
                        .borrow_mut()
                        .retain(|(id, _)| !failed.contains(id));
                    active
                        .compute_state
                        .copy_to_response_buffer
                        .borrow_mut()
                        .retain(|(id, _)| !failed.contains(id));
                    let waiting: BTreeSet<_> = query
                        .flows
                        .values()
                        .filter(|flow| !flow.acknowledged)
                        .flat_map(|flow| flow.exports.iter().copied())
                        .collect();
                    let mut delayed_subscribes = Vec::new();
                    active
                        .compute_state
                        .subscribe_response_buffer
                        .borrow_mut()
                        .retain(|(id, response)| {
                            if waiting.contains(id) {
                                delayed_subscribes.push((*id, response.clone()));
                                false
                            } else {
                                true
                            }
                        });
                    let mut delayed_copies = Vec::new();
                    active
                        .compute_state
                        .copy_to_response_buffer
                        .borrow_mut()
                        .retain(|(id, response)| {
                            if waiting.contains(id) {
                                delayed_copies.push((*id, response.clone()));
                                false
                            } else {
                                true
                            }
                        });
                    active.process_peeks();
                    active.process_subscribes();
                    for (id, _) in active.compute_state.copy_to_response_buffer.borrow().iter() {
                        for flow in query.flows.values_mut() {
                            flow.copies.remove(id);
                        }
                    }
                    active.process_copy_tos();
                    active
                        .compute_state
                        .subscribe_response_buffer
                        .borrow_mut()
                        .extend(delayed_subscribes);
                    active
                        .compute_state
                        .copy_to_response_buffer
                        .borrow_mut()
                        .extend(delayed_copies);
                    active.report_frontiers();
                },
            );
            self.queries.insert(nonce, query);
        }
        self.retiring_dataflows.retain(|index, token| {
            if token.strong_count() == 0 {
                worker.drop_dataflow(*index);
                false
            } else {
                true
            }
        });
    }

    fn report_query_catalog_frontiers(&mut self, nonce: Uuid, sender: &ResponseSender) {
        let mut frontiers = BTreeMap::new();
        for (&id, collection) in &self.collections {
            if !(id.is_user() || id.is_system()) || collection.is_subscribe_or_copy {
                continue;
            }
            let mut write = Antichain::new();
            let mut read = Antichain::new();
            if let Some(trace) = self.traces.get_mut(&id) {
                read = collection.read_frontier(trace);
                trace.oks_mut().read_upper(&mut write);
            } else if let Some(frontier) = &collection.sink_write_frontier {
                write.clone_from(&frontier.borrow());
            } else {
                continue;
            }
            let mut output = write.clone();
            if let Some(probe) = &collection.compute_probe {
                if *collection.read_only_rx.borrow() {
                    output.clear();
                }
                probe.with_frontier(|f| output.extend(f.iter().copied()));
            }
            let mut input = Antichain::new();
            for probe in collection.input_probes.values() {
                probe.with_frontier(|f| input.extend(f.iter().copied()));
            }
            frontiers.insert(
                id,
                FrontiersResponse {
                    write_frontier: Some(write),
                    input_frontier: Some(input),
                    output_frontier: Some(output),
                    read_frontier: Some(read),
                },
            );
        }
        for (id, frontier) in frontiers {
            let _ = sender.send_query(nonce, ComputeResponse::Frontiers(id, frontier));
        }
    }
}

impl ActiveComputeState<'_> {
    fn validate_query_read(
        &mut self,
        query: &QueryState,
        id: GlobalId,
        as_of: &Antichain<Timestamp>,
    ) -> Result<(), String> {
        if let Some(admission) = query.admission(id) {
            let admission = admission.borrow();
            if let Some(error) = admission.error() {
                return Err(error);
            }
            if !admission.ready() {
                return Err(format!("query collection {id} is not admitted"));
            }
        }
        self.validate_query_import(id, as_of)
    }

    fn validate_query_import(
        &mut self,
        id: GlobalId,
        as_of: &Antichain<Timestamp>,
    ) -> Result<(), String> {
        if !(id.is_user() || id.is_system() || id.is_transient()) {
            return Err(format!("invalid query import {id}"));
        }
        if self
            .compute_state
            .collections
            .get(&id)
            .is_some_and(|collection| !PartialOrder::less_equal(&collection.as_of, as_of))
        {
            return Err(format!("query read of {id} is before its as_of"));
        }
        let trace = self
            .compute_state
            .traces
            .get_mut(&id)
            .ok_or_else(|| format!("query collection {id} does not exist"))?;
        if !PartialOrder::less_equal(&trace.compaction_frontier(), as_of) {
            return Err(format!("query read of {id} is before its since"));
        }
        Ok(())
    }

    fn create_query_dataflow(
        &mut self,
        query: &mut QueryState,
        request_id: Uuid,
        dataflow: DataflowDescription<RenderPlan, CollectionMetadata>,
    ) {
        let validate = |active: &mut Self| -> Result<(), String> {
            if query.flows.contains_key(&request_id) {
                return Err("duplicate query dataflow request".into());
            }
            let as_of = dataflow
                .as_of
                .as_ref()
                .ok_or("query dataflow requires as_of")?;
            if dataflow.export_ids().next().is_none()
                || !dataflow.export_ids().all(|id| id.is_transient())
            {
                return Err("query dataflow requires transient exports".into());
            }
            if dataflow.sink_exports.len()
                != dataflow.subscribe_ids().count() + dataflow.copy_to_ids().count()
            {
                return Err("query dataflow cannot create maintained sinks".into());
            }
            if dataflow.export_ids().any(|id| {
                active.compute_state.collections.contains_key(&id) || query.admission(id).is_some()
            }) {
                return Err("query export already exists or still has readers".into());
            }
            for id in dataflow.index_imports.keys() {
                // Source-event delivery is asynchronous. Do not let observing poison
                // on one worker change which dataflows get rendered there. Import
                // admission below carries poison to every partition of the consumer.
                active.validate_query_import(*id, as_of)?;
            }
            Ok(())
        };
        if let Err(error) = validate(self) {
            self.send_compute_response(ComputeResponse::QueryDataflowResponse {
                request_id,
                error: Some(error),
            });
            return;
        }
        // Rendering retains importer tokens only for dependencies of actual exports.
        // Unused source operators can disappear without ever acquiring a reader.
        let dependencies: BTreeSet<_> = dataflow
            .index_exports
            .values()
            .flat_map(|(index, _)| dataflow.depends_on(index.on_id))
            .chain(
                dataflow
                    .sink_exports
                    .values()
                    .flat_map(|sink| dataflow.depends_on(sink.from)),
            )
            .collect();
        let pending_sources = dataflow
            .source_imports
            .keys()
            .filter(|id| dependencies.contains(id))
            .copied()
            .collect();
        let admission = Rc::new(RefCell::new(Admission {
            pending_sources,
            error: None,
            imports: dataflow
                .index_imports
                .keys()
                .filter_map(|id| query.admission(*id))
                .collect(),
        }));
        let exports = dataflow.export_ids().collect();
        let subscribes = dataflow.subscribe_ids().collect();
        let copies = dataflow.copy_to_ids().collect();
        self.compute_state.query_admission = Some(Rc::clone(&admission));
        self.handle_create_dataflow(dataflow);
        self.compute_state.query_admission = None;
        query.flows.insert(
            request_id,
            QueryDataflow {
                admission,
                exports,
                subscribes,
                copies,
                acknowledged: false,
                failed: false,
            },
        );
        self.poll_query_dataflows(query);
    }

    fn poll_query_dataflows(&mut self, query: &mut QueryState) {
        let mut rejected = Vec::new();
        for (&request_id, flow) in &mut query.flows {
            let error = flow.admission.borrow().error();
            if !flow.acknowledged && (error.is_some() || flow.admission.borrow().ready()) {
                self.send_compute_response(ComputeResponse::QueryDataflowResponse {
                    request_id,
                    error: error.clone(),
                });
                flow.acknowledged = true;
                if error.is_some() {
                    rejected.extend(flow.exports.iter().copied());
                    flow.failed = true;
                    continue;
                }
            }
            let Some(error) = error else {
                continue;
            };
            if flow.failed {
                continue;
            }
            flow.failed = true;
            let queued = std::mem::take(&mut self.compute_state.queued_peeks);
            for peek in queued {
                if matches!(
                    peek.peek.target,
                    PeekTarget::Index { id } if flow.exports.contains(&id)
                ) {
                    self.send_peek_response(
                        PendingPeek::Index(peek),
                        PeekResponse::Error(PeekError::unstructured(error.clone())),
                    );
                } else {
                    self.compute_state.queued_peeks.push_back(peek);
                }
            }
            let pending = std::mem::take(&mut self.compute_state.pending_peeks);
            for peek in pending {
                if matches!(
                    peek.peek().target,
                    PeekTarget::Index { id } if flow.exports.contains(&id)
                ) {
                    self.send_peek_response(
                        peek,
                        PeekResponse::Error(PeekError::unstructured(error.clone())),
                    );
                } else {
                    self.compute_state.pending_peeks.push_back(peek);
                }
            }
            for &id in &flow.exports {
                if let Some(collection) = self.compute_state.collections.get(&id) {
                    if flow.subscribes.contains(&id)
                        && !collection.reported_frontiers.write_frontier.is_empty()
                    {
                        // Keep the failed flow's source capabilities alive. A failure must not
                        // turn into a completed, empty trace before subsequent reads see poison.
                        self.compute_state
                            .subscribe_response_buffer
                            .borrow_mut()
                            .retain(|(sink, _)| *sink != id);
                        self.compute_state
                            .copy_to_response_buffer
                            .borrow_mut()
                            .retain(|(sink, _)| *sink != id);
                        let lower = match &collection.reported_frontiers.write_frontier {
                            ReportedFrontier::NotReported { lower } => lower.clone(),
                            ReportedFrontier::Reported(frontier) => frontier.clone(),
                        };
                        self.send_compute_response(ComputeResponse::SubscribeResponse(
                            id,
                            SubscribeResponse::Batch(SubscribeBatch {
                                lower,
                                upper: Antichain::new(),
                                updates: Err(error.clone()),
                            }),
                        ));
                    } else if flow.copies.contains(&id) {
                        self.send_compute_response(ComputeResponse::CopyToResponse(
                            id,
                            CopyToResponse::Error(error.clone()),
                        ));
                    }
                }
            }
        }
        for id in rejected {
            if self.compute_state.collections.contains_key(&id) {
                self.drop_collection(id);
            }
            self.compute_state
                .subscribe_response_buffer
                .borrow_mut()
                .retain(|(sink, _)| *sink != id);
            self.compute_state
                .copy_to_response_buffer
                .borrow_mut()
                .retain(|(sink, _)| *sink != id);
        }
        query.flows.retain(|_, flow| {
            !flow.acknowledged
                || flow
                    .exports
                    .iter()
                    .any(|id| self.compute_state.collections.contains_key(id))
                || self.compute_state.queued_peeks.iter().any(|p| {
                    matches!(p.peek.target, PeekTarget::Index { id } if flow.exports.contains(&id))
                })
                || self.compute_state.pending_peeks.iter().any(|p| {
                    matches!(
                        p.peek().target,
                        PeekTarget::Index { id } if flow.exports.contains(&id)
                    )
                })
        });
    }
}
