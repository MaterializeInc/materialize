// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Traits and types for capturing and replaying collections of data.
use std::any::Any;
use std::cell::RefCell;
use std::marker::{Send, Sync};
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::Collection;
use timely::dataflow::Scope;

use mz_dataflow_types::{DataflowError, SourceInstanceRequest};
use mz_repr::{Diff, GlobalId, Row};

/// A type that can capture a specific source.
pub trait StorageCapture {
    /// Captures the source and binds to `id`.
    fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: GlobalId,
        ok: Collection<G, Row, Diff>,
        err: Collection<G, DataflowError, Diff>,
        token: Arc<dyn Any + Send + Sync>,
        name: &str,
        dataflow_id: uuid::Uuid,
    );
}

/// A type that can replay specific sources
pub trait ComputeReplay {
    /// Replays the source bound to `id`.
    ///
    /// `None` is returned if the source does not exist, either because
    /// it was not there to begin, or has already been replayed. Either
    /// case is likely an error.
    fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        scope: &mut G,
        name: &str,
        request: SourceInstanceRequest,
    ) -> (
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    );
}

impl<SC: StorageCapture> StorageCapture for Rc<RefCell<SC>> {
    fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: GlobalId,
        ok: Collection<G, Row, Diff>,
        err: Collection<G, DataflowError, Diff>,
        token: Arc<dyn Any + Send + Sync>,
        name: &str,
        dataflow_id: uuid::Uuid,
    ) {
        self.borrow_mut()
            .capture(id, ok, err, token, name, dataflow_id)
    }
}

impl<CR: ComputeReplay> ComputeReplay for Rc<RefCell<CR>> {
    fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        scope: &mut G,
        name: &str,
        request: SourceInstanceRequest,
    ) -> (
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    ) {
        self.borrow_mut().replay(scope, name, request)
    }
}

/// A boundary implementation that panics on use.
pub struct DummyBoundary;

impl ComputeReplay for DummyBoundary {
    fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        _scope: &mut G,
        _name: &str,
        _request: SourceInstanceRequest,
    ) -> (
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    ) {
        panic!("DummyBoundary cannot replay")
    }
}

impl StorageCapture for DummyBoundary {
    fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        _id: GlobalId,
        _ok: Collection<G, Row, Diff>,
        _err: Collection<G, DataflowError, Diff>,
        _token: Arc<dyn Any + Send + Sync>,
        _name: &str,
        _dataflow_id: uuid::Uuid,
    ) {
        panic!("DummyBoundary cannot capture")
    }
}

pub use boundary_hook::BoundaryHook;
mod boundary_hook {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::iter::once;

    use async_trait::async_trait;

    use mz_dataflow_types::client::controller::storage::CollectionMetadata;
    use mz_dataflow_types::client::{
        GenericClient, RenderSourcesCommand, StorageCommand, StorageResponse,
    };
    use mz_dataflow_types::sources::SourceDesc;
    use mz_dataflow_types::{SourceInstanceDesc, SourceInstanceId, SourceInstanceRequest};
    use mz_repr::GlobalId;

    /// A client wrapper that observes source instantiation requests and enqueues them as commands.
    #[derive(Debug)]
    pub struct BoundaryHook<C, T = mz_repr::Timestamp> {
        /// The wrapped client,
        client: C,
        /// Source creation requests to suppress.
        suppress: BTreeMap<SourceInstanceId, u64>,
        /// Enqueue source rendering requests.
        requests: tokio::sync::mpsc::UnboundedReceiver<SourceInstanceRequest<T>>,
        /// The number of storage workers, of whom requests will be made.
        workers: u64,
        /// Created sources so that we can form render requests.
        sources: BTreeMap<GlobalId, (SourceDesc, CollectionMetadata)>,
        /// Pending render requests, awaiting source creation.
        pending: BTreeMap<GlobalId, Vec<SourceInstanceRequest<T>>>,
    }

    impl<C> BoundaryHook<C> {
        /// Creates a new boundary hook from parts, and the number of workers.
        pub fn new(
            client: C,
            requests: tokio::sync::mpsc::UnboundedReceiver<SourceInstanceRequest>,
            workers: u64,
        ) -> Self {
            Self {
                client,
                requests,
                workers,
                suppress: Default::default(),
                sources: Default::default(),
                pending: Default::default(),
            }
        }
    }

    #[async_trait]
    impl<C, T> GenericClient<StorageCommand<T>, StorageResponse<T>> for BoundaryHook<C, T>
    where
        C: GenericClient<StorageCommand<T>, StorageResponse<T>>,
        T: fmt::Debug + Clone + Send,
    {
        async fn send(&mut self, cmd: StorageCommand<T>) -> Result<(), anyhow::Error> {
            let mut render_requests = Vec::new();
            if let StorageCommand::CreateSources(sources) = &cmd {
                for source in sources.iter() {
                    if let Some(requests) = self.pending.remove(&source.id) {
                        render_requests.extend(requests.into_iter().map(|request| {
                            RenderSourcesCommand {
                                debug_name: format!(
                                    "SourceDataflow({:?}, {:?})",
                                    request.dataflow_id, request.source_id
                                ),
                                dataflow_id: request.dataflow_id,
                                as_of: Some(request.as_of.clone()),
                                source_imports: once((
                                    request.source_id,
                                    SourceInstanceDesc {
                                        description: source.desc.clone(),
                                        storage_metadata: source.storage_metadata.clone(),
                                        arguments: request.arguments,
                                    },
                                ))
                                .collect(),
                            }
                        }));
                    }
                    self.sources.insert(
                        source.id,
                        (source.desc.clone(), source.storage_metadata.clone()),
                    );
                }
            }

            self.client.send(cmd).await?;
            if !render_requests.is_empty() {
                self.client
                    .send(StorageCommand::RenderSources(render_requests))
                    .await?;
            }
            Ok(())
        }
        async fn recv(&mut self) -> Result<Option<StorageResponse<T>>, anyhow::Error> {
            // The receive logic draws from either the responses of the client, or requests for source instantiation.
            let mut response = None;
            while response.is_none() {
                tokio::select! {
                    cmd = self.requests.recv() => match cmd {
                        None => break,
                        Some(request) => {
                            let unique_id = request.unique_id();
                            if !self.suppress.contains_key(&unique_id) {
                                if let Some((source, storage_metadata)) = self.sources.get(&request.source_id) {
                                    let command = StorageCommand::RenderSources(vec![RenderSourcesCommand {
                                        debug_name: format!("SourceDataflow({:?}, {:?})", request.dataflow_id, request.source_id),
                                        dataflow_id: request.dataflow_id,
                                        as_of: Some(request.as_of.clone()),
                                        source_imports: once((request.source_id, SourceInstanceDesc {
                                            description: source.clone(),
                                            storage_metadata: storage_metadata.clone(),
                                            arguments: request.arguments,
                                        })).collect(),
                                    }]);
                                    self.client.send(command).await.unwrap()
                                } else {
                                    self.pending.entry(request.source_id).or_insert(Vec::new()).push(request);
                                }
                            }
                            // Introduce, decrement, and potentially remove the suppression count.
                            *self.suppress.entry(unique_id).or_insert(self.workers) -= 1;
                            if self.suppress[&unique_id] == 0 {
                                self.suppress.remove(&unique_id);
                            }
                        },
                    },
                    resp = self.client.recv() => {
                        response = resp?;
                    }
                }
            }
            Ok(response)
        }
    }
}
