// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Ready-queue DAG executor for parallel typechecking.
//!
//! Each node carries an `OnceLock<Result<Arc<T>, NodeFailure>>`. Workers pull
//! ready nodes from a shared queue, run the supplied closure with the node's
//! direct-dep results, set the result slot, then enqueue any dependent whose
//! remaining-dep count reached zero.

use crate::project::compiler::typecheck::ObjectTypeCheckError;
use crate::project::ir::object_id::ObjectId;
use crossbeam_channel::unbounded;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Final outcome for one node after the DAG executor runs.
#[derive(Debug)]
pub(super) enum NodeOutcome<T> {
    /// Validation succeeded and produced this value.
    Ok(Arc<T>),
    /// The node's own validation failed.
    Failed(ObjectTypeCheckError),
    /// An upstream direct dependency did not produce a successful result.
    Blocked(ObjectId),
}

struct NodeBookkeeping<T> {
    direct_deps: Vec<ObjectId>,
    dependents: Vec<ObjectId>,
    remaining_deps: AtomicUsize,
    result: OnceLock<NodeOutcome<T>>,
}

/// Run the DAG executor over `nodes`.
///
/// `direct_deps` maps each node ID to the IDs of its direct dependencies that
/// are *also nodes* (deps satisfied by external column maps must be excluded
/// before calling this function).
///
/// `dependents` maps each node ID to the IDs of nodes that directly depend on
/// it (the inverse of `direct_deps`).
///
/// `work` is invoked per-node with the node's ID and a map of dep ID → dep
/// result; it returns either the node's produced value or a typecheck error.
pub(super) fn run<T, F>(
    nodes: Vec<ObjectId>,
    mut direct_deps: BTreeMap<ObjectId, Vec<ObjectId>>,
    mut dependents: BTreeMap<ObjectId, Vec<ObjectId>>,
    work: F,
) -> BTreeMap<ObjectId, NodeOutcome<T>>
where
    T: Send + Sync + 'static,
    F: Fn(&ObjectId, &BTreeMap<ObjectId, Arc<T>>) -> Result<T, ObjectTypeCheckError> + Send + Sync,
{
    if nodes.is_empty() {
        return BTreeMap::new();
    }

    let bookkeeping: BTreeMap<ObjectId, Arc<NodeBookkeeping<T>>> = nodes
        .iter()
        .map(|node_id| {
            let deps = direct_deps.remove(node_id).unwrap_or_default();
            let deps_count = deps.len();
            let downstream = dependents.remove(node_id).unwrap_or_default();
            (
                node_id.clone(),
                Arc::new(NodeBookkeeping {
                    direct_deps: deps,
                    dependents: downstream,
                    remaining_deps: AtomicUsize::new(deps_count),
                    result: OnceLock::new(),
                }),
            )
        })
        .collect();
    let bookkeeping = Arc::new(bookkeeping);

    let total = bookkeeping.len();
    let completed = Arc::new(AtomicUsize::new(0));
    // Channel carries `Some(id)` for real work and `None` as a shutdown
    // sentinel posted by the worker that completes the last node.
    let (tx, rx) = unbounded::<Option<ObjectId>>();

    for (node_id, bk) in bookkeeping.iter() {
        if bk.remaining_deps.load(Ordering::Relaxed) == 0 {
            tx.send(Some(node_id.clone())).expect("channel open");
        }
    }

    rayon::scope(|scope| {
        let worker_count = rayon::current_num_threads().max(1);
        for _ in 0..worker_count {
            let rx = rx.clone();
            let tx = tx.clone();
            let bookkeeping = Arc::clone(&bookkeeping);
            let completed = Arc::clone(&completed);
            let work = &work;
            scope.spawn(move |_| {
                worker_loop(rx, tx, bookkeeping, completed, total, worker_count, work);
            });
        }
    });

    bookkeeping
        .iter()
        .map(|(node_id, bk)| {
            let outcome = match bk
                .result
                .get()
                .expect("every node's result must be set before run() returns")
            {
                NodeOutcome::Ok(value) => NodeOutcome::Ok(Arc::clone(value)),
                NodeOutcome::Failed(err) => NodeOutcome::Failed(err.clone()),
                NodeOutcome::Blocked(id) => NodeOutcome::Blocked(id.clone()),
            };
            (node_id.clone(), outcome)
        })
        .collect()
}

fn worker_loop<T, F>(
    rx: crossbeam_channel::Receiver<Option<ObjectId>>,
    tx: crossbeam_channel::Sender<Option<ObjectId>>,
    bookkeeping: Arc<BTreeMap<ObjectId, Arc<NodeBookkeeping<T>>>>,
    completed: Arc<AtomicUsize>,
    total: usize,
    worker_count: usize,
    work: &F,
) where
    T: Send + Sync + 'static,
    F: Fn(&ObjectId, &BTreeMap<ObjectId, Arc<T>>) -> Result<T, ObjectTypeCheckError> + Send + Sync,
{
    loop {
        // `recv()` blocks until a message arrives or the channel disconnects;
        // the worker that completes the last node posts N-1 `None` sentinels
        // so all other workers wake from this call and exit.
        let node_id = match rx.recv() {
            Ok(Some(id)) => id,
            Ok(None) | Err(_) => return,
        };
        let bk = Arc::clone(
            bookkeeping
                .get(&node_id)
                .expect("scheduled node has a bookkeeping entry"),
        );

        let outcome = match gather_dep_results(&bk, &bookkeeping) {
            Ok(deps) => match work(&node_id, &deps) {
                Ok(value) => NodeOutcome::Ok(Arc::new(value)),
                Err(err) => NodeOutcome::Failed(err),
            },
            Err(blocker) => NodeOutcome::Blocked(blocker),
        };

        bk.result
            .set(outcome)
            .unwrap_or_else(|_| panic!("result slot already filled for {node_id:?}"));

        for dependent_id in &bk.dependents {
            let dep_bk = bookkeeping
                .get(dependent_id)
                .expect("dependent has a bookkeeping entry");
            let prev = dep_bk.remaining_deps.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(prev >= 1, "remaining_deps underflow for {dependent_id:?}");
            if prev == 1 {
                tx.send(Some(dependent_id.clone())).expect("channel open");
            }
        }

        let prev = completed.fetch_add(1, Ordering::Relaxed);
        if prev + 1 == total {
            for _ in 0..worker_count.saturating_sub(1) {
                tx.send(None).expect("channel open");
            }
            return;
        }
    }
}

/// Collect direct-dep results for `bk`. Returns `Err(blocker_id)` on the first
/// dep whose result is a failure.
fn gather_dep_results<T>(
    bk: &NodeBookkeeping<T>,
    bookkeeping: &BTreeMap<ObjectId, Arc<NodeBookkeeping<T>>>,
) -> Result<BTreeMap<ObjectId, Arc<T>>, ObjectId> {
    let mut deps = BTreeMap::new();
    for dep_id in &bk.direct_deps {
        let dep_bk = bookkeeping
            .get(dep_id)
            .expect("dep has a bookkeeping entry");
        match dep_bk
            .result
            .get()
            .expect("dep result set before dependent runs")
        {
            NodeOutcome::Ok(value) => {
                deps.insert(dep_id.clone(), Arc::clone(value));
            }
            NodeOutcome::Failed(_) | NodeOutcome::Blocked(_) => {
                return Err(dep_id.clone());
            }
        }
    }
    Ok(deps)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(name: &str) -> ObjectId {
        ObjectId::new("d".into(), "s".into(), name.into())
    }

    /// Build the (nodes, direct_deps, dependents) triple from a list of
    /// `(parent, child)` edges. Roots are inferred — any node mentioned but
    /// without an incoming edge starts with empty deps.
    fn dag(
        edges: &[(&str, &str)],
    ) -> (
        Vec<ObjectId>,
        BTreeMap<ObjectId, Vec<ObjectId>>,
        BTreeMap<ObjectId, Vec<ObjectId>>,
    ) {
        use std::collections::BTreeSet;
        let mut nodes: Vec<ObjectId> = Vec::new();
        let mut seen: BTreeSet<ObjectId> = BTreeSet::new();
        let mut direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
        let mut dependents: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
        for (parent, child) in edges {
            for n in [parent, child] {
                let n_id = id(n);
                if seen.insert(n_id.clone()) {
                    nodes.push(n_id.clone());
                    direct_deps.insert(n_id.clone(), Vec::new());
                    dependents.insert(n_id, Vec::new());
                }
            }
            direct_deps.entry(id(child)).or_default().push(id(parent));
            dependents.entry(id(parent)).or_default().push(id(child));
        }
        (nodes, direct_deps, dependents)
    }

    #[test]
    fn node_outcome_types_compile() {
        let _: NodeOutcome<i32> = NodeOutcome::Ok(Arc::new(7));
        let _: NodeOutcome<i32> = NodeOutcome::Blocked(id("o"));
        let _: NodeOutcome<i32> = NodeOutcome::Failed(ObjectTypeCheckError::internal(
            id("o"),
            std::path::PathBuf::new(),
            String::new(),
        ));
    }

    #[test]
    fn empty_graph_returns_empty() {
        let outcomes = run::<i32, _>(
            Vec::<ObjectId>::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            |_id, _deps| -> Result<i32, ObjectTypeCheckError> {
                panic!("work closure must not be called for empty graph")
            },
        );
        assert!(outcomes.is_empty());
    }

    #[test]
    fn independent_leaves_run_to_completion() {
        let nodes = vec![id("a"), id("b"), id("c"), id("d")];
        let direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> =
            nodes.iter().map(|id| (id.clone(), Vec::new())).collect();
        let dependents: BTreeMap<ObjectId, Vec<ObjectId>> =
            nodes.iter().map(|id| (id.clone(), Vec::new())).collect();

        let outcomes = run::<String, _>(nodes.clone(), direct_deps, dependents, |id, _deps| {
            Ok(id.object().to_string())
        });

        assert_eq!(outcomes.len(), 4);
        for id in &nodes {
            match outcomes.get(id) {
                Some(NodeOutcome::Ok(v)) => assert_eq!(v.as_ref(), id.object()),
                other => panic!("unexpected outcome for {id:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn linear_chain_threads_results() {
        let (nodes, direct_deps, dependents) = dag(&[("a", "b"), ("b", "c")]);

        let outcomes = run::<u64, _>(nodes, direct_deps, dependents, |_id, deps| {
            let upstream: u64 = deps.values().map(|v| **v).sum();
            Ok(1 + upstream)
        });

        let unwrap_ok = |id: &ObjectId| -> u64 {
            match outcomes.get(id).expect("outcome for id") {
                NodeOutcome::Ok(v) => **v,
                other => panic!("unexpected outcome for {id:?}: {other:?}"),
            }
        };
        assert_eq!(unwrap_ok(&id("a")), 1);
        assert_eq!(unwrap_ok(&id("b")), 2);
        assert_eq!(unwrap_ok(&id("c")), 3);
    }

    #[test]
    fn diamond_dispatches_b_and_c_in_parallel() {
        use std::sync::Barrier;

        // a -> {b, c} -> d. b and c each rendezvous on a 2-party barrier
        // before completing — they can only progress if both are running
        // concurrently, proving the executor dispatches them in parallel.
        let (nodes, direct_deps, dependents) =
            dag(&[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")]);
        let barrier = Arc::new(Barrier::new(2));

        let outcomes = run::<u64, _>(nodes, direct_deps, dependents, move |obj_id, _deps| {
            if obj_id.object() == "b" || obj_id.object() == "c" {
                barrier.wait();
            }
            Ok(1u64)
        });

        for name in ["a", "b", "c", "d"] {
            assert!(
                matches!(outcomes.get(&id(name)), Some(NodeOutcome::Ok(_))),
                "node {name} should have succeeded"
            );
        }
    }

    fn fake_typecheck_error(id: &ObjectId, msg: &str) -> ObjectTypeCheckError {
        ObjectTypeCheckError::internal(id.clone(), std::path::PathBuf::from("test"), msg.into())
    }

    #[test]
    fn failure_propagates_to_dependents_and_isolates_other_branches() {
        // Failing branch:  a (FAIL) -> b -> c
        // Healthy branch:  x -> y
        let (nodes, direct_deps, dependents) = dag(&[("a", "b"), ("b", "c"), ("x", "y")]);

        let a = id("a");
        let a_for_closure = a.clone();
        let outcomes = run::<u32, _>(nodes, direct_deps, dependents, move |obj_id, _deps| {
            if *obj_id == a_for_closure {
                Err(fake_typecheck_error(obj_id, "boom"))
            } else {
                Ok(1)
            }
        });

        match outcomes.get(&a).unwrap() {
            NodeOutcome::Failed(err) => assert_eq!(err.error_message(), "boom"),
            other => panic!("expected Failed for a, got {other:?}"),
        }
        match outcomes.get(&id("b")).unwrap() {
            NodeOutcome::Blocked(blocker) => assert_eq!(blocker, &a),
            other => panic!("expected Blocked(a) for b, got {other:?}"),
        }
        match outcomes.get(&id("c")).unwrap() {
            NodeOutcome::Blocked(blocker) => assert_eq!(blocker, &id("b")),
            other => panic!("expected Blocked(b) for c, got {other:?}"),
        }
        assert!(matches!(outcomes.get(&id("x")), Some(NodeOutcome::Ok(_))));
        assert!(matches!(outcomes.get(&id("y")), Some(NodeOutcome::Ok(_))));
    }
}
