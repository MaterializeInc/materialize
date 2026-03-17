// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Wrappers around [`lgalloc`] that add application-level memory profiling.
//!
//! All lgalloc allocations and deallocations should go through this module
//! instead of calling the `lgalloc` crate directly. This allows us to track
//! how much memory the application currently holds via lgalloc, including
//! stack traces at each allocation site for heap profiling.
//!
//! Stack traces are stored in a trie (prefix tree) inspired by
//! [heaptrack](https://github.com/KDE/heaptrack). Common stack prefixes share
//! tree nodes, so memory usage is proportional to the number of unique stack
//! *frames* rather than unique stack *traces*.

use std::collections::BTreeMap;
use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub use lgalloc::AllocError;

/// Total bytes currently allocated via lgalloc.
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
/// Number of live lgalloc allocations.
static LIVE_COUNT: AtomicUsize = AtomicUsize::new(0);
/// Monotonically increasing allocation ID.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// All profiling state, protected by a single mutex.
static STATE: Mutex<ProfileState> = Mutex::new(ProfileState::new());

/// A trie node index. Index 0 is the root sentinel.
type NodeId = usize;

/// A node in the stack trace trie.
struct TraceNode {
    /// Instruction pointer address for this frame.
    ip: usize,
    /// Parent node index (0 for the root's children).
    parent: NodeId,
    /// Children sorted by instruction pointer for binary search.
    children: Vec<(usize, NodeId)>,
}

/// A trie that compresses stack traces by sharing common prefixes.
///
/// Traces are inserted root-to-leaf. Each unique path from root to a leaf
/// represents a unique stack trace. The leaf's `NodeId` identifies the trace.
struct TraceTree {
    /// All nodes. Index 0 is the root sentinel.
    nodes: Vec<TraceNode>,
}

impl TraceTree {
    const fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    /// Ensure the root node exists.
    fn ensure_root(&mut self) {
        if self.nodes.is_empty() {
            self.nodes.push(TraceNode {
                ip: 0,
                parent: 0,
                children: Vec::new(),
            });
        }
    }

    /// Insert a stack trace (root-to-leaf order) and return the leaf node ID.
    fn insert(&mut self, addrs: &[usize]) -> NodeId {
        self.ensure_root();
        let mut current = 0; // start at root
        for &ip in addrs {
            let children = &self.nodes[current].children;
            current = match children.binary_search_by_key(&ip, |&(ip, _)| ip) {
                Ok(pos) => children[pos].1,
                Err(pos) => {
                    let new_id = self.nodes.len();
                    self.nodes.push(TraceNode {
                        ip,
                        parent: current,
                        children: Vec::new(),
                    });
                    self.nodes[current].children.insert(pos, (ip, new_id));
                    new_id
                }
            };
        }
        current
    }

    /// Resolve a node ID back to a full stack trace (root-to-leaf order).
    fn resolve(&self, node_id: NodeId) -> Vec<usize> {
        let mut addrs = Vec::new();
        let mut current = node_id;
        while current != 0 {
            addrs.push(self.nodes[current].ip);
            current = self.nodes[current].parent;
        }
        addrs.reverse();
        addrs
    }
}

struct ProfileState {
    /// Trie of stack traces.
    traces: TraceTree,
    /// Live allocations: allocation ID -> (trace node ID, capacity bytes).
    allocations: BTreeMap<u64, (NodeId, usize)>,
}

impl ProfileState {
    const fn new() -> Self {
        Self {
            traces: TraceTree::new(),
            allocations: BTreeMap::new(),
        }
    }

    fn insert(&mut self, alloc_id: u64, addrs: &[usize], capacity_bytes: usize) {
        let node_id = self.traces.insert(addrs);
        self.allocations.insert(alloc_id, (node_id, capacity_bytes));
    }

    fn remove(&mut self, alloc_id: u64) {
        self.allocations.remove(&alloc_id);
    }
}

/// Application-level lgalloc memory stats, tracking live allocations made
/// through this module's [`allocate`] and [`deallocate`] functions.
#[derive(Debug, Clone, Copy)]
pub struct LgAllocStats {
    /// Total bytes currently allocated via lgalloc.
    pub live_bytes: usize,
    /// Number of live lgalloc allocations.
    pub live_count: usize,
}

/// Returns application-level lgalloc memory stats.
pub fn stats() -> LgAllocStats {
    LgAllocStats {
        live_bytes: LIVE_BYTES.load(Ordering::Relaxed),
        live_count: LIVE_COUNT.load(Ordering::Relaxed),
    }
}

/// Returns the current lgalloc heap profile as aggregated (stack_addresses, total_bytes) pairs.
///
/// Allocations sharing the same stack trace are grouped together with their byte counts summed.
pub fn heap_profile() -> Vec<(Vec<usize>, usize)> {
    let state = STATE.lock().expect("poisoned");
    // Aggregate bytes per trace node ID.
    let mut by_node: BTreeMap<NodeId, usize> = BTreeMap::new();
    for &(node_id, capacity_bytes) in state.allocations.values() {
        *by_node.entry(node_id).or_default() += capacity_bytes;
    }
    // Resolve node IDs to full stack traces.
    by_node
        .into_iter()
        .map(|(node_id, bytes)| (state.traces.resolve(node_id), bytes))
        .collect()
}

/// An lgalloc handle that tracks its allocation for profiling.
///
/// Must be passed to [`deallocate`] to free the memory and update
/// the global profiling counters.
pub struct Handle {
    inner: lgalloc::Handle,
    id: u64,
    capacity_bytes: usize,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("id", &self.id)
            .field("capacity_bytes", &self.capacity_bytes)
            .finish_non_exhaustive()
    }
}

/// Allocate memory via lgalloc, tracking the allocation in global profiling counters.
///
/// Captures a backtrace at the call site for heap profiling.
// TODO: Add a feature flag to enable/disable backtrace capture at runtime,
// so that the profiling overhead can be avoided when not needed.
pub fn allocate<T>(capacity: usize) -> Result<(NonNull<T>, usize, Handle), AllocError> {
    #[allow(clippy::disallowed_methods)]
    let (ptr, actual_capacity, handle) = lgalloc::allocate::<T>(capacity)?;
    let bytes = actual_capacity * std::mem::size_of::<T>();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

    // Capture backtrace for heap profiling. `backtrace::trace` walks leaf-to-root,
    // but the trie stores root-to-leaf, so we reverse.
    let mut addrs = Vec::new();
    backtrace::trace(|frame| {
        addrs.push(frame.ip().addr());
        true
    });
    addrs.reverse();

    LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed);
    LIVE_COUNT.fetch_add(1, Ordering::Relaxed);
    STATE.lock().expect("poisoned").insert(id, &addrs, bytes);

    Ok((
        ptr,
        actual_capacity,
        Handle {
            inner: handle,
            id,
            capacity_bytes: bytes,
        },
    ))
}

/// Deallocate lgalloc memory and update profiling counters.
pub fn deallocate(handle: Handle) {
    LIVE_BYTES.fetch_sub(handle.capacity_bytes, Ordering::Relaxed);
    LIVE_COUNT.fetch_sub(1, Ordering::Relaxed);
    STATE.lock().expect("poisoned").remove(handle.id);
    #[allow(clippy::disallowed_methods)]
    lgalloc::deallocate(handle.inner);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[crate::test]
    fn trace_tree_empty_trace() {
        let mut tree = TraceTree::new();
        let id = tree.insert(&[]);
        assert_eq!(id, 0, "empty trace should return root");
        assert_eq!(tree.resolve(id), Vec::<usize>::new());
    }

    #[crate::test]
    fn trace_tree_single_frame() {
        let mut tree = TraceTree::new();
        let id = tree.insert(&[42]);
        assert_ne!(id, 0);
        assert_eq!(tree.resolve(id), vec![42]);
    }

    #[crate::test]
    fn trace_tree_roundtrip() {
        let mut tree = TraceTree::new();
        let trace = vec![10, 20, 30, 40];
        let id = tree.insert(&trace);
        assert_eq!(tree.resolve(id), trace);
    }

    #[crate::test]
    fn trace_tree_shared_prefix() {
        let mut tree = TraceTree::new();
        let id_a = tree.insert(&[1, 2, 3]);
        let id_b = tree.insert(&[1, 2, 4]);

        assert_ne!(id_a, id_b);
        assert_eq!(tree.resolve(id_a), vec![1, 2, 3]);
        assert_eq!(tree.resolve(id_b), vec![1, 2, 4]);

        // Shared prefix [1, 2] means only 4 nodes: root, 1, 2, 3, 4 = 5 total.
        assert_eq!(tree.nodes.len(), 5);
    }

    #[crate::test]
    fn trace_tree_duplicate_returns_same_id() {
        let mut tree = TraceTree::new();
        let id1 = tree.insert(&[10, 20, 30]);
        let id2 = tree.insert(&[10, 20, 30]);
        assert_eq!(id1, id2);
    }

    #[crate::test]
    fn trace_tree_prefix_is_different_from_full() {
        let mut tree = TraceTree::new();
        let id_full = tree.insert(&[1, 2, 3]);
        let id_prefix = tree.insert(&[1, 2]);

        assert_ne!(id_full, id_prefix);
        assert_eq!(tree.resolve(id_full), vec![1, 2, 3]);
        assert_eq!(tree.resolve(id_prefix), vec![1, 2]);
    }

    #[crate::test]
    fn trace_tree_disjoint_traces() {
        let mut tree = TraceTree::new();
        let id_a = tree.insert(&[100, 200]);
        let id_b = tree.insert(&[300, 400]);

        assert_eq!(tree.resolve(id_a), vec![100, 200]);
        assert_eq!(tree.resolve(id_b), vec![300, 400]);
    }

    #[crate::test]
    #[cfg(feature = "proptest")]
    fn trace_tree_roundtrip_proptest() {
        use proptest::prelude::*;

        // Generate 1-10 traces, each with 1-20 frames.
        let trace_strategy =
            proptest::collection::vec(proptest::collection::vec(any::<usize>(), 1..20), 1..10);

        proptest!(|(traces in trace_strategy)| {
            let mut tree = TraceTree::new();
            let ids: Vec<_> = traces.iter().map(|t| tree.insert(t)).collect();
            for (trace, &id) in itertools::Itertools::zip_eq(traces.iter(), &ids) {
                prop_assert_eq!(&tree.resolve(id), trace);
            }
        });
    }

    #[crate::test]
    #[cfg(feature = "proptest")]
    fn trace_tree_dedup_proptest() {
        use proptest::prelude::*;

        let trace_strategy = proptest::collection::vec(any::<usize>(), 1..20);

        proptest!(|(trace in trace_strategy)| {
            let mut tree = TraceTree::new();
            let id1 = tree.insert(&trace);
            let id2 = tree.insert(&trace);
            prop_assert_eq!(id1, id2, "duplicate traces must return same ID");
        });
    }

    #[crate::test]
    #[cfg(feature = "proptest")]
    fn trace_tree_prefix_sharing_proptest() {
        use proptest::prelude::*;

        let strategy = (
            proptest::collection::vec(any::<usize>(), 1..10),
            proptest::collection::vec(any::<usize>(), 1..10),
        );

        proptest!(|(prefix_and_suffix in strategy)| {
            let (prefix, suffix) = prefix_and_suffix;
            let mut full = prefix.clone();
            full.extend_from_slice(&suffix);

            let mut tree = TraceTree::new();
            let id_full = tree.insert(&full);
            let id_prefix = tree.insert(&prefix);

            prop_assert_eq!(&tree.resolve(id_full), &full);
            prop_assert_eq!(&tree.resolve(id_prefix), &prefix);

            if !suffix.is_empty() {
                prop_assert_ne!(id_full, id_prefix);
            }
        });
    }
}
