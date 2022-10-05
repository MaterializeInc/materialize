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

//! Graph utilities.

use std::collections::HashSet;

/// A non-recursive implementation of a fallible depth-first traversal
/// starting from `root`.
///
/// Assumes that nodes in the graph all have unique node ids.
///
/// `at_enter` runs when entering a node. It is expected to return an in-order
/// list of the children of the node. You can omit children from the list
/// returned if you want to skip traversing the subgraphs corresponding to
/// those children. If no children are omitted, `at_enter` can be thought
/// of as a function that processes the nodes of the graph in pre-order.
///
/// `at_exit` runs when exiting a node. It can be thought of as a function that
/// processes the nodes of the graph in post-order.
///
/// This function only enters and exits a node at most once and thus is safe to
/// run even if the graph contains a cycle.
pub fn try_nonrecursive_dft<Graph, NodeId, AtEnter, AtExit, E>(
    graph: &Graph,
    root: NodeId,
    at_enter: &mut AtEnter,
    at_exit: &mut AtExit,
) -> Result<(), E>
where
    NodeId: std::cmp::Eq + std::hash::Hash,
    AtEnter: FnMut(&Graph, &NodeId) -> Result<Vec<NodeId>, E>,
    AtExit: FnMut(&Graph, &NodeId) -> Result<(), E>,
{
    // All nodes that have been entered but not exited. Last node in the vec is
    // the node that we most recently entered.
    let mut entered = Vec::new();
    // All nodes that have been exited.
    let mut exited = HashSet::new();

    // Pseudocode for the recursive version of this function would look like:
    // ```
    // children = at_enter(graph, node)
    // foreach child in children:
    //    recursive_call(graph, child)
    // atexit(graph, node)
    // ```
    // In this non-recursive implementation, you can think of the call stack as
    // been replaced by `entered`. Every time an object is pushed into `entered`
    // would have been a time you would have pushed a recursive call onto the
    // call stack. Likewise, times an object is popped from `entered` would have
    // been times when recursive calls leave the stack.

    // Enter from the root.
    let children = at_enter(graph, &root)?;
    entered_node(&mut entered, root, children);
    while !entered.is_empty() {
        if let Some(to_enter) = find_next_child_to_enter(&mut entered, &mut exited) {
            let children = at_enter(graph, &to_enter)?;
            entered_node(&mut entered, to_enter, children);
        } else {
            // If this node has no more children to descend into,
            // exit the current node and run `at_exit`.
            let (to_exit, _) = entered.pop().unwrap();
            at_exit(graph, &to_exit)?;
            exited.insert(to_exit);
        }
    }
    Ok(())
}

/// Same as [`try_nonrecursive_dft`], but allows changes to be made to the graph.
pub fn try_nonrecursive_dft_mut<Graph, NodeId, AtEnter, AtExit, E>(
    graph: &mut Graph,
    root: NodeId,
    at_enter: &mut AtEnter,
    at_exit: &mut AtExit,
) -> Result<(), E>
where
    NodeId: std::cmp::Eq + std::hash::Hash + Clone,
    AtEnter: FnMut(&mut Graph, &NodeId) -> Result<Vec<NodeId>, E>,
    AtExit: FnMut(&mut Graph, &NodeId) -> Result<(), E>,
{
    // Code in this method is identical to the code in `nonrecursive_dft`.
    let mut entered = Vec::new();
    let mut exited = HashSet::new();

    let children = at_enter(graph, &root)?;
    entered_node(&mut entered, root, children);
    while !entered.is_empty() {
        if let Some(to_enter) = find_next_child_to_enter(&mut entered, &mut exited) {
            let children = at_enter(graph, &to_enter)?;
            entered_node(&mut entered, to_enter, children);
        } else {
            let (to_exit, _) = entered.pop().unwrap();
            at_exit(graph, &to_exit)?;
            exited.insert(to_exit);
        }
    }
    Ok(())
}

/// A non-recursive implementation of an infallible depth-first traversal
/// starting from `root`.
///
/// Assumes that nodes in the graph all have unique node ids.
///
/// `at_enter` runs when entering a node. It is expected to return an in-order
/// list of the children of the node. You can omit children from the list
/// returned if you want to skip the traversing subgraphs corresponding to
/// those children. If no children are omitted, `at_enter` can be thought
/// of as a function that processes the nodes of the graph in pre-order.
///
/// `at_exit` runs when exiting a node. It can be thought of as a function that
/// processes the nodes of the graph in post-order.
///
/// This function only enters and exits a node at most once and thus is safe to
/// run even if the graph contains a cycle.
pub fn nonrecursive_dft<Graph, NodeId, AtEnter, AtExit>(
    graph: &Graph,
    root: NodeId,
    at_enter: &mut AtEnter,
    at_exit: &mut AtExit,
) where
    NodeId: std::cmp::Eq + std::hash::Hash,
    AtEnter: FnMut(&Graph, &NodeId) -> Vec<NodeId>,
    AtExit: FnMut(&Graph, &NodeId) -> (),
{
    // All nodes that have been entered but not exited. Last node in the vec is
    // the node that we most recently entered.
    let mut entered = Vec::new();
    // All nodes that have been exited.
    let mut exited = HashSet::new();

    // Pseudocode for the recursive version of this function would look like:
    // ```
    // atenter(graph, node)
    // foreach child in children(graph, node):
    //    recursive_call(graph, child)
    // atexit(graph, node)
    // ```
    // In this non-recursive implementation, you can think of the call stack as
    // been replaced by `entered`. Every time an object is pushed into `entered`
    // would have been a time you would have pushed a recursive call onto the
    // call stack. Likewise, times an object is popped from `entered` would have
    // been times when recursive calls leave the stack.

    // Enter from the root.
    let children = at_enter(graph, &root);
    entered_node(&mut entered, root, children);
    while !entered.is_empty() {
        if let Some(to_enter) = find_next_child_to_enter(&mut entered, &mut exited) {
            let children = at_enter(graph, &to_enter);
            entered_node(&mut entered, to_enter, children);
        } else {
            // If this node has no more children to descend into,
            // exit the current node and run `at_exit`.
            let (to_exit, _) = entered.pop().unwrap();
            at_exit(graph, &to_exit);
            exited.insert(to_exit);
        }
    }
}

/// Same as [`nonrecursive_dft`], but allows changes to be made to the graph.
pub fn nonrecursive_dft_mut<Graph, NodeId, AtEnter, AtExit>(
    graph: &mut Graph,
    root: NodeId,
    at_enter: &mut AtEnter,
    at_exit: &mut AtExit,
) where
    NodeId: std::cmp::Eq + std::hash::Hash + Clone,
    AtEnter: FnMut(&mut Graph, &NodeId) -> Vec<NodeId>,
    AtExit: FnMut(&mut Graph, &NodeId) -> (),
{
    // Code in this method is identical to the code in `nonrecursive_dft`.
    let mut entered = Vec::new();
    let mut exited = HashSet::new();

    let children = at_enter(graph, &root);
    entered_node(&mut entered, root, children);
    while !entered.is_empty() {
        if let Some(to_enter) = find_next_child_to_enter(&mut entered, &mut exited) {
            let children = at_enter(graph, &to_enter);
            entered_node(&mut entered, to_enter, children);
        } else {
            let (to_exit, _) = entered.pop().unwrap();
            at_exit(graph, &to_exit);
            exited.insert(to_exit);
        }
    }
}

/// Add to `entered` that we have entered `node` and `node` has `children`.
fn entered_node<NodeId>(
    entered: &mut Vec<(NodeId, Vec<NodeId>)>,
    node: NodeId,
    mut children: Vec<NodeId>,
) where
    NodeId: std::cmp::Eq + std::hash::Hash,
{
    // Reverse children because `find_next_child_to_enter` will traverse the
    // list of children by popping them out from the back.
    children.reverse();
    entered.push((node, children))
}

/// Find the next child node, if any, that we have not entered.
fn find_next_child_to_enter<NodeId>(
    entered: &mut Vec<(NodeId, Vec<NodeId>)>,
    exited: &mut HashSet<NodeId>,
) -> Option<NodeId>
where
    NodeId: std::cmp::Eq + std::hash::Hash,
{
    let (_, children) = entered.last_mut().unwrap();
    while let Some(child) = children.pop() {
        if !exited.contains(&child) {
            return Some(child);
        }
    }
    None
}
