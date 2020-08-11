// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::HashMap, ffi::c_void, time::Instant};

pub mod time;

#[derive(Copy, Clone, Debug)]
// These constructors are dead on macOS
#[allow(dead_code)]
pub enum ProfStartTime {
    Instant(Instant),
    TimeImmemorial,
}

#[derive(Clone, Debug)]
pub struct WeightedStack {
    pub addrs: Vec<usize>,
    pub weight: f64,
}

#[derive(Default)]
pub struct StackProfile {
    annotations: Vec<String>,
    // The second element is the index in `annotations`, if one exists.
    stacks: Vec<(WeightedStack, Option<usize>)>,
}

impl StackProfile {
    pub fn push(&mut self, stack: WeightedStack, annotation: Option<&str>) {
        let anno_idx = if let Some(annotation) = annotation {
            Some(
                self.annotations
                    .iter()
                    .position(|anno| annotation == anno.as_str())
                    .unwrap_or_else(|| {
                        self.annotations.push(annotation.to_string());
                        self.annotations.len() - 1
                    }),
            )
        } else {
            None
        };
        self.stacks.push((stack, anno_idx))
    }
}
pub struct SymbolTrieNode {
    pub name: String,
    pub weight: f64,
    links: Vec<usize>,
}

pub struct WeightedSymbolTrie {
    arena: Vec<SymbolTrieNode>,
}

impl WeightedSymbolTrie {
    fn new() -> Self {
        let root = SymbolTrieNode {
            name: "".to_string(),
            weight: 0.0,
            links: vec![],
        };
        let arena = vec![root];
        Self { arena }
    }
    pub fn dfs<F: FnMut(&SymbolTrieNode), G: FnMut(&SymbolTrieNode, bool)>(
        &self,
        mut pre: F,
        mut post: G,
    ) {
        self.dfs_inner(0, &mut pre, &mut post, true);
    }

    fn dfs_inner<F: FnMut(&SymbolTrieNode), G: FnMut(&SymbolTrieNode, bool)>(
        &self,
        cur: usize,
        pre: &mut F,
        post: &mut G,
        is_last: bool,
    ) {
        let node = &self.arena[cur];
        pre(node);
        for &link_idx in node.links.iter() {
            let is_last = link_idx == *node.links.last().unwrap();
            self.dfs_inner(link_idx, pre, post, is_last);
        }
        post(node, is_last);
    }

    fn step(&mut self, node: usize, next_name: &str) -> usize {
        for &link_idx in self.arena[node].links.iter() {
            if next_name == self.arena[link_idx].name {
                return link_idx;
            }
        }
        let next = SymbolTrieNode {
            name: next_name.to_string(),
            weight: 0.0,
            links: vec![],
        };
        let idx = self.arena.len();
        self.arena.push(next);
        self.arena[node].links.push(idx);
        idx
    }

    fn node_mut(&mut self, idx: usize) -> &mut SymbolTrieNode {
        &mut self.arena[idx]
    }
}
/// Given some stack traces along with their weights,
/// collate them into a tree structure by function name.
///
/// For example: given the following stacks and weights:
/// ([0x1234, 0xabcd], 100)
/// ([0x123a, 0xabff, 0x1234], 200)
/// ([0x1234, 0xffcc], 50)
/// and assuming that 0x1234 and 0x123a come from the function `f`,
/// 0xabcd and 0xabff come from `g`, and 0xffcc from `h`, this will produce:
///
/// "f" (350) -> "g" 200
///  |
///  v
/// "h" (50)
pub fn collate_stacks(profile: StackProfile) -> WeightedSymbolTrie {
    let mut all_addrs = vec![];
    let mut any_annotation = false;
    for (stack, annotation) in profile.stacks.iter() {
        all_addrs.extend(stack.addrs.iter().cloned());
        any_annotation |= annotation.is_some();
    }
    // Sort so addresses from the same images are together,
    // to avoid thrashing `backtrace::resolve`'s cache of
    // parsed images.
    all_addrs.sort();
    all_addrs.dedup();
    let addr_to_symbols = all_addrs
        .into_iter()
        .map(|addr| {
            let mut syms = vec![];
            backtrace::resolve(addr as *mut c_void, |sym| {
                let name = sym
                    .name()
                    .map(|sn| sn.to_string())
                    .unwrap_or_else(|| "???".to_string());
                syms.push(name);
            });
            syms.reverse();
            (addr, syms)
        })
        .collect::<HashMap<_, _>>();
    let mut trie = WeightedSymbolTrie::new();
    let StackProfile {
        annotations,
        stacks,
    } = profile;
    for (stack, annotation) in stacks {
        let mut cur = if any_annotation {
            let annotation = annotation
                .map(|idx| annotations[idx].as_str())
                .unwrap_or("unknown");
            trie.node_mut(0).weight += stack.weight;
            trie.step(0, annotation)
        } else {
            0
        };
        for name in stack
            .addrs
            .into_iter()
            .flat_map(|addr| addr_to_symbols.get(&addr).unwrap().iter())
        {
            trie.node_mut(cur).weight += stack.weight;
            cur = trie.step(cur, name);
        }
        trie.node_mut(cur).weight += stack.weight;
    }
    trie
}
