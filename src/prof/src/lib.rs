// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;
use std::sync::Mutex;
use std::{
    collections::HashMap,
    ffi::{c_void, CString},
    io::BufRead,
    time::Instant,
};

use jemalloc_ctl::raw;
use lazy_static::lazy_static;
use tempfile::NamedTempFile;

use anyhow::bail;

lazy_static! {
    pub static ref PROF_CTL: Option<Arc<Mutex<JemallocProfCtl>>> = {
        if let Some(ctl) = JemallocProfCtl::get() {
            Some(Arc::new(Mutex::new(ctl)))
        } else {
            None
        }
    };
}

#[derive(Copy, Clone, Debug)]
// These constructors are dead on macOS
#[allow(dead_code)]
pub enum ProfStartTime {
    Instant(Instant),
    TimeImmemorial,
}

#[derive(Copy, Clone, Debug)]
pub struct JemallocProfMetadata {
    pub start_time: Option<ProfStartTime>,
}

#[derive(Debug)]
// Per-process singleton object allowing control of jemalloc profiling facilities.
pub struct JemallocProfCtl {
    md: JemallocProfMetadata,
}

#[derive(Clone, Debug)]
pub struct WeightedStack {
    addrs: Vec<usize>,
    weight: usize,
}

pub fn parse_jeheap<R: BufRead>(r: R) -> anyhow::Result<Vec<WeightedStack>> {
    let mut cur_stack = None;
    let mut result = vec![];
    for line in r.lines().into_iter() {
        let line = line?;
        let line = line.trim();
        let words = line.split_ascii_whitespace().collect::<Vec<_>>();
        if words.len() > 0 && words[0] == "@" {
            if cur_stack.is_some() {
                bail!("Stack without corresponding weight!")
            }
            let mut addrs = words[1..]
                .iter()
                .map(|w| {
                    let raw = w.trim_start_matches("0x");
                    usize::from_str_radix(raw, 16)
                })
                .collect::<Result<Vec<_>, _>>()?;
            addrs.reverse();
            cur_stack = Some(addrs);
        }
        if words.len() > 2 && words[0] == "t*:" {
            if let Some(addrs) = cur_stack.take() {
                let weight = str::parse::<usize>(words[2])?;
                result.push(WeightedStack { addrs, weight });
            }
        }
    }
    if cur_stack.is_some() {
        bail!("Stack without corresponding weight!")
    }
    Ok(result)
}

pub struct SymbolTrieNode {
    pub name: String,
    pub weight: usize,
    links: Vec<usize>,
}

pub struct WeightedSymbolTrie {
    arena: Vec<SymbolTrieNode>,
}

impl WeightedSymbolTrie {
    fn new() -> Self {
        let root = SymbolTrieNode {
            name: "".to_string(),
            weight: 0,
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
            weight: 0,
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

pub fn collate_stacks(stacks: Vec<WeightedStack>) -> WeightedSymbolTrie {
    let mut all_addrs = stacks
        .iter()
        .flat_map(|ws| ws.addrs.iter().copied())
        .collect::<Vec<_>>();
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
    for stack in stacks {
        let mut cur = 0;
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

#[cfg(not(target_os = "macos"))]
impl JemallocProfCtl {
    // Creates and returns the global singleton.
    fn get() -> Option<Self> {
        // SAFETY: "opt.prof" is documented as being readable and returning a bool:
        // http://jemalloc.net/jemalloc.3.html#opt.prof
        let prof_enabled: bool = unsafe { raw::read(b"opt.prof\0") }.unwrap();
        if prof_enabled {
            // SAFETY: "opt.prof_active" is documented as being readable and returning a bool:
            // http://jemalloc.net/jemalloc.3.html#opt.prof_active
            let prof_active: bool = unsafe { raw::read(b"opt.prof_active\0") }.unwrap();
            let start_time = if prof_active {
                Some(ProfStartTime::TimeImmemorial)
            } else {
                None
            };
            let md = JemallocProfMetadata { start_time };
            Some(Self { md })
        } else {
            None
        }
    }

    pub fn get_md(&self) -> JemallocProfMetadata {
        self.md
    }

    pub fn activate(&mut self) -> Result<(), jemalloc_ctl::Error> {
        // SAFETY: "prof.active" is documented as being writable and taking a bool:
        // http://jemalloc.net/jemalloc.3.html#prof.active
        unsafe { raw::write(b"prof.active\0", true) }?;
        if self.md.start_time.is_none() {
            self.md.start_time = Some(ProfStartTime::Instant(Instant::now()));
        }
        Ok(())
    }

    pub fn deactivate(&mut self) -> Result<(), jemalloc_ctl::Error> {
        // SAFETY: "prof.active" is documented as being writable and taking a bool:
        // http://jemalloc.net/jemalloc.3.html#prof.active
        unsafe { raw::write(b"prof.active\0", false) }?;
        self.md.start_time = None;
        Ok(())
    }

    pub fn dump(&mut self) -> anyhow::Result<std::fs::File> {
        let f = NamedTempFile::new()?;
        let path = CString::new(f.path().as_os_str().as_bytes().to_vec()).unwrap();

        // SAFETY: "prof.dump" is documented as being writable and taking a C string as input:
        // http://jemalloc.net/jemalloc.3.html#prof.dump
        unsafe { raw::write(b"prof.dump\0", path.as_ptr()) }?;
        Ok(f.into_file())
    }
}
