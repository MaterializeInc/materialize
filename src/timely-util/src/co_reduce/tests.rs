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

// The tests drive the operator with differential's stock `arrange_by_key`, which the
// repo lint discourages in favor of the compute-crate `MzArrange` wrapper. This crate
// has no such wrapper, and the stock arrangement is exactly what these unit tests need.
#![allow(clippy::disallowed_methods)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use super::cursor::{own_current_key, read_key_values};
use super::*;
use differential_dataflow::AsCollection;
use differential_dataflow::input::Input;
use differential_dataflow::trace::implementations::ord_neu::{OrdValSpine, RcOrdValBuilder};
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::{Capture, Inspect, Probe, ToStream};

type Spine = OrdValSpine<u64, u64, u64, isize>;
type Builder_ = RcOrdValBuilder<u64, u64, u64, isize>;

/// Net-of-two closure: input 0 positive, input 1 negated, keep positive residual on
/// the empty output value `0u64`.
fn net_positive(_k: &u64, inputs: &[&[(u64, isize)]], out: &mut Vec<(u64, isize)>) {
    let sum = |s: &[(u64, isize)]| s.iter().map(|(_v, d)| *d).sum::<isize>();
    let net = sum(inputs[0]) - sum(inputs[1]);
    if net > 0 {
        out.push((0u64, net));
    }
}

#[mz_ore::test]
fn co_reduce_sum_two_inputs_static() {
    let captured = timely::execute_directly(move |worker| {
        let (mut in0, mut in1, cap) = worker.dataflow(|scope| {
            let (h0, c0) = scope.new_collection::<u64, isize>();
            let (h1, c1) = scope.new_collection::<u64, isize>();
            let a0 = c0.map(|k| (k, k)).arrange_by_key();
            let a1 = c1.map(|k| (k, k)).arrange_by_key();
            let out = co_reduce2::<
                u64,
                TraceAgent<Spine>,
                TraceAgent<Spine>,
                u64,
                u64,
                u64,
                isize,
                Builder_,
                Spine,
                _,
            >(a0, a1, "test", usize::MAX, net_positive);
            let cap = out.as_collection(|k, _v| *k).inner.capture();
            (h0, h1, cap)
        });
        // input 0: keys 1,2,3 each once; input 1: key 2 once, key 3 twice.
        // net: 1 -> +1 (keep), 2 -> 0 (drop), 3 -> -1 (drop). Output: {1}.
        in0.insert(1);
        in0.insert(2);
        in0.insert(3);
        in1.insert(2);
        in1.insert(3);
        in1.insert(3);
        in0.close();
        in1.close();
        cap
    });
    let mut rows: Vec<(u64, u64, isize)> = captured
        .extract()
        .into_iter()
        .flat_map(|(_t, data)| data)
        .map(|(k, _t, r)| (k, 0u64, r))
        .collect();
    rows.sort();
    assert_eq!(rows, vec![(1u64, 0u64, 1isize)]);
}

#[mz_ore::test]
fn co_reduce_incremental() {
    let captured = timely::execute_directly(move |worker| {
        let (mut in0, mut in1, probe, cap) = worker.dataflow(|scope| {
            let (h0, c0) = scope.new_collection::<u64, isize>();
            let (h1, c1) = scope.new_collection::<u64, isize>();
            let a0 = c0.map(|k| (k, k)).arrange_by_key();
            let a1 = c1.map(|k| (k, k)).arrange_by_key();
            let out = co_reduce2::<
                u64,
                TraceAgent<Spine>,
                TraceAgent<Spine>,
                u64,
                u64,
                u64,
                isize,
                Builder_,
                Spine,
                _,
            >(a0, a1, "test", usize::MAX, net_positive);
            let coll = out.as_collection(|k, _v| *k);
            let (probe, probed) = coll.inner.probe();
            let cap = probed.capture();
            (h0, h1, probe, cap)
        });

        // Round 1 (time 0): in0={1,1}, in1={1}. net(1) = 2 - 1 = +1 -> output +1.
        in0.insert(1);
        in0.insert(1);
        in1.insert(1);
        in0.advance_to(1);
        in1.advance_to(1);
        in0.flush();
        in1.flush();
        worker.step_while(|| probe.less_than(in0.time()));

        // Round 2 (time 1): retract one in0 1. net(1) = 1 - 1 = 0 -> retract -> -1.
        in0.remove(1);
        in0.advance_to(2);
        in1.advance_to(2);
        in0.flush();
        in1.flush();
        worker.step_while(|| probe.less_than(in0.time()));

        // Round 3 (time 2): insert in0 1 once. net(1) = 2 - 1 = +1 -> output +1.
        in0.insert(1);
        in0.advance_to(3);
        in1.advance_to(3);
        in0.flush();
        in1.flush();
        worker.step_while(|| probe.less_than(in0.time()));

        in0.close();
        in1.close();
        cap
    });

    let stream: Vec<(u64, u64, isize)> = captured
        .extract()
        .into_iter()
        .flat_map(|(t, data)| data.into_iter().map(move |(k, _t, r)| (k, t, r)))
        .collect();

    // The round-2 retraction must appear in the change stream.
    assert!(
        stream.iter().any(|(k, _t, r)| *k == 1 && *r == -1),
        "expected a retraction of key 1 in the change stream, got {stream:?}"
    );

    // Consolidating over time leaves the net multiplicity +1 for key 1.
    let mut consolidated: BTreeMap<u64, isize> = BTreeMap::new();
    for (k, _t, r) in &stream {
        *consolidated.entry(*k).or_default() += *r;
    }
    consolidated.retain(|_k, r| *r != 0);
    assert_eq!(
        consolidated.into_iter().collect::<Vec<_>>(),
        vec![(1u64, 1isize)]
    );
}

/// Value-aware closure: output the maximum present value across all inputs, with diff
/// `+1`. A collapsed empty-value operator could not express this.
fn max_present(_k: &u64, inputs: &[&[(u64, isize)]], out: &mut Vec<(u64, isize)>) {
    let mut best: Option<u64> = None;
    for slice in inputs {
        for (v, d) in *slice {
            if *d > 0 {
                best = Some(best.map_or(*v, |b| b.max(*v)));
            }
        }
    }
    if let Some(v) = best {
        out.push((v, 1));
    }
}

#[mz_ore::test]
fn co_reduce_value_aware() {
    let captured = timely::execute_directly(move |worker| {
        let (mut in0, mut in1, cap) = worker.dataflow(|scope| {
            let (h0, c0) = scope.new_collection::<(u64, u64), isize>();
            let (h1, c1) = scope.new_collection::<(u64, u64), isize>();
            let a0 = c0.arrange_by_key();
            let a1 = c1.arrange_by_key();
            let out = co_reduce2::<
                u64,
                TraceAgent<Spine>,
                TraceAgent<Spine>,
                u64,
                u64,
                u64,
                isize,
                Builder_,
                Spine,
                _,
            >(a0, a1, "test", usize::MAX, max_present);
            // Retain the output value: the closure keys output on input values.
            let cap = out.as_collection(|k, v| (*k, *v)).inner.capture();
            (h0, h1, cap)
        });
        // key 1: in0 value 10, in1 value 20, both +1. max = 20 -> ((1, 20), +1).
        in0.insert((1, 10));
        in1.insert((1, 20));
        in0.close();
        in1.close();
        cap
    });
    let mut rows: Vec<((u64, u64), isize)> = captured
        .extract()
        .into_iter()
        .flat_map(|(_t, data)| data)
        .map(|(kv, _t, r)| (kv, r))
        .collect();
    rows.sort();
    assert_eq!(rows, vec![((1u64, 20u64), 1isize)]);
}

/// Echoes every input `(value, diff)` unchanged, so one key emits every distinct
/// input value as its own output value.
fn echo_values(_k: &u64, inputs: &[&[(u64, isize)]], out: &mut Vec<(u64, isize)>) {
    for slice in inputs {
        for (v, d) in *slice {
            out.push((*v, *d));
        }
    }
}

#[mz_ore::test]
fn co_reduce_multi_value_per_key() {
    // Read the output arrangement back through its batch stream. A linear scan of a
    // batch (or `as_collection` + external consolidation) always recovers the correct
    // multiset even from a value-unsorted batch, because updates are only ever combined
    // when values compare equal, never dropped. So the corruption is invisible to a
    // summed readback. It is visible only in the batch's stored value order, which
    // downstream value seeks (binary search) and spine merges rely on being ascending.
    //
    // Batches are `Rc`-backed (neither `Send` nor `Ord`), so they cannot leave the
    // worker via `capture`/`extract`. Instead an `inspect_batch` walks each batch's
    // cursor in place and records, per key, its `(value, time, diff)` updates in stored
    // order into a shared buffer.
    type Recorded = Vec<(u64, Vec<(u64, u64, isize)>)>;
    let recorded: Arc<Mutex<Recorded>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&recorded);
    timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            // Key 1 gains a new distinct value at each of three times, in non-ascending
            // value order. Feeding all updates as one stream message makes
            // `arrange_by_key` form a single batch spanning times 0, 1, 2 under one
            // capability, so all three times retire in one round under that capability.
            // `emit_deltas` appends them to the same per-key buffer time-major
            // (30@t0, 10@t1, 20@t2), i.e. value-unsorted. Without the value sort the
            // builder writes the vals in that order, violating the batch invariant.
            let a0 = vec![
                ((1u64, 30u64), 0, 1isize),
                ((1u64, 10u64), 1, 1isize),
                ((1u64, 20u64), 2, 1isize),
            ]
            .to_stream(scope)
            .as_collection()
            .arrange_by_key();
            // `co_reduce2` is two-input; the second arm carries no updates, so
            // `echo_values` echoes only `a0` and the stored-order guard still holds.
            let a1 = Vec::<((u64, u64), u64, isize)>::new()
                .to_stream(scope)
                .as_collection()
                .arrange_by_key();
            let out = co_reduce2::<
                u64,
                TraceAgent<Spine>,
                TraceAgent<Spine>,
                u64,
                u64,
                u64,
                isize,
                Builder_,
                Spine,
                _,
            >(a0, a1, "test", usize::MAX, echo_values);
            out.stream.inspect_batch(move |_t, batches| {
                let mut recorded = sink.lock().expect("lock poisoned");
                for batch in batches {
                    let mut cursor = batch.cursor();
                    while cursor.key_valid(batch) {
                        let key = own_current_key::<_, u64>(&cursor, batch);
                        let mut updates: Vec<(u64, u64, isize)> = Vec::new();
                        read_key_values(&mut cursor, batch, &mut updates);
                        recorded.push((key, updates));
                        cursor.step_key(batch);
                    }
                }
            });
        });
    });

    let recorded = Arc::try_unwrap(recorded)
        .expect("no outstanding references")
        .into_inner()
        .expect("lock poisoned");

    // The batch invariant: within a batch each key's stored values are strictly
    // ascending. The pre-fix code stores them time-major (30, 10, 20), failing this.
    for (key, updates) in &recorded {
        let mut vals_order: Vec<u64> = Vec::new();
        for (v, _t, _d) in updates {
            if vals_order.last() != Some(v) {
                vals_order.push(*v);
            }
        }
        let mut sorted = vals_order.clone();
        sorted.sort();
        assert_eq!(
            vals_order, sorted,
            "key {key} stored values are not ascending: {vals_order:?}"
        );
    }

    // The multiset is correct regardless (see the note above), asserted for completeness.
    let mut multiset: BTreeMap<(u64, u64), isize> = BTreeMap::new();
    for (key, updates) in &recorded {
        for (v, _t, d) in updates {
            *multiset.entry((*key, *v)).or_default() += d;
        }
    }
    multiset.retain(|_kv, r| *r != 0);
    assert_eq!(
        multiset.into_iter().collect::<Vec<_>>(),
        vec![((1, 10), 1), ((1, 20), 1), ((1, 30), 1)]
    );
}

/// Builds a one-round dataflow with `keys` keys, all present once in input 0 only (net
/// `+1` each), drives the single worker to completion counting worker steps, and returns
/// the consolidated output multiset and the step count.
///
/// The step count is the yielding witness. `co_reduce2` is the only operator that
/// re-activates itself here (via its fuel path), so extra steps under a small fuel
/// isolate its yielding. We count worker steps rather than the operator's own closure
/// invocations because the closure is internal to `co_reduce2` and cannot be hooked
/// without changing its signature.
fn run_fueled_round(fuel: usize, keys: u64) -> (Vec<(u64, isize)>, usize) {
    let (captured, steps) = timely::execute_directly(move |worker| {
        let (mut in0, in1, cap) = worker.dataflow(|scope| {
            let (h0, c0) = scope.new_collection::<u64, isize>();
            let (h1, c1) = scope.new_collection::<u64, isize>();
            let a0 = c0.map(|k| (k, k)).arrange_by_key();
            let a1 = c1.map(|k| (k, k)).arrange_by_key();
            let out = co_reduce2::<
                u64,
                TraceAgent<Spine>,
                TraceAgent<Spine>,
                u64,
                u64,
                u64,
                isize,
                Builder_,
                Spine,
                _,
            >(a0, a1, "test", fuel, net_positive);
            let cap = out.as_collection(|k, _v| *k).inner.capture();
            (h0, h1, cap)
        });

        for k in 0..keys {
            in0.insert(k);
        }
        in0.close();
        in1.close();

        // Drive to completion, counting steps. Each `co_reduce2` self-reactivation
        // under fuel adds a step.
        let mut steps = 0usize;
        while worker.step() {
            steps += 1;
        }
        (cap, steps)
    });

    let mut consolidated: BTreeMap<u64, isize> = BTreeMap::new();
    for (_t, data) in captured.extract() {
        for (k, _t, r) in data {
            *consolidated.entry(k).or_default() += r;
        }
    }
    consolidated.retain(|_k, r| *r != 0);
    (consolidated.into_iter().collect(), steps)
}

#[mz_ore::test]
fn co_reduce_fuels() {
    // Stage a dirty set far larger than the fuel budget in one round. Assert both that
    // the final output is correct and complete and that the operator yielded (took more
    // steps than the same round drained in one shot). The complete-output assertion
    // guards the invariant that a round is never sealed while any dirty key still owes
    // output: a premature seal would drop the deferred keys' `+1`s from the result.
    const KEYS: u64 = 8_000;
    const SMALL_FUEL: usize = 100;

    let expected: Vec<(u64, isize)> = (0..KEYS).map(|k| (k, 1isize)).collect();

    // Small fuel: many activations. Large fuel: the whole round drains in one activation.
    let (out_small, steps_small) = run_fueled_round(SMALL_FUEL, KEYS);
    let (out_large, steps_large) = run_fueled_round(usize::MAX, KEYS);

    // (1) Output is correct and complete under both budgets: fueling shed no work.
    assert_eq!(out_small, expected, "small-fuel output incomplete or wrong");
    assert_eq!(out_large, expected, "large-fuel output incomplete or wrong");

    // (2) The small-fuel run took strictly more worker steps, proving `co_reduce2`
    // re-activated itself to yield rather than draining the whole dirty set in one shot.
    assert!(
        steps_small > steps_large,
        "expected fueling to add worker steps: small={steps_small}, large={steps_large}"
    );
    assert!(
        steps_small > 1,
        "expected the fueled operator to take more than one activation, took {steps_small}"
    );
}

#[mz_ore::test]
fn co_reduce_fuels_retraction() {
    // `co_reduce_fuels` only covers insertion under small fuel. Retraction is the case
    // that actually stresses the fuel/seal interaction: a round must not seal while any
    // fuel-deferred key still owes a retraction, or the stale `+1` from a prior round
    // would survive in the output. Stage a dirty set (KEYS/2) far larger than the fuel
    // budget for the retracting round, so it spans many activations before it seals.
    const KEYS: u64 = 8_000;
    const SMALL_FUEL: usize = 100;

    let stream: Vec<(u64, u64, isize)> = {
        let captured = timely::execute_directly(move |worker| {
            let (mut in0, mut in1, probe, cap) = worker.dataflow(|scope| {
                let (h0, c0) = scope.new_collection::<u64, isize>();
                let (h1, c1) = scope.new_collection::<u64, isize>();
                let a0 = c0.map(|k| (k, k)).arrange_by_key();
                let a1 = c1.map(|k| (k, k)).arrange_by_key();
                let out = co_reduce2::<
                    u64,
                    TraceAgent<Spine>,
                    TraceAgent<Spine>,
                    u64,
                    u64,
                    u64,
                    isize,
                    Builder_,
                    Spine,
                    _,
                >(a0, a1, "test", SMALL_FUEL, net_positive);
                let coll = out.as_collection(|k, _v| *k);
                let (probe, probed) = coll.inner.probe();
                let cap = probed.capture();
                (h0, h1, probe, cap)
            });

            // Round 1 (time 0): every key present once in input 0 only. net(k) = 1 for
            // all KEYS keys -> output +1 each.
            for k in 0..KEYS {
                in0.insert(k);
            }
            in0.advance_to(1);
            in1.advance_to(1);
            in0.flush();
            in1.flush();
            worker.step_while(|| probe.less_than(in0.time()));

            // Round 2 (time 1): retract the first half of the keys by matching them on
            // input 1, netting those keys to 0 -> output -1 each. The other half is
            // untouched and stays at +1.
            for k in 0..KEYS / 2 {
                in1.insert(k);
            }
            in0.advance_to(2);
            in1.advance_to(2);
            in0.flush();
            in1.flush();
            worker.step_while(|| probe.less_than(in0.time()));

            in0.close();
            in1.close();
            cap
        });

        captured
            .extract()
            .into_iter()
            .flat_map(|(t, data)| data.into_iter().map(move |(k, _t, r)| (k, t, r)))
            .collect()
    };

    // Every retracted key must appear as a negative diff in the change stream, not merely
    // be absent from the final consolidated output: fuel must retract explicitly, not
    // just fail to re-emit.
    let mut retracted: BTreeSet<u64> = BTreeSet::new();
    for (k, _t, r) in &stream {
        if *k < KEYS / 2 && *r < 0 {
            retracted.insert(*k);
        }
    }
    assert_eq!(
        retracted.len(),
        usize::try_from(KEYS / 2).expect("KEYS/2 fits in usize"),
        "expected every retracted key to appear as a negative diff in the change stream"
    );

    // Consolidating over time leaves exactly the surviving keys, each once.
    let mut consolidated: BTreeMap<u64, isize> = BTreeMap::new();
    for (k, _t, r) in &stream {
        *consolidated.entry(*k).or_default() += r;
    }
    consolidated.retain(|_k, r| *r != 0);
    let expected: Vec<(u64, isize)> = (KEYS / 2..KEYS).map(|k| (k, 1isize)).collect();
    assert_eq!(
        consolidated.into_iter().collect::<Vec<_>>(),
        expected,
        "final output must be exactly the surviving keys"
    );
}
