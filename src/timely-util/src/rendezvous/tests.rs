// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use itertools::Itertools;
use proptest::prelude::*;
use proptest::strategy::Union;
use timely::progress::Antichain;
use timely::progress::frontier::MutableAntichain;
use timely::scheduling::activate::{Activations, SyncActivator};

use super::{Attachment, Event, Reader, Rendezvous, Subscription, adjust};

/// Items are `u64`s, the metadata counts the publishes that reached the point, and there are two
/// hold kinds.
type Point = Rendezvous<u64, u64, u64, 2>;

fn at(t: u64) -> Antichain<u64> {
    Antichain::from_elem(t)
}

fn frontier(t: Option<u64>) -> Antichain<u64> {
    t.map_or_else(Antichain::new, at)
}

/// Records the activations of paths `[0]`, `[1]`, ... so a test can see which writer was woken.
struct Wakes(Activations);

impl Wakes {
    fn new() -> Self {
        Wakes(Activations::new(None))
    }

    fn activator(&self, path: usize) -> SyncActivator {
        SyncActivator::new(vec![path], self.0.sync())
    }

    /// The paths activated since the last call.
    fn take(&mut self) -> BTreeSet<usize> {
        self.0.advance();
        let woken = RefCell::new(BTreeSet::new());
        self.0.map_active(|path| {
            woken.borrow_mut().insert(path[0]);
        });
        woken.into_inner()
    }
}

/// A toy writer publishing the log `0..len` with `upper` at `len`.
fn publish_log(attachment: &mut Attachment<u64, u64, u64, 2>, len: u64) -> bool {
    let arrived = len.checked_sub(1);
    attachment.publish(
        (0..len).collect(),
        at(len),
        |count| *count += 1,
        arrived.as_ref(),
    )
}

fn upper(point: &Point) -> Antichain<u64> {
    point.read(|published| published.upper.clone())
}

/// The accumulated hold updates of `kind`, consolidated.
fn hold_updates(point: &Point, kind: usize) -> Vec<(u64, i64)> {
    let mut state = point.lock();
    state.holds[kind].updates().cloned().collect()
}

#[mz_ore::test]
fn a_hold_at_or_above_the_meet_does_not_move_it() {
    let mut holds = MutableAntichain::new();
    assert!(
        adjust(&mut holds, &Antichain::new(), &at(5)),
        "the first hold sets the meet"
    );
    assert!(
        !adjust(&mut holds, &Antichain::new(), &at(7)),
        "a second hold above the meet leaves it at 5"
    );
    assert!(
        !adjust(&mut holds, &at(7), &at(9)),
        "advancing the hold above the meet leaves it at 5"
    );
    assert!(
        !adjust(&mut holds, &at(9), &Antichain::new()),
        "releasing the hold above the meet leaves it at 5"
    );
}

#[mz_ore::test]
fn moving_the_lowest_hold_moves_the_meet() {
    let mut holds = MutableAntichain::new();
    adjust(&mut holds, &Antichain::new(), &at(5));
    adjust(&mut holds, &Antichain::new(), &at(7));
    assert!(adjust(&mut holds, &at(5), &at(6)), "the meet advances to 6");
    assert!(
        adjust(&mut holds, &at(6), &Antichain::new()),
        "the meet advances to 7"
    );
    assert!(
        adjust(&mut holds, &at(7), &Antichain::new()),
        "the meet becomes empty"
    );
}

#[mz_ore::test]
fn a_reader_wakes_the_writer_only_when_it_moves_a_meet() {
    let mut wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let writer = point.attach(
        wakes.activator(0),
        Vec::new(),
        at(0),
        |_| {},
        [Vec::new(), Vec::new()],
    );
    let mut low = point
        .reader_with(|_| Ok::<_, ()>([at(2), at(2)]))
        .expect("infallible");
    let mut high = point
        .reader_with(|_| Ok::<_, ()>([at(5), at(5)]))
        .expect("infallible");
    assert_eq!(wakes.take(), BTreeSet::new(), "registering wakes nobody");

    high.set_hold(0, at(6));
    assert_eq!(wakes.take(), BTreeSet::new(), "the meet stays at 2");
    assert!(!writer.take_holds_moved());
    low.set_hold(0, at(3));
    assert_eq!(wakes.take(), BTreeSet::from([0]), "the meet moved to 3");
    assert!(writer.take_holds_moved());
    assert!(!writer.take_holds_moved(), "taking the flag resets it");
    assert_eq!(point.holds(0), at(3));
    assert_eq!(point.holds(1), at(2), "kinds are independent");

    drop(high);
    assert_eq!(wakes.take(), BTreeSet::new(), "dropping above the meet");
    assert!(!writer.take_holds_moved());
    drop(low);
    assert_eq!(wakes.take(), BTreeSet::from([0]), "dropping the last hold");
    assert!(writer.take_holds_moved());
    assert!(point.holds(0).is_empty());
    assert!(point.holds(1).is_empty());
}

#[mz_ore::test]
fn reader_with_refuses_without_registering() {
    let point = Arc::new(Point::with_meta(0));
    let refused = point.reader_with(|published| Err::<[Antichain<u64>; 2], _>(published.meta));
    assert_eq!(refused.err(), Some(0));
    assert!(point.holds(0).is_empty());
    assert!(point.holds(1).is_empty());
}

#[mz_ore::test]
fn dropping_the_writer_clears_the_wakeup() {
    let mut wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let writer = point.attach(
        wakes.activator(0),
        Vec::new(),
        at(0),
        |_| {},
        [Vec::new(), Vec::new()],
    );
    let mut reader = point
        .reader_with(|_| Ok::<_, ()>([at(1), at(1)]))
        .expect("infallible");
    drop(writer);
    reader.set_hold(0, at(2));
    assert_eq!(wakes.take(), BTreeSet::new());
}

/// Writer drop leaves readers at the last published `upper`, whether the writer closes before it
/// drops, drops without closing, or tries to publish the empty frontier after closing.
#[mz_ore::test]
fn writer_drop_freezes_readers_at_the_last_published_upper() {
    for close_first in [false, true] {
        let wakes = Wakes::new();
        let point = Arc::new(Point::with_meta(0));
        let mut writer = point.attach(
            wakes.activator(0),
            Vec::new(),
            at(0),
            |_| {},
            [Vec::new(), Vec::new()],
        );
        let (subscription, _) = point.subscribe(wakes.activator(1));
        assert!(publish_log(&mut writer, 4));
        if close_first {
            writer.close();
            assert!(writer.is_closed());
            assert!(
                !writer.publish(Vec::new(), Antichain::new(), |_| {}, None),
                "a closed writer does not publish"
            );
        }
        drop(writer);
        assert_eq!(upper(&point), at(4));
        let uppers: Vec<_> = subscription
            .drain()
            .into_iter()
            .filter_map(|event| match event {
                Event::Upper(upper) => Some(upper),
                Event::Item(_) => None,
            })
            .collect();
        assert_eq!(uppers.last(), Some(&at(4)));
        assert!(
            uppers.iter().all(|upper| !upper.is_empty()),
            "no subscriber sees the empty frontier"
        );
        let reader = point
            .reader_with(|published| Ok::<_, ()>([published.upper.clone(), Antichain::new()]))
            .expect("infallible");
        assert_eq!(reader.hold(0), at(4).borrow());
    }
}

#[derive(Clone, Debug)]
enum HoldOp {
    NewReader(Option<u64>, Option<u64>),
    SetHold(prop::sample::Index, usize, Option<u64>),
    CloneReader(prop::sample::Index),
    DropReader(prop::sample::Index),
    Attach(Option<u64>, Option<u64>),
    WriterSet(prop::sample::Index, usize, Option<u64>),
    DropWriter(prop::sample::Index),
}

fn hold_op() -> impl Strategy<Value = HoldOp> {
    let time = || prop::option::weighted(0.9, 0..8u64);
    Union::new(vec![
        (time(), time())
            .prop_map(|(a, b)| HoldOp::NewReader(a, b))
            .boxed(),
        (any::<prop::sample::Index>(), 0..2usize, time())
            .prop_map(|(i, k, t)| HoldOp::SetHold(i, k, t))
            .boxed(),
        any::<prop::sample::Index>()
            .prop_map(HoldOp::CloneReader)
            .boxed(),
        any::<prop::sample::Index>()
            .prop_map(HoldOp::DropReader)
            .boxed(),
        (time(), time())
            .prop_map(|(a, b)| HoldOp::Attach(a, b))
            .boxed(),
        (any::<prop::sample::Index>(), 0..2usize, time())
            .prop_map(|(i, k, t)| HoldOp::WriterSet(i, k, t))
            .boxed(),
        any::<prop::sample::Index>()
            .prop_map(HoldOp::DropWriter)
            .boxed(),
    ])
}

/// The reference accumulation: the count of every time held, per kind.
#[derive(Default)]
struct Model([BTreeMap<u64, i64>; 2]);

impl Model {
    fn update(&mut self, kind: usize, lower: &Antichain<u64>, upper: &Antichain<u64>) {
        for time in upper.iter() {
            *self.0[kind].entry(*time).or_default() += 1;
        }
        for time in lower.iter() {
            *self.0[kind].entry(*time).or_default() -= 1;
        }
        self.0[kind].retain(|_, count| *count != 0);
    }

    fn meet(&self, kind: usize) -> Antichain<u64> {
        frontier(self.0[kind].keys().next().copied())
    }
}

/// A toy writer that registers holds of its own at attachment and releases them before dropping.
struct ToyWriter {
    attachment: Attachment<u64, u64, u64, 2>,
    holds: [Antichain<u64>; 2],
}

impl ToyWriter {
    fn set(&mut self, kind: usize, next: Antichain<u64>) {
        let previous = std::mem::replace(&mut self.holds[kind], next);
        let mut locked = Attachment::lock_all(&[&self.attachment]);
        locked.adjust(0, kind, &previous, &self.holds[kind]);
    }
}

proptest! {
    /// After every operation the point's meets equal the reference model's, and once every reader
    /// and writer has dropped every accumulation is empty.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn holds_balance(ops in prop::collection::vec(hold_op(), 0..64)) {
        let wakes = Wakes::new();
        let point = Arc::new(Point::with_meta(0));
        let mut model = Model::default();
        let mut readers: Vec<Reader<u64, u64, u64, 2>> = Vec::new();
        let mut writers: Vec<ToyWriter> = Vec::new();
        // The point accepts one writer in its life, so later `Attach`es are skipped.
        let mut attached = false;
        for op in ops {
            match op {
                HoldOp::NewReader(a, b) => {
                    let holds = [frontier(a), frontier(b)];
                    for (kind, hold) in holds.iter().enumerate() {
                        model.update(kind, &Antichain::new(), hold);
                    }
                    readers.push(
                        point.reader_with(|_| Ok::<_, ()>(holds)).expect("infallible"),
                    );
                }
                HoldOp::SetHold(index, kind, t) => {
                    if readers.is_empty() {
                        continue;
                    }
                    let index = index.index(readers.len());
                    let reader = &mut readers[index];
                    let next = frontier(t);
                    model.update(kind, &reader.hold(kind).to_owned(), &next);
                    reader.set_hold(kind, next);
                }
                HoldOp::CloneReader(index) => {
                    if readers.is_empty() {
                        continue;
                    }
                    let clone = readers[index.index(readers.len())].clone();
                    for kind in 0..2 {
                        model.update(kind, &Antichain::new(), &clone.hold(kind).to_owned());
                    }
                    readers.push(clone);
                }
                HoldOp::DropReader(index) => {
                    if readers.is_empty() {
                        continue;
                    }
                    let reader = readers.swap_remove(index.index(readers.len()));
                    for kind in 0..2 {
                        model.update(kind, &reader.hold(kind).to_owned(), &Antichain::new());
                    }
                }
                HoldOp::Attach(a, b) => {
                    if std::mem::replace(&mut attached, true) {
                        continue;
                    }
                    let holds = [frontier(a), frontier(b)];
                    for (kind, hold) in holds.iter().enumerate() {
                        model.update(kind, &Antichain::new(), hold);
                    }
                    let updates = holds.clone().map(|hold| {
                        hold.into_iter().map(|time| (time, 1)).collect()
                    });
                    let attachment = point.attach(
                        wakes.activator(writers.len()),
                        Vec::new(),
                        at(0),
                        |_| {},
                        updates,
                    );
                    writers.push(ToyWriter { attachment, holds });
                }
                HoldOp::WriterSet(index, kind, t) => {
                    if writers.is_empty() {
                        continue;
                    }
                    let index = index.index(writers.len());
                    let writer = &mut writers[index];
                    let next = frontier(t);
                    model.update(kind, &writer.holds[kind], &next);
                    writer.set(kind, next);
                }
                HoldOp::DropWriter(index) => {
                    if writers.is_empty() {
                        continue;
                    }
                    let mut writer = writers.swap_remove(index.index(writers.len()));
                    for kind in 0..2 {
                        model.update(kind, &writer.holds[kind], &Antichain::new());
                        writer.set(kind, Antichain::new());
                    }
                }
            }
            for kind in 0..2 {
                prop_assert_eq!(point.holds(kind), model.meet(kind));
            }
        }
        for mut writer in writers.drain(..) {
            for kind in 0..2 {
                writer.set(kind, Antichain::new());
            }
        }
        readers.clear();
        for kind in 0..2 {
            prop_assert!(hold_updates(&point, kind).is_empty());
        }
    }
}

#[derive(Clone, Debug)]
enum UpperOp {
    /// The writer publishes the next item to both points.
    Publish,
    /// The writer closes both attachments.
    Close,
    /// The writer drops both attachments.
    Drop,
}

fn upper_op() -> impl Strategy<Value = UpperOp> {
    Union::new_weighted(vec![
        (6, Just(UpperOp::Publish).boxed()),
        (1, Just(UpperOp::Close).boxed()),
        (1, Just(UpperOp::Drop).boxed()),
    ])
}

proptest! {
    /// With a writer publishing a growing log to two points, closing and dropping, the `upper` of
    /// every point only moves forward.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn upper_is_monotone_at_every_point(ops in prop::collection::vec(upper_op(), 0..64)) {
        let wakes = Wakes::new();
        let points = [Arc::new(Point::with_meta(0)), Arc::new(Point::with_meta(0))];
        let mut attachments: Vec<Attachment<u64, u64, u64, 2>> = points
            .iter()
            .map(|point| {
                point.attach(
                    wakes.activator(0),
                    Vec::new(),
                    at(0),
                    |_| {},
                    [Vec::new(), Vec::new()],
                )
            })
            .collect();
        let mut len = 0;
        let mut previous: Vec<Antichain<u64>> = points.iter().map(|p| upper(p)).collect();
        for op in ops {
            match op {
                UpperOp::Publish => {
                    len += 1;
                    for attachment in attachments.iter_mut() {
                        publish_log(attachment, len);
                    }
                }
                UpperOp::Close => {
                    for attachment in attachments.iter_mut() {
                        attachment.close();
                    }
                }
                UpperOp::Drop => attachments.clear(),
            }
            for (point, previous) in points.iter().zip_eq(previous.iter_mut()) {
                let now = upper(point);
                prop_assert!(
                    timely::PartialOrder::less_equal(previous, &now),
                    "upper regressed from {:?} to {:?}",
                    previous,
                    now
                );
                *previous = now;
            }
        }
    }
}

/// Checks that `events` is a seed `Item(0..s), Upper(s)` followed by `Item(k), Upper(k + 1)` for
/// every `k` in `s..len`.
fn assert_seed_then_deltas(events: Vec<Event<u64, u64>>, len: u64) {
    let mut items = Vec::new();
    let mut last_upper = None;
    for event in events {
        match event {
            Event::Item(item) => items.push(item),
            Event::Upper(upper) => {
                assert_eq!(
                    upper,
                    at(u64::try_from(items.len()).expect("fits")),
                    "an upper covers exactly the items before it"
                );
                last_upper = Some(upper);
            }
        }
    }
    assert_eq!(
        items,
        (0..len).collect::<Vec<_>>(),
        "no gap and no duplicate"
    );
    assert_eq!(last_upper, Some(at(len)));
}

#[mz_ore::test]
fn a_subscriber_sees_the_seed_and_every_later_item_once() {
    let wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let mut writer = point.attach(
        wakes.activator(0),
        Vec::new(),
        at(0),
        |_| {},
        [Vec::new(), Vec::new()],
    );
    let (early, seed) = point.subscribe(wakes.activator(1));
    assert_eq!(seed, at(0));
    for len in 1..=3 {
        publish_log(&mut writer, len);
    }
    let (late, seed) = point.subscribe(wakes.activator(2));
    assert_eq!(seed, at(3));
    for len in 4..=5 {
        publish_log(&mut writer, len);
    }
    // A publish without an arrived item, as after merge work, enqueues nothing.
    writer.publish((0..5).collect(), at(5), |_| {}, None);
    assert_seed_then_deltas(early.drain(), 5);
    assert_seed_then_deltas(late.drain(), 5);
}

#[mz_ore::test]
fn a_subscriber_registered_before_the_writer_attaches_is_seeded() {
    let wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let (subscription, seed) = point.subscribe(wakes.activator(1));
    assert_eq!(seed, at(0));
    let mut writer = point.attach(
        wakes.activator(0),
        (0..3).collect(),
        at(3),
        |_| {},
        [Vec::new(), Vec::new()],
    );
    publish_log(&mut writer, 4);
    // The empty seed's `Upper(0)`, then the attachment's seed, then the delta.
    let events = subscription.drain();
    assert!(matches!(events.first(), Some(Event::Upper(upper)) if *upper == at(0)));
    assert_seed_then_deltas(events.into_iter().skip(1).collect(), 4);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn subscribers_registered_while_the_writer_publishes_see_no_gap_and_no_duplicate() {
    const LEN: u64 = 2000;
    let wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let mut writer = point.attach(
        wakes.activator(0),
        Vec::new(),
        at(0),
        |_| {},
        [Vec::new(), Vec::new()],
    );
    let publisher = std::thread::spawn(move || {
        for len in 1..=LEN {
            publish_log(&mut writer, len);
        }
        writer
    });
    // Bounded, since each subscription buffers up to the whole log.
    let mut subscriptions: Vec<Subscription<u64, u64>> = Vec::new();
    while !publisher.is_finished() && subscriptions.len() < 128 {
        subscriptions.push(point.subscribe(wakes.activator(1)).0);
        std::thread::sleep(std::time::Duration::from_micros(20));
    }
    let _writer = publisher.join().expect("publisher panicked");
    assert!(!subscriptions.is_empty());
    for subscription in subscriptions {
        assert_seed_then_deltas(subscription.drain(), LEN);
    }
}

#[mz_ore::test]
fn a_closed_writer_publishes_no_metadata_but_adjusts_holds() {
    let wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let mut writer = point.attach(
        wakes.activator(0),
        Vec::new(),
        at(0),
        |_| {},
        [vec![(1, 1)], Vec::new()],
    );
    writer.close();
    {
        let mut locked = Attachment::lock_all(&[&writer]);
        locked.update_open(|count| *count = 99);
        assert!(
            locked.adjust(0, 0, &[1], &[2]),
            "a closed writer's holds move"
        );
    }
    assert_eq!(
        point.read(|published| published.meta),
        0,
        "a closed writer leaves the metadata as last published"
    );
    assert_eq!(point.holds(0), at(2));
}

#[mz_ore::test]
fn the_writers_own_adjust_wakes_nobody() {
    let mut wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let writer = point.attach(
        wakes.activator(0),
        Vec::new(),
        at(0),
        |_| {},
        [vec![(1, 1)], Vec::new()],
    );
    let (_subscription, _) = point.subscribe(wakes.activator(1));
    wakes.take();
    {
        let mut locked = Attachment::lock_all(&[&writer]);
        assert!(locked.adjust(0, 0, &[1], &[2]), "the meet moved");
    }
    assert_eq!(wakes.take(), BTreeSet::new());
}

#[mz_ore::test]
fn a_publish_without_an_arrived_item_still_reports_a_moved_upper() {
    let mut wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let mut writer = point.attach(
        wakes.activator(0),
        vec![0],
        at(1),
        |_| {},
        [Vec::new(), Vec::new()],
    );
    let (subscription, _) = point.subscribe(wakes.activator(1));
    subscription.drain();
    wakes.take();

    assert!(
        !writer.publish(vec![0], at(1), |_| {}, None),
        "an unchanged upper"
    );
    assert!(
        subscription.drain().is_empty(),
        "nothing moved, nothing queued"
    );
    assert_eq!(wakes.take(), BTreeSet::new());

    assert!(writer.publish(vec![0], at(3), |_| {}, None));
    assert!(
        matches!(subscription.drain().as_slice(), [Event::Upper(upper)] if *upper == at(3)),
        "the moved upper reaches the subscriber"
    );
    assert_eq!(wakes.take(), BTreeSet::from([1]));
}

#[mz_ore::test]
#[should_panic(expected = "publish regresses upper")]
fn publishing_a_regressing_upper_is_refused_by_a_soft_assert() {
    let wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let mut writer = point.attach(
        wakes.activator(0),
        Vec::new(),
        at(0),
        |_| {},
        [Vec::new(), Vec::new()],
    );
    publish_log(&mut writer, 3);
    writer.publish(Vec::new(), at(1), |_| {}, None);
}

/// Attaches a second writer to a point whose first writer is still attached, has closed, or has
/// dropped, as `end` names.
fn attach_twice(end: &str) {
    let wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let mut first = Some(point.attach(
        wakes.activator(0),
        Vec::new(),
        at(0),
        |_| {},
        [Vec::new(), Vec::new()],
    ));
    match end {
        "closed" => first.as_mut().expect("attached").close(),
        "dropped" => first = None,
        _ => {}
    }
    let _second = point.attach(
        wakes.activator(1),
        Vec::new(),
        at(0),
        |_| {},
        [Vec::new(), Vec::new()],
    );
    drop(first);
}

#[mz_ore::test]
#[should_panic(expected = "attach to a point that already had a writer")]
fn a_point_refuses_a_second_writer_while_the_first_is_attached() {
    attach_twice("attached");
}

#[mz_ore::test]
#[should_panic(expected = "attach to a point that already had a writer")]
fn a_point_refuses_a_second_writer_after_the_first_closed() {
    attach_twice("closed");
}

#[mz_ore::test]
#[should_panic(expected = "attach to a point that already had a writer")]
fn a_point_refuses_a_second_writer_after_the_first_dropped() {
    attach_twice("dropped");
}

#[mz_ore::test]
fn a_point_freezes_when_its_writer_closes_or_drops() {
    let wakes = Wakes::new();
    for close in [false, true] {
        let point = Arc::new(Point::with_meta(0));
        assert!(!point.is_frozen(), "an unattached point is not frozen");
        let mut writer = point.attach(
            wakes.activator(0),
            Vec::new(),
            at(0),
            |_| {},
            [Vec::new(), Vec::new()],
        );
        assert!(!point.is_frozen(), "an attached point is not frozen");
        if close {
            writer.close();
            assert!(point.is_frozen(), "closing freezes the point");
        }
        drop(writer);
        assert!(point.is_frozen(), "dropping freezes the point");
    }
}

#[mz_ore::test]
fn into_holds_returns_the_writers_holds() {
    let wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let writer = point.attach(
        wakes.activator(0),
        Vec::new(),
        at(0),
        |_| {},
        [vec![(3, 1), (5, 1)], vec![(4, 1)]],
    );
    {
        let mut locked = Attachment::lock_all(&[&writer]);
        locked.adjust(0, 0, &[3], &[6]);
    }
    drop(point);
    let consolidated = writer.into_holds().map(|updates| {
        let mut counts = BTreeMap::<u64, i64>::new();
        for (time, diff) in updates {
            *counts.entry(time).or_default() += diff;
        }
        counts
            .into_iter()
            .filter(|(_, count)| *count != 0)
            .collect::<Vec<_>>()
    });
    assert_eq!(consolidated, [vec![(5, 1), (6, 1)], vec![(4, 1)]]);
}

#[mz_ore::test]
#[should_panic(expected = "into_holds: the point is referenced elsewhere")]
fn into_holds_on_a_point_others_reference_is_refused_by_a_soft_assert() {
    let wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let writer = point.attach(
        wakes.activator(0),
        Vec::new(),
        at(0),
        |_| {},
        [Vec::new(), Vec::new()],
    );
    let _reader = point
        .reader_with(|_| Ok::<_, ()>([at(1), at(1)]))
        .expect("infallible");
    drop(point);
    let _ = writer.into_holds();
}

#[mz_ore::test]
#[should_panic(expected = "lock_all: a point is named twice")]
fn lock_all_refuses_a_point_named_twice() {
    let wakes = Wakes::new();
    let point = Arc::new(Point::with_meta(0));
    let writer = point.attach(
        wakes.activator(0),
        Vec::new(),
        at(0),
        |_| {},
        [Vec::new(), Vec::new()],
    );
    let _ = Attachment::lock_all(&[&writer, &writer]);
}
