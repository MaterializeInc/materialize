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

//! Keeping a fallible operator's two outputs apart when it has only one output.
//!
//! [`StreamExt::unary_fallible`](crate::operator::StreamExt::unary_fallible) gives an
//! operator an output per half, which is what a fallible operator should use. An
//! operator whose core takes a single container builder cannot: a join core hands its
//! closure one session and returns one stream. [`SplitBuilder`] is for that case. It
//! routes each `Result` as it is pushed, into one builder per half, and [`Split`]
//! carries the two halves to [`unzip`], which hands each to its own stream.
//!
//! Each half is a container of its own type, so the ok half can be columnar while the
//! error half stays a `Vec`, and a consumer of either half reads it without consulting
//! the other.
//!
//! A sum would be the worse shape for this data. Its columnar container shares one time
//! and one diff column across both variants, so reading a row requires a rank over the
//! variant bitmap to find its position. The ok half is read on every record of a join
//! and the error half is empty in a healthy dataflow, which is the wrong way round to
//! be paying for that.

use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::progress::Timestamp;
use timely::{Accountable, Container, container::NoopBuilder};

/// The ok and error halves of a fallible operator's output.
#[derive(Clone, Debug)]
pub struct Split<O, E> {
    /// Records the operator computed.
    pub ok: O,
    /// Errors the operator produced instead.
    pub err: E,
}

impl<O: Default, E: Default> Default for Split<O, E> {
    fn default() -> Self {
        Self {
            ok: O::default(),
            err: E::default(),
        }
    }
}

impl<O: Accountable, E: Accountable> Accountable for Split<O, E> {
    #[inline]
    fn record_count(&self) -> i64 {
        self.ok.record_count() + self.err.record_count()
    }
}

/// Routes `Result`s into a [`Split`], each half built by a builder of its own.
///
/// The routing happens where the record is pushed, so the halves arrive already apart
/// and [`unzip`] moves containers rather than visiting records.
pub struct SplitBuilder<OCB, ECB>
where
    OCB: ContainerBuilder<Container: Default>,
    ECB: ContainerBuilder<Container: Default>,
{
    ok: OCB,
    err: ECB,
    /// Pairs up whatever the two builders release, so `extract` and `finish` can hand
    /// back one container. Either half may be empty: the builders decide when to
    /// release independently of each other.
    staged: Split<OCB::Container, ECB::Container>,
}

impl<OCB, ECB> Default for SplitBuilder<OCB, ECB>
where
    OCB: ContainerBuilder<Container: Default>,
    ECB: ContainerBuilder<Container: Default>,
{
    fn default() -> Self {
        Self {
            ok: OCB::default(),
            err: ECB::default(),
            staged: Split {
                ok: Default::default(),
                err: Default::default(),
            },
        }
    }
}

impl<D, X, T, R, OCB, ECB> PushInto<(Result<D, X>, T, R)> for SplitBuilder<OCB, ECB>
where
    OCB: ContainerBuilder<Container: Default> + PushInto<(D, T, R)>,
    ECB: ContainerBuilder<Container: Default> + PushInto<(X, T, R)>,
{
    #[inline]
    fn push_into(&mut self, (data, time, diff): (Result<D, X>, T, R)) {
        match data {
            Ok(data) => self.ok.push_into((data, time, diff)),
            Err(err) => self.err.push_into((err, time, diff)),
        }
    }
}

impl<OCB, ECB> ContainerBuilder for SplitBuilder<OCB, ECB>
where
    OCB: ContainerBuilder<Container: Default>,
    ECB: ContainerBuilder<Container: Default>,
    OCB::Container: Accountable + Clone + 'static,
    ECB::Container: Accountable + Clone + 'static,
{
    type Container = Split<OCB::Container, ECB::Container>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Self::Container> {
        let Self { ok, err, staged } = self;
        pair(ok.extract(), err.extract(), staged)
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Self::Container> {
        let Self { ok, err, staged } = self;
        pair(ok.finish(), err.finish(), staged)
    }

    #[inline]
    fn relax(&mut self) {
        self.ok.relax();
        self.err.relax();
        self.staged = Default::default();
    }
}

/// Moves whatever the two builders released into `staged`, leaving them empty.
///
/// Taking rather than borrowing is what the builders expect: a released container is
/// the caller's to consume, and the builder reuses the allocation it gets back.
fn pair<'a, O: Default, E: Default>(
    ok: Option<&mut O>,
    err: Option<&mut E>,
    staged: &'a mut Split<O, E>,
) -> Option<&'a mut Split<O, E>> {
    if ok.is_none() && err.is_none() {
        return None;
    }
    staged.ok = ok.map(std::mem::take).unwrap_or_default();
    staged.err = err.map(std::mem::take).unwrap_or_default();
    Some(staged)
}

/// Separates the halves a [`SplitBuilder`] paired up, one stream each.
///
/// Both halves are already the container their stream wants, so this moves containers
/// and visits no record. `name` is the rendered operator's name.
pub fn unzip<'scope, T, O, E>(
    stream: Stream<'scope, T, Split<O, E>>,
    name: &str,
) -> (Stream<'scope, T, O>, Stream<'scope, T, E>)
where
    T: Timestamp,
    O: Container + Default + Clone + 'static,
    E: Container + Default + Clone + 'static,
{
    let mut builder = OperatorBuilder::new(name.to_string(), stream.scope());
    let (ok_output, ok_stream) = builder.new_output();
    let mut ok_output = OutputBuilder::<_, NoopBuilder<O>>::from(ok_output);
    let (err_output, err_stream) = builder.new_output();
    let mut err_output = OutputBuilder::<_, NoopBuilder<E>>::from(err_output);
    let mut input = builder.new_input(stream, Pipeline);

    builder.build(move |_capabilities| {
        move |_frontiers| {
            let mut ok_output = ok_output.activate();
            let mut err_output = err_output.activate();
            input.for_each(|time, data| {
                // One capability per output, both at the input's time.
                let ok_cap = time.retain(0);
                let err_cap = time.retain(1);
                ok_output
                    .session_with_builder(&ok_cap)
                    .give_container(&mut data.ok);
                err_output
                    .session_with_builder(&err_cap)
                    .give_container(&mut data.err);
            });
        }
    });

    (ok_stream, err_stream)
}

#[cfg(test)]
mod tests;
