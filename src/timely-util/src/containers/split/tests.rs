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

use timely::container::{CapacityContainerBuilder, ContainerBuilder, PushInto};

use super::{Split, SplitBuilder};

type Builder = SplitBuilder<
    CapacityContainerBuilder<Vec<(u64, u64, i64)>>,
    CapacityContainerBuilder<Vec<(String, u64, i64)>>,
>;

/// Drains a builder, concatenating every half it releases.
fn drain(builder: &mut Builder) -> (Vec<(u64, u64, i64)>, Vec<(String, u64, i64)>) {
    let mut oks = Vec::new();
    let mut errs = Vec::new();
    while let Some(Split { ok, err }) = builder.extract() {
        oks.append(ok);
        errs.append(err);
    }
    while let Some(Split { ok, err }) = builder.finish() {
        oks.append(ok);
        errs.append(err);
    }
    (oks, errs)
}

#[mz_ore::test]
fn routes_each_variant_to_its_half() {
    let mut builder = Builder::default();
    for i in 0..8u64 {
        let item = if i % 3 == 0 {
            Err(format!("e{i}"))
        } else {
            Ok(i)
        };
        builder.push_into((item, i, 1i64));
    }
    let (oks, errs) = drain(&mut builder);
    assert_eq!(
        oks,
        vec![(1, 1, 1), (2, 2, 1), (4, 4, 1), (5, 5, 1), (7, 7, 1)]
    );
    let errs: Vec<_> = errs.into_iter().map(|(e, t, _)| (e, t)).collect();
    assert_eq!(
        errs,
        vec![
            ("e0".to_string(), 0),
            ("e3".to_string(), 3),
            ("e6".to_string(), 6)
        ]
    );
}

/// `record_count` is load bearing for progress tracking, and a pair has to account for
/// both halves or the operator under-reports what it produced.
#[mz_ore::test]
fn record_count_sums_both_halves() {
    use timely::Accountable;

    let pair = Split {
        ok: vec![(1u64, 0u64, 1i64); 3],
        err: vec![("e".to_string(), 0u64, 1i64); 2],
    };
    assert_eq!(pair.record_count(), 5);
}

/// A builder that released nothing must say so, or the operator ships empty containers
/// on every activation.
#[mz_ore::test]
fn an_empty_builder_releases_nothing() {
    let mut builder = Builder::default();
    assert!(builder.extract().is_none());
    assert!(builder.finish().is_none());
}

/// Only one half receiving records still has to release, with the other half empty.
#[mz_ore::test]
fn one_sided_input_still_releases() {
    let mut builder = Builder::default();
    for i in 0..4u64 {
        builder.push_into((Ok(i), i, 1i64));
    }
    let (oks, errs) = drain(&mut builder);
    assert_eq!(oks.len(), 4);
    assert!(errs.is_empty());
}

/// The halves have to reach separate streams, each under a capability of its own, or a
/// downstream operator reading one half sees the other half's progress.
#[mz_ore::test]
fn unzip_sends_each_half_to_its_own_stream() {
    use timely::dataflow::operators::Capture;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::core::to_stream::ToStreamBuilder;

    let updates: Vec<(Result<u64, String>, u64, i64)> = vec![
        (Ok(0), 0, 1),
        (Err("e1".to_string()), 1, 1),
        (Ok(2), 2, 1),
        (Err("e3".to_string()), 3, -1),
    ];

    let (oks, errs) = timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let stream = updates.to_stream_with_builder::<_, Builder>(scope);
            let (oks, errs) = super::unzip(stream, "Test");
            (oks.capture(), errs.capture())
        })
    });

    assert_eq!(
        oks.extract()
            .into_iter()
            .flat_map(|(_cap, updates)| updates)
            .collect::<Vec<_>>(),
        vec![(0, 0, 1), (2, 2, 1)]
    );
    assert_eq!(
        errs.extract()
            .into_iter()
            .flat_map(|(_cap, updates)| updates)
            .collect::<Vec<_>>(),
        vec![("e1".to_string(), 1, 1), ("e3".to_string(), 3, -1)]
    );
}
