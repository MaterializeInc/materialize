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

//! Stack profiles from [`mz_ore::alloc_track`] snapshots.

use std::time::Duration;

use mz_ore::alloc_track::{FreeKind, Site, Snapshot};
use mz_ore::cast::CastFrom;
use pprof_util::{StackProfile, WeightedStack};

/// Which aggregate of a [`Snapshot`] to render.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    /// Live bytes by allocating stack.
    Live,
    /// Allocated bytes since the last history reset, by allocating stack.
    Allocated,
    /// Freed bytes by freeing stack, with the allocating stack stacked on
    /// top of it. A flame graph's base shows who frees, and each tower above
    /// shows what they free.
    Freed,
}

/// Builds a profile weighted by estimated bytes. Every stack is annotated
/// with its memory space, and freed stacks also with how the allocation
/// ended and its mean lifetime bucket. The profile carries no mappings.
pub fn to_stack_profile(snapshot: &Snapshot, view: View) -> StackProfile {
    let mut profile = StackProfile::default();
    let frames = |id: u32| &*snapshot.stacks[usize::cast_from(id)];
    let mut push_sites = |sites: &[Site]| {
        for site in sites {
            profile.push_stack(
                WeightedStack {
                    addrs: frames(site.stack).to_vec(),
                    weight: site.bytes,
                },
                Some(site.space.name()),
            );
        }
    };
    match view {
        View::Live => push_sites(&snapshot.live),
        View::Allocated => push_sites(&snapshot.allocated),
        View::Freed => {
            for site in &snapshot.freed {
                let mut addrs = frames(site.free_stack).to_vec();
                addrs.extend_from_slice(frames(site.alloc_stack));
                let kind = match site.kind {
                    FreeKind::Dealloc => "dealloc",
                    FreeKind::Realloc => "realloc",
                };
                let annotation = format!(
                    "{} {kind} lifetime {}",
                    site.space.name(),
                    lifetime_bucket(site.mean_lifetime)
                );
                profile.push_stack(
                    WeightedStack {
                        addrs,
                        weight: site.bytes,
                    },
                    Some(&annotation),
                );
            }
        }
    }
    profile
}

/// A decade bucket label for `lifetime`, from `<1us` to `>=1000s`.
fn lifetime_bucket(lifetime: Duration) -> &'static str {
    const BUCKETS: [(Duration, &str); 10] = [
        (Duration::from_micros(1), "<1us"),
        (Duration::from_micros(10), "<10us"),
        (Duration::from_micros(100), "<100us"),
        (Duration::from_millis(1), "<1ms"),
        (Duration::from_millis(10), "<10ms"),
        (Duration::from_millis(100), "<100ms"),
        (Duration::from_secs(1), "<1s"),
        (Duration::from_secs(10), "<10s"),
        (Duration::from_secs(100), "<100s"),
        (Duration::from_secs(1000), "<1000s"),
    ];
    BUCKETS
        .iter()
        .find(|(bound, _)| lifetime < *bound)
        .map_or(">=1000s", |(_, label)| label)
}
