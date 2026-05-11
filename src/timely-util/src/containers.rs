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

//! Reusable containers.

use std::marker::PhantomData;

use timely::container::ContainerBuilder;

pub mod stack;

/// A no-op [`ContainerBuilder`] that never emits a container.
///
/// Useful for outputs that require a `ContainerBuilder` type parameter but only ever push whole
/// containers (e.g. through a `give_container`-style API), never individual elements. The builder
/// holds no state and returns `None` from both `extract` and `finish`; there is no [`PushInto`]
/// impl, so element-wise pushes are a compile error.
///
/// [`PushInto`]: timely::container::PushInto
#[derive(Debug, Copy, Clone)]
pub struct NoopContainerBuilder<C>(PhantomData<C>);

impl<C> Default for NoopContainerBuilder<C> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<C> ContainerBuilder for NoopContainerBuilder<C> {
    type Container = C;

    #[inline]
    fn extract(&mut self) -> Option<&mut Self::Container> {
        None
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Self::Container> {
        None
    }
}
