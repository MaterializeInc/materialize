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

use std::{cell::RefCell, rc::Rc};

use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, ScopeParent};
use timely::progress::operate::SharedProgress;
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Operate, Subgraph, SubgraphBuilder, Timestamp};
use timely::scheduling::Schedule;
use timely::worker::AsWorker;

use crate::builder_async::{button, Button, ButtonHandle};

pub trait ScopeExt: Scope {
    fn scoped_fused<T, R, F>(&mut self, name: &str, func: F) -> (Button, R)
    where
        T: Timestamp + Refines<Self::Timestamp>,
        F: FnOnce(&mut Child<Child<Self, Self::Timestamp>, T>) -> R;
}

impl<'a, G, T> ScopeExt for Child<'a, G, T>
where
    G: ScopeParent,
    T: Timestamp + Refines<G::Timestamp>,
{
    fn scoped_fused<T2, R, F>(&mut self, name: &str, func: F) -> (Button, R)
    where
        T2: Timestamp + Refines<Self::Timestamp>,
        F: FnOnce(&mut Child<Child<Self, T>, T2>) -> R,
    {
        // By creating a nested region that is never exposed to the user we are guaranteed that no
        // streams can enter or leave the region.
        self.region_named(&format!("{name}-inner"), |region| {
            let index = region.subgraph.borrow_mut().allocate_child_id();
            let path = region.subgraph.borrow().path.clone();

            let subscope = RefCell::new(SubgraphBuilder::new_from(
                index,
                path,
                region.logging(),
                region.progress_logging.clone(),
                name,
            ));
            let result = {
                let mut builder = Child {
                    subgraph: &subscope,
                    parent: region.clone(),
                    logging: region.logging.clone(),
                    progress_logging: region.progress_logging.clone(),
                };
                func(&mut builder)
            };
            let mut subgraph = subscope.into_inner().build(region);
            let (internal_summary, shared_progress) = subgraph.get_internal_summary();
            let (handle, button) = button(region, &subgraph.path);
            let subscope = FusedSubgraph {
                local: subgraph.local(),
                inputs: subgraph.inputs(),
                outputs: subgraph.outputs(),
                name: subgraph.name().to_owned(),
                path: subgraph.path().to_vec(),
                handle,
                shared_progress,
                internal_summary,
                subgraph: Some(subgraph),
            };

            region.add_operator_with_index(Box::new(subscope), index);

            (button, result)
        })
    }
}

pub struct FusedSubgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp + Refines<TOuter>,
{
    local: bool,
    inputs: usize,
    outputs: usize,
    name: String,
    path: Vec<usize>,
    subgraph: Option<Subgraph<TOuter, TInner>>,
    shared_progress: Rc<RefCell<SharedProgress<TOuter>>>,
    internal_summary: Vec<Vec<Antichain<TOuter::Summary>>>,
    handle: ButtonHandle,
}

impl<TOuter, TInner> Operate<TOuter> for FusedSubgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp + Refines<TOuter>,
{
    fn local(&self) -> bool {
        self.local
    }
    fn inputs(&self) -> usize {
        self.inputs
    }
    fn outputs(&self) -> usize {
        self.outputs
    }

    fn get_internal_summary(
        &mut self,
    ) -> (
        Vec<Vec<Antichain<TOuter::Summary>>>,
        Rc<RefCell<SharedProgress<TOuter>>>,
    ) {
        (
            self.internal_summary.clone(),
            Rc::clone(&self.shared_progress),
        )
    }

    fn set_external_summary(&mut self) {
        if let Some(ref mut subgraph) = self.subgraph {
            subgraph.set_external_summary()
        }
    }
}

impl<TOuter, TInner> Schedule for FusedSubgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp + Refines<TOuter>,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn path(&self) -> &[usize] {
        &self.path
    }

    fn schedule(&mut self) -> bool {
        if self.handle.local_pressed() {
            if self.handle.all_pressed() {
                self.subgraph = None;
                false
            } else {
                true
            }
        } else {
            self.subgraph.as_mut().unwrap().schedule()
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Barrier;

    use timely::dataflow::operators::generic::source;
    use timely::WorkerConfig;

    use super::*;

    #[mz_ore::test]
    fn region_shutdown() {
        let barrier = Barrier::new(2);
        let (builders, other) = timely::CommunicationConfig::Process(2).try_build().unwrap();
        timely::execute::execute_from(builders, other, WorkerConfig::default(), move |worker| {
            let index = worker.index();
            let (mut button, _) = worker.dataflow::<u64, _, _>(move |scope| {
                scope.scoped_fused::<u64, _, _>("FusedRegion", |scope| {
                    // Create a source that stays alive forever and knows nothing about shutdown
                    source(scope, "Source", |cap, _| {
                        move |output| {
                            output.session(&cap).give(());
                        }
                    });
                })
            });

            // Stage 1, verify dataflow stays alive
            barrier.wait();
            worker.step();
            assert_eq!(worker.installed_dataflows().len(), 1);

            // Stage 2, one worker presses the button but the dataflow stays alive on both workers.
            barrier.wait();
            if index == 0 {
                button.press();
            }
            // This extra barrier is needed to ensure that the other worker sees the press and does
            // the right thing.
            barrier.wait();
            worker.step();
            assert_eq!(worker.installed_dataflows().len(), 1);

            // Stage 3, the other worker also presses the button and the dataflow is cleaned up.
            barrier.wait();
            if index == 1 {
                button.press();
            }
            barrier.wait();
            // After a single step the dataflow is deemed done and is cleaned up.
            worker.step();
            assert_eq!(worker.installed_dataflows().len(), 0);
        })
        .unwrap();
    }
}
