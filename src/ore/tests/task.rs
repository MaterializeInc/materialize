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

use mz_ore::task::JoinSetExt;

#[tokio::test] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
async fn join_set_exts() {
    let mut js = tokio::task::JoinSet::new();
    js.spawn_named(|| "test".to_string(), async {
        std::future::pending::<String>().await
    })
    .abort();

    assert!(matches!(js.join_next().await, Some(Err(je)) if je.is_cancelled()));
}
