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

use std::panic;

use scopeguard::defer;

use mz_ore::future::OreFutureExt;
use mz_ore::panic::set_abort_on_panic;

// IMPORTANT!!! Do not add any tests to this file. This test sets and removes panic hooks
// and can interfere with any concurrently running test. Therefore, it needs to be run
// in isolation.

#[tokio::test]
async fn catch_panic_async() {
    let old_hook = panic::take_hook();
    defer! {
        panic::set_hook(old_hook);
    }

    set_abort_on_panic();

    let result = async {
        panic!("panicked");
    }
    .ore_catch_unwind()
    .await
    .unwrap_err()
    .downcast::<&str>()
    .unwrap();

    assert_eq!(*result, "panicked");
}
