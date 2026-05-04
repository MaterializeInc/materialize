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

#![cfg(feature = "pager")]

use mz_ore::pager::{Backend, Handle, pageout, read_at, set_backend, set_scratch_dir, take};
use tempfile::tempdir;

fn ensure_scratch() {
    static INIT: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    let dir = INIT.get_or_init(|| tempdir().unwrap());
    set_scratch_dir(dir.path().to_owned());
}

#[test] // allow(test-attribute)
fn round_trip_swap() {
    set_backend(Backend::Swap);
    let payload: Vec<u64> = (0..1024).collect();
    let mut chunks = [payload.clone()];
    let h = pageout(&mut chunks);
    let mut dst = Vec::new();
    take(h, &mut dst);
    assert_eq!(dst, payload);
}

#[test] // allow(test-attribute)
fn round_trip_file() {
    ensure_scratch();
    set_backend(Backend::File);
    let payload: Vec<u64> = (0..4096).collect();
    let mut chunks = [payload.clone()];
    let h = pageout(&mut chunks);
    let mut dst = Vec::new();
    take(h, &mut dst);
    assert_eq!(dst, payload);
    set_backend(Backend::Swap);
}

#[test] // allow(test-attribute)
fn handle_survives_backend_flip() {
    ensure_scratch();
    set_backend(Backend::File);
    let payload: Vec<u64> = (0..256).collect();
    let mut chunks = [payload.clone()];
    let h: Handle = pageout(&mut chunks);

    // Flip to Swap; existing handle should still be readable as File.
    set_backend(Backend::Swap);

    let mut dst = Vec::new();
    read_at(&h, 0, payload.len(), &mut dst);
    assert_eq!(dst, payload);

    let mut dst2 = Vec::new();
    take(h, &mut dst2);
    assert_eq!(dst2, payload);
}

#[test] // allow(test-attribute)
fn empty_input_yields_zero_len_handle() {
    set_backend(Backend::Swap);
    let mut chunks: [Vec<u64>; 0] = [];
    let h = pageout(&mut chunks);
    assert_eq!(h.len(), 0);
    assert!(h.is_empty());
}

#[test] // allow(test-attribute)
fn scatter_round_trip() {
    set_backend(Backend::Swap);
    let mut chunks = [vec![1u64, 2, 3], vec![4, 5], vec![6, 7, 8, 9]];
    let h = pageout(&mut chunks);
    assert_eq!(h.len(), 9);
    let mut dst = Vec::new();
    take(h, &mut dst);
    assert_eq!(dst, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
}
