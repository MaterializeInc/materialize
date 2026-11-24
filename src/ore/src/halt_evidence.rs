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

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_FRAME_BYTES: usize = 4096;
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) fn record(code: i32, location: &str, message: &str) {
    let seq = SEQUENCE.load(Ordering::Relaxed);
    let reserved = seq.checked_add(1).is_some_and(|next| {
        SEQUENCE
            .compare_exchange(seq, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    });
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH);
    let nanos = stamp.as_ref().map_or(0, |stamp| stamp.as_nanos());
    let invalid = if !reserved {
        Some("sequence")
    } else if stamp.is_err() {
        Some("clock")
    } else {
        None
    };
    let frame = frame(seq, code, nanos, location, message, invalid);
    let path = std::env::var_os("MZ_ANTITHESIS_TERMINATION_LOG")
        .unwrap_or_else(|| "/dev/termination-log".into());
    let Ok(mut file) = OpenOptions::new()
        .append(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
    else {
        return;
    };
    if !file.metadata().is_ok_and(|metadata| metadata.is_file()) {
        return;
    }
    // One append keeps concurrent records separate. A short write leaves an
    // incomplete frame that the consumer rejects. Never retry while halting.
    let _ = file.write(frame.as_bytes());
}

fn frame(
    seq: u64,
    code: i32,
    nanos: u128,
    location: &str,
    message: &str,
    invalid: Option<&str>,
) -> String {
    let mut frame = format!(
        "MZHALT1 {seq} {code} {nanos} {} {} ",
        location.len(),
        message.len()
    );
    let size = location
        .len()
        .checked_add(message.len())
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| n.checked_add(frame.len() + 6));
    // Kubelet reads the last 4096 bytes. An exact-cap frame could hide an
    // earlier conflicting append, so accepted frames must leave room.
    let invalid = invalid.or_else(|| {
        size.is_none_or(|n| n >= MAX_FRAME_BYTES)
            .then_some("oversize")
    });
    if let Some(reason) = invalid {
        frame = format!("MZHALT1 {seq} {code} {nanos} INVALID {reason} END\n");
    } else {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for (index, value) in [location, message].into_iter().enumerate() {
            if index != 0 {
                frame.push(' ');
            }
            for byte in value.bytes() {
                frame.push(char::from(HEX[usize::from(byte >> 4)]));
                frame.push(char::from(HEX[usize::from(byte & 0xf)]));
            }
        }
        frame.push_str(" END\n");
    }
    frame
}

#[cfg(test)]
mod tests {
    use super::frame;

    #[test] // allow(test-attribute): compiled independently in the upgrade-base probe.
    fn a_complete_frame_at_the_kubelet_tail_limit_is_ambiguous() {
        let accepted = frame(0, 166, 1, "x", &"x".repeat(2032), None);
        assert_eq!(accepted.len(), 4095);
        assert!(!accepted.contains(" INVALID "));
        let rejected = frame(0, 166, 1, "0123456789", &"x".repeat(2023), None);
        assert!(rejected.ends_with(" INVALID oversize END\n"), "{rejected}");
    }
}
