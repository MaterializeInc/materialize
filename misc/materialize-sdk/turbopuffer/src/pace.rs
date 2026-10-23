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

//! Pacing embedding requests to turbopuffer's per-minute limits, so that a
//! long snapshot waits for room instead of collecting HTTP 429s.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

const WINDOW: Duration = Duration::from_secs(60);

/// A bound on the tokens one text is billed. turbopuffer cuts a text at the
/// model's context without an error, and billed 26,213 tokens for 300,000
/// bytes in tests. Models with a longer context can bill more.
pub(crate) const MAX_TOKENS_PER_TEXT: u64 = 32_768;

/// The tokens a text of `bytes` is estimated to cost. English text in tests
/// billed between a fifth and a third of a token per byte.
pub(crate) fn estimate_tokens(bytes: usize) -> u64 {
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    (bytes / 3).saturating_add(1).min(MAX_TOKENS_PER_TEXT)
}

/// The embedding requests and tokens of the last minute, and how many tokens
/// one request may carry.
#[derive(Debug)]
pub(crate) struct Pacer {
    tokens_per_minute: u64,
    requests_per_minute: usize,
    /// The token budget of one request, halved after each refusal, because
    /// turbopuffer refuses a request over the minute's limit every time and
    /// estimates can fall short.
    request_tokens: u64,
    sent: VecDeque<Sent>,
    next: u64,
}

#[derive(Debug)]
struct Sent {
    at: Instant,
    tokens: u64,
    id: u64,
}

/// A request counted in the window before it is sent, so that one whose
/// response never arrives still counts.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Reservation(u64);

impl Pacer {
    pub(crate) fn new(tokens_per_minute: u64, requests_per_minute: usize) -> Pacer {
        Pacer {
            tokens_per_minute,
            requests_per_minute,
            request_tokens: tokens_per_minute,
            sent: VecDeque::new(),
            next: 0,
        }
    }

    pub(crate) fn tokens_per_minute(&self) -> u64 {
        self.tokens_per_minute
    }

    pub(crate) fn request_tokens(&self) -> u64 {
        self.request_tokens
    }

    /// Counts a request of `estimate` tokens at `now`, or returns how long to
    /// wait for room. A request alone in the window always goes.
    pub(crate) fn reserve(&mut self, now: Instant, estimate: u64) -> Result<Reservation, Duration> {
        while self
            .sent
            .front()
            .is_some_and(|sent| now.saturating_duration_since(sent.at) >= WINDOW)
        {
            self.sent.pop_front();
        }
        if let Some(oldest) = self.sent.front() {
            let used: u64 = self.sent.iter().map(|sent| sent.tokens).sum();
            if used.saturating_add(estimate) > self.tokens_per_minute
                || self.sent.len() >= self.requests_per_minute
            {
                return Err(WINDOW.saturating_sub(now.saturating_duration_since(oldest.at)));
            }
        }
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        self.sent.push_back(Sent {
            at: now,
            tokens: estimate,
            id,
        });
        Ok(Reservation(id))
    }

    /// Replaces a reservation's estimate with the tokens turbopuffer billed,
    /// and lets later requests carry more again.
    pub(crate) fn applied(&mut self, reservation: Reservation, tokens: u64) {
        if let Some(sent) = self.sent.iter_mut().find(|sent| sent.id == reservation.0) {
            sent.tokens = tokens;
        }
        self.request_tokens = self
            .request_tokens
            .saturating_mul(2)
            .min(self.tokens_per_minute);
    }

    /// Gives back a reservation whose request turbopuffer refused as invalid,
    /// which it embedded nothing for.
    pub(crate) fn returned(&mut self, reservation: Reservation) {
        if let Some(sent) = self.sent.iter_mut().find(|sent| sent.id == reservation.0) {
            sent.tokens = 0;
        }
    }

    /// Records that turbopuffer refused a reservation's request for its rate
    /// limit, so it embedded nothing, and halves the next requests.
    pub(crate) fn refused(&mut self, reservation: Reservation) {
        if let Some(sent) = self.sent.iter_mut().find(|sent| sent.id == reservation.0) {
            sent.tokens = 0;
        }
        self.request_tokens = (self.request_tokens / 2).max(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_wait_for_room_in_the_last_minute() {
        let start = Instant::now();
        let at = |seconds| start + Duration::from_secs(seconds);
        let mut pacer = Pacer::new(100, 3);
        let first = pacer.reserve(at(0), 500).expect("alone in the window");
        pacer.applied(first, 60);
        let second = pacer.reserve(at(10), 40).unwrap();
        pacer.applied(second, 40);
        assert_eq!(
            pacer.reserve(at(20), 1).unwrap_err(),
            Duration::from_secs(40)
        );
        // The first request leaves the window after a minute.
        let third = pacer.reserve(at(60), 50).unwrap();
        pacer.applied(third, 10);
        pacer.reserve(at(61), 0).unwrap();
        assert_eq!(
            pacer.reserve(at(62), 0).unwrap_err(),
            Duration::from_secs(8),
            "three requests in the window"
        );
    }

    #[test]
    fn a_reservation_counts_until_turbopuffer_reports() {
        let start = Instant::now();
        let mut pacer = Pacer::new(100, 10);
        // A request whose response never arrives keeps its estimate.
        let _lost = pacer.reserve(start, 80).unwrap();
        assert!(pacer.reserve(start, 30).is_err());
        let mut pacer = Pacer::new(100, 10);
        let refused = pacer.reserve(start, 80).unwrap();
        pacer.refused(refused);
        assert!(pacer.reserve(start, 30).is_ok());
    }

    #[test]
    fn refusals_halve_the_request_budget_and_applied_requests_restore_it() {
        let mut pacer = Pacer::new(100, 10);
        let now = Instant::now();
        for expected in [50, 25, 12] {
            let reservation = pacer.reserve(now, 0).unwrap();
            pacer.refused(reservation);
            assert_eq!(pacer.request_tokens(), expected);
        }
        for expected in [24, 48, 96, 100] {
            let reservation = pacer.reserve(now, 0).unwrap();
            pacer.applied(reservation, 0);
            assert_eq!(pacer.request_tokens(), expected);
        }
    }

    #[test]
    fn estimates_are_capped_at_what_a_model_reads() {
        assert_eq!(estimate_tokens(0), 1);
        assert_eq!(estimate_tokens(6000), 2001);
        assert_eq!(estimate_tokens(10 << 20), MAX_TOKENS_PER_TEXT);
    }
}
