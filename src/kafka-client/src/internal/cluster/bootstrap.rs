// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `bootstrap.servers` parsing and reconnect backoff.

use std::time::{Duration, Instant};

use rand::RngExt;

use crate::config::settings::SecurityProtocol;

/// The default broker port.
pub(crate) const DEFAULT_PORT: u16 = 9092;

/// One bootstrap server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BootstrapServer {
    /// The host as written, with IPv6 brackets: used in the broker name.
    pub host: String,
    pub port: u16,
}

impl BootstrapServer {
    /// The host for resolution and TLS: without IPv6 brackets.
    pub(crate) fn bare_host(&self) -> &str {
        bare_host(&self.host)
    }
}

/// Strips IPv6 brackets.
pub(crate) fn bare_host(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// librdkafka's nodename: `host:port`, with IPv6 hosts in brackets.
pub(crate) fn nodename(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// librdkafka's broker name: `[proto://]nodename/<id|bootstrap>`, without
/// the protocol prefix for plaintext.
pub(crate) fn broker_name(
    protocol: SecurityProtocol,
    host: &str,
    port: u16,
    id: Option<i32>,
) -> String {
    let proto = match protocol {
        SecurityProtocol::Plaintext => String::new(),
        p => format!("{}://", p.name()),
    };
    let id = match id {
        Some(id) => id.to_string(),
        None => "bootstrap".to_string(),
    };
    format!("{proto}{}/{id}", nodename(host, port))
}

/// Parses one `bootstrap.servers` entry like librdkafka's
/// `rd_kafka_broker_name_parse`: `[proto://]host[:port][/path]`, default
/// port 9092, bracketed IPv6, empty host meaning `localhost`.
///
/// Fails with librdkafka's `BROKER` warning text when the protocol prefix is
/// unknown or does not match `security.protocol`.
pub(crate) fn parse_server(
    entry: &str,
    protocol: SecurityProtocol,
) -> Result<BootstrapServer, String> {
    let mut s = entry;
    if let Some(idx) = s.find("://") {
        let proto = &s[..idx];
        if proto.is_empty() {
            return Err(format!(
                "Broker name \"{entry}\" parse error: empty protocol name"
            ));
        }
        let known = [
            SecurityProtocol::Plaintext,
            SecurityProtocol::Ssl,
            SecurityProtocol::SaslPlaintext,
            SecurityProtocol::SaslSsl,
        ];
        let Some(p) = known.iter().find(|p| p.name().eq_ignore_ascii_case(proto)) else {
            return Err(format!(
                "Broker name \"{entry}\" parse error: unsupported protocol \"{}\"",
                proto.to_ascii_uppercase()
            ));
        };
        if *p != protocol {
            return Err(format!(
                "Broker name \"{entry}\" parse error: protocol \"{}\" does not match \
                 security.protocol setting \"{}\"",
                proto.to_ascii_uppercase(),
                protocol.name().to_ascii_uppercase()
            ));
        }
        s = &s[idx + 3..];
        if let Some(slash) = s.find('/') {
            s = &s[..slash];
        }
    }
    let mut host = s;
    let mut port = DEFAULT_PORT;
    if let Some(last) = s.rfind(':') {
        let first = s.find(':').expect("found last");
        // One colon: host:port. Several: IPv6, with a port only after `]`.
        if first == last || s[..last].ends_with(']') {
            host = &s[..last];
            port = atoi_u16(&s[last + 1..]);
        }
    }
    if host.is_empty() {
        host = "localhost";
    }
    Ok(BootstrapServer {
        host: host.to_string(),
        port,
    })
}

/// C `atoi` truncated to `u16`, as librdkafka assigns it to a `uint16_t`.
fn atoi_u16(s: &str) -> u16 {
    let s = s.trim_start();
    let (neg, digits) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let mut v: i64 = 0;
    for c in digits.chars() {
        let Some(d) = c.to_digit(10) else { break };
        v = v.saturating_mul(10).saturating_add(i64::from(d));
    }
    if neg {
        v = -v;
    }
    let low = v.rem_euclid(65536);
    u16::try_from(low).expect("in range")
}

/// Reconnect backoff like librdkafka's
/// `rd_kafka_broker_update_reconnect_backoff`: computed at each connection
/// attempt, `reconnect.backoff.ms` doubling up to `reconnect.backoff.max.ms`
/// with -25%..+50% jitter, reset when the previous attempt is longer than the
/// maximum ago.
#[derive(Debug)]
pub(crate) struct Backoff {
    initial: Duration,
    max: Duration,
    current: Duration,
    /// The earliest time of the next attempt; `None` before the first.
    next_attempt: Option<Instant>,
}

impl Backoff {
    pub(crate) fn new(initial: Duration, max: Duration) -> Backoff {
        Backoff {
            initial,
            max,
            current: initial,
            next_attempt: None,
        }
    }

    /// When the next attempt may start; `None` means now.
    pub(crate) fn next_attempt(&self) -> Option<Instant> {
        self.next_attempt.filter(|at| *at > Instant::now())
    }

    /// Records an attempt starting at `now` and schedules the next one.
    pub(crate) fn on_attempt(&mut self, now: Instant) {
        let jitter = |lo: Duration, hi: Duration| -> Duration {
            let lo = u64::try_from(lo.as_millis()).unwrap_or(u64::MAX);
            let hi = u64::try_from(hi.as_millis()).unwrap_or(u64::MAX).max(lo);
            Duration::from_millis(rand::rng().random_range(lo..=hi))
        };
        self.on_attempt_with(now, jitter)
    }

    fn on_attempt_with(
        &mut self,
        now: Instant,
        jitter: impl FnOnce(Duration, Duration) -> Duration,
    ) {
        if let Some(prev) = self.next_attempt {
            if prev + self.max < now {
                self.current = self.initial;
            }
        }
        let delay = jitter(self.current.mul_f64(0.75), self.current.mul_f64(1.5)).min(self.max);
        self.next_attempt = Some(now + delay);
        self.current = (self.current * 2).min(self.max);
    }

    /// Lets the next attempt start immediately (e.g. after a broker's address
    /// changed), as librdkafka does when expediting a reconnect.
    pub(crate) fn expedite(&mut self) {
        self.next_attempt = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(entry: &str) -> Result<(String, u16), String> {
        parse_server(entry, SecurityProtocol::Plaintext).map(|s| (s.host, s.port))
    }

    #[mz_ore::test]
    fn parses_servers() {
        assert_eq!(p("kafka"), Ok(("kafka".into(), 9092)));
        assert_eq!(p("kafka:9093"), Ok(("kafka".into(), 9093)));
        assert_eq!(p("1.2.3.4:1"), Ok(("1.2.3.4".into(), 1)));
        assert_eq!(p(":9093"), Ok(("localhost".into(), 9093)));
        assert_eq!(p(""), Ok(("localhost".into(), 9092)));
        assert_eq!(p("[::1]:9094"), Ok(("[::1]".into(), 9094)));
        assert_eq!(p("::1"), Ok(("::1".into(), 9092)));
        assert_eq!(p("[::1]"), Ok(("[::1]".into(), 9092)));
        assert_eq!(p("plaintext://h:1/path"), Ok(("h".into(), 1)));
        assert_eq!(p("PLAINTEXT://h"), Ok(("h".into(), 9092)));
        assert_eq!(p("h:notaport"), Ok(("h".into(), 0)));
        assert_eq!(p("h:70000"), Ok(("h".into(), 4464)));
        assert_eq!(
            p("ssl://h:1"),
            Err(
                "Broker name \"ssl://h:1\" parse error: protocol \"SSL\" does not match \
                 security.protocol setting \"PLAINTEXT\""
                    .into()
            )
        );
        assert_eq!(
            p("foo://h:1"),
            Err("Broker name \"foo://h:1\" parse error: unsupported protocol \"FOO\"".into())
        );
        assert_eq!(
            p("://h"),
            Err("Broker name \"://h\" parse error: empty protocol name".into())
        );
        let s = parse_server("sasl_ssl://b:9096", SecurityProtocol::SaslSsl).unwrap();
        assert_eq!(s.bare_host(), "b");
        assert_eq!(
            parse_server("[fe80::1]:1", SecurityProtocol::Ssl)
                .unwrap()
                .bare_host(),
            "fe80::1"
        );
    }

    #[mz_ore::test]
    fn names() {
        assert_eq!(
            broker_name(SecurityProtocol::Plaintext, "localhost", 9092, None),
            "localhost:9092/bootstrap"
        );
        assert_eq!(
            broker_name(SecurityProtocol::SaslSsl, "b-1.example", 9096, Some(3)),
            "sasl_ssl://b-1.example:9096/3"
        );
        assert_eq!(
            broker_name(SecurityProtocol::Ssl, "::1", 9092, Some(1)),
            "ssl://[::1]:9092/1"
        );
        assert_eq!(nodename("[::1]", 1), "[::1]:1");
    }

    #[mz_ore::test]
    fn backoff_doubles_with_jitter_and_resets() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_millis(1000));
        assert!(b.next_attempt().is_none());
        let t0 = Instant::now();
        let mut ranges = vec![];
        let mut now = t0;
        for _ in 0..6 {
            b.on_attempt_with(now, |lo, hi| {
                ranges.push((lo.as_millis(), hi.as_millis()));
                hi
            });
            now = b.next_attempt.unwrap();
        }
        assert_eq!(
            ranges,
            [
                (75, 150),
                (150, 300),
                (300, 600),
                (600, 1200),
                (750, 1500),
                (750, 1500)
            ]
        );
        // An attempt long after the previous one resets the backoff.
        let later = now + Duration::from_secs(10);
        let mut seen = None;
        b.on_attempt_with(later, |lo, hi| {
            seen = Some((lo.as_millis(), hi.as_millis()));
            lo
        });
        assert_eq!(seen, Some((75, 150)));
        assert_eq!(b.next_attempt.unwrap() - later, Duration::from_millis(75));
        // Real jitter stays in range.
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_millis(1000));
        let now = Instant::now();
        b.on_attempt(now);
        let d = b.next_attempt.unwrap() - now;
        assert!(
            d >= Duration::from_millis(75) && d <= Duration::from_millis(150),
            "{d:?}"
        );
        b.expedite();
        assert!(b.next_attempt().is_none());
    }
}
