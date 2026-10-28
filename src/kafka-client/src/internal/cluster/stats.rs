// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Periodic statistics (`statistics.interval.ms`).
//!
//! The cluster fills the client and broker parts of [`Statistics`]; engines
//! registered with [`Cluster::register_stats_source`] fill the rest (message
//! queue counts, produced message totals, topics, `eos`, `cgrp`).

use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use crate::config::settings::ClientKind;
use crate::statistics::{self, Statistics, Window};

use super::Cluster;

/// A source of engine-level statistics, e.g. the producer.
pub(crate) trait StatsSource: Send + Sync {
    /// Fills the fields this source owns, such as `msg_cnt`, `msg_size`,
    /// `msg_max`, `msg_size_max`, `txmsgs`, `txmsg_bytes`, `rxmsgs`,
    /// `rxmsg_bytes`, `topics`, `eos` or `cgrp`. Called on a `kafka-io`
    /// thread; must not block.
    fn fill(&self, stats: &mut Statistics);
}

fn micros(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

fn counter(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Builds the statistics of the cluster and its registered sources.
pub(crate) fn build(cluster: &Cluster) -> Statistics {
    let settings = cluster.settings();
    let age = cluster.created().elapsed();
    let mut stats = Statistics {
        name: cluster.handle_name().to_string(),
        client_id: settings.client_id.clone(),
        client_type: match settings.kind {
            ClientKind::Consumer => "consumer".into(),
            ClientKind::Producer | ClientKind::Admin => "producer".into(),
        },
        ts: micros(age),
        time: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_secs()).ok())
            .unwrap_or(0),
        age: micros(age),
        ..Statistics::default()
    };
    if settings.kind != ClientKind::Consumer {
        stats.msg_max = settings
            .producer
            .queue_buffering_max_messages
            .map_or(0, u64::from);
        stats.msg_size_max = settings.producer.queue_buffering_max_bytes;
    }
    for broker in cluster.brokers() {
        let c = broker.counters();
        let b = statistics::Broker {
            name: broker.name(),
            nodeid: broker.id(),
            nodename: broker.nodename(),
            source: broker.source().name().to_string(),
            state: broker.state().name().to_string(),
            stateage: micros(broker.state_age()),
            outbuf_cnt: c.outbuf_cnt.load(Ordering::Relaxed).max(0),
            outbuf_msg_cnt: c.outbuf_msg_cnt.load(Ordering::Relaxed).max(0),
            waitresp_cnt: c.waitresp_cnt.load(Ordering::Relaxed).max(0),
            waitresp_msg_cnt: c.waitresp_msg_cnt.load(Ordering::Relaxed).max(0),
            tx: c.tx.load(Ordering::Relaxed),
            txbytes: c.tx_bytes.load(Ordering::Relaxed),
            txerrs: c.txerrs.load(Ordering::Relaxed),
            txretries: c.txretries.load(Ordering::Relaxed),
            txidle: -1,
            req_timeouts: c.req_timeouts.load(Ordering::Relaxed),
            rx: c.rx.load(Ordering::Relaxed),
            rxbytes: c.rx_bytes.load(Ordering::Relaxed),
            rxerrs: c.rxerrs.load(Ordering::Relaxed),
            rxidle: -1,
            wakeups: Some(0),
            connects: Some(c.connects.load(Ordering::Relaxed)),
            disconnects: Some(c.disconnects.load(Ordering::Relaxed)),
            int_latency: Some(Window::default()),
            outbuf_latency: Some(Window::default()),
            rtt: Some(c.take_rtt()),
            throttle: Some(Window::default()),
            ..statistics::Broker::default()
        };
        stats.tx = stats.tx.saturating_add(counter(b.tx));
        stats.tx_bytes = stats.tx_bytes.saturating_add(counter(b.txbytes));
        stats.rx = stats.rx.saturating_add(counter(b.rx));
        stats.rx_bytes = stats.rx_bytes.saturating_add(counter(b.rxbytes));
        stats.brokers.insert(b.name.clone(), b);
    }
    stats.metadata_cache_cnt = i64::try_from(cluster.metadata().topics.len()).unwrap_or(i64::MAX);
    for source in cluster.stats_sources() {
        source.fill(&mut stats);
    }
    stats
}

/// Emits statistics to the stats hook every `interval`, the first one
/// interval after start.
pub(super) async fn stats_loop(
    cluster: Weak<Cluster>,
    shutdown: CancellationToken,
    interval: Duration,
) {
    let mut next = Instant::now() + interval;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep_until(next.into()) => {}
        }
        next += interval;
        let Some(cluster) = cluster.upgrade() else {
            return;
        };
        let stats = build(&cluster);
        cluster.events().stats(stats);
    }
}
