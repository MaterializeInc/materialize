// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Report rusage metrics.

use mz_ore::metrics::MetricsRegistry;
use prometheus::IntGauge;
use std::time::Duration;

macro_rules! metrics {
    ($namespace:ident $(($name:ident, $desc:expr)),*) => {
        metrics! { @define $namespace $(($name, $desc)),*}
    };
    (@define $namespace:ident $(($name:ident, $desc:expr)),*) => {
        struct RuMetrics {
            $($name: IntGauge,)*
        }
        impl RuMetrics {
            fn new(registry: &MetricsRegistry) -> Self {
                Self {
                    $($name: registry.register(mz_ore::metric!(
                        name: concat!(stringify!($namespace), "_", stringify!($name)),
                        help: $desc,
                    )),)*
                }
            }
            fn update(&self) {
                let rusage = unsafe {
                    let mut rusage = std::mem::zeroed();
                    libc::getrusage(libc::RUSAGE_SELF, &mut rusage);
                    rusage
                };
                $(self.$name.set(rusage.$name);)*
            }
        }
    };
}

metrics! {
    mz_metrics_libc
    (ru_maxrss, "maximum resident set size"),
    (ru_ixrss, "integral shared memory size"),
    (ru_idrss, "integral unshared data size"),
    (ru_isrss, "integral unshared stack size"),
    (ru_minflt, "page reclaims (soft page faults)"),
    (ru_majflt, "page faults (hard page faults)"),
    (ru_nswap, "swaps"),
    (ru_inblock, "block input operations"),
    (ru_oublock, "block output operations"),
    (ru_msgsnd, "IPC messages sent"),
    (ru_msgrcv, "IPC messages received"),
    (ru_nsignals, "signals received"),
    (ru_nvcsw, "voluntary context switches"),
    (ru_nivcsw, "involuntary context switches")
}

/// Register a task to read rusage stats.
#[allow(clippy::unused_async)]
pub async fn register_metrics_into(metrics_registry: &MetricsRegistry) {
    let rusage = RuMetrics::new(metrics_registry);

    mz_ore::task::spawn(|| "rusage_stats_update", async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            rusage.update();
        }
    });
}
