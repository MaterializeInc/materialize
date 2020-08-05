// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::time::Instant;

use askama::Template;
use futures::channel::mpsc::UnboundedSender;
use futures::future::{self, FutureExt, TryFutureExt};
use futures::sink::SinkExt;
use hyper::{header, service, Body, Method, Request, Response};
use include_dir::{include_dir, Dir};
use lazy_static::lazy_static;
use openssl::ssl::SslAcceptor;
use prometheus::{
    register_gauge_vec, register_int_gauge_vec, Encoder, Gauge, GaugeVec, IntGauge, IntGaugeVec,
};
use tokio::io::{AsyncRead, AsyncWrite};

use ore::netio::SniffedStream;

#[cfg(not(target_os = "macos"))]
mod prof;
mod util;

#[derive(Template)]
#[template(path = "http/templates/home.html")]
struct HomeTemplate<'a> {
    version: &'a str,
    build_time: &'a str,
    build_sha: &'static str,
}

#[derive(Template)]
#[template(path = "http/templates/status.html")]
struct StatusTemplate<'a> {
    version: &'a str,
    query_count: u64,
    start_time: Instant,
    metrics: Vec<&'a PromMetric<'a>>,
}

const STATIC_DIR: Dir = include_dir!("src/http/static");

lazy_static! {
    static ref SERVER_METADATA_RAW: GaugeVec = register_gauge_vec!(
        "mz_server_metadata_seconds",
        "server metadata, value is uptime",
        &["build_time", "build_version", "build_sha"]
    )
    .expect("can build mz_server_metadata");
    static ref SERVER_METADATA: Gauge = SERVER_METADATA_RAW.with_label_values(&[
        crate::BUILD_TIME,
        crate::VERSION,
        crate::BUILD_SHA,
    ]);
    static ref WORKER_COUNT: IntGaugeVec = register_int_gauge_vec!(
        "mz_server_metadata_timely_worker_threads",
        "number of timely workers materialized is running with",
        &["count"]
    )
    .unwrap();
    static ref REQUEST_TIMES: IntGaugeVec = register_int_gauge_vec!(
        "mz_server_scrape_metrics_times",
        "how long it took to gather metrics, used for very low frequency high accuracy measures",
        &["action"]
    )
    .unwrap();
    static ref REQUEST_METRICS_GATHER: IntGauge = REQUEST_TIMES.with_label_values(&["gather"]);
    static ref REQUEST_METRICS_ENCODE: IntGauge = REQUEST_TIMES.with_label_values(&["encode"]);
}

const METHODS: &[&[u8]] = &[
    b"OPTIONS", b"GET", b"HEAD", b"POST", b"PUT", b"DELETE", b"TRACE", b"CONNECT",
];

const TLS_HANDSHAKE_START: u8 = 22;

fn sniff_tls(buf: &[u8]) -> bool {
    !buf.is_empty() && buf[0] == TLS_HANDSHAKE_START
}

pub struct Server {
    tls: Option<SslAcceptor>,
    cmdq_tx: UnboundedSender<coord::Command>,
    /// When this server started
    start_time: Instant,
}

impl Server {
    pub fn new(
        tls: Option<SslAcceptor>,
        cmdq_tx: UnboundedSender<coord::Command>,
        start_time: Instant,
        worker_count: &str,
    ) -> Server {
        // just set this so it shows up in metrics
        WORKER_COUNT.with_label_values(&[worker_count]).set(1);
        Server {
            tls,
            cmdq_tx,
            start_time,
        }
    }

    pub fn match_handshake(&self, buf: &[u8]) -> bool {
        if self.tls.is_some() && sniff_tls(buf) {
            return true;
        }
        let buf = if let Some(pos) = buf.iter().position(|&b| b == b' ') {
            &buf[..pos]
        } else {
            &buf[..]
        };
        METHODS.contains(&buf)
    }

    pub async fn handle_connection<A>(&self, conn: SniffedStream<A>) -> Result<(), anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    {
        match (&self.tls, sniff_tls(&conn.sniff_buffer())) {
            (Some(tls), true) => {
                let conn = tokio_openssl::accept(tls, conn).await?;
                self.handle_connection_inner(conn).await
            }
            _ => self.handle_connection_inner(conn).await,
        }
    }

    async fn handle_connection_inner<A>(&self, conn: A) -> Result<(), anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let svc = service::service_fn(move |req| match (req.method(), req.uri().path()) {
            (&Method::GET, "/") => self.handle_home(req).boxed(),
            (&Method::GET, "/metrics") => self.handle_prometheus(req).boxed(),
            (&Method::GET, "/status") => self.handle_status(req).boxed(),
            (&Method::GET, "/prof") => self.handle_prof(req).boxed(),
            (&Method::POST, "/prof") => self.handle_prof(req).boxed(),
            (&Method::GET, "/internal/catalog") => self.handle_internal_catalog(req).boxed(),
            _ => self.handle_static(req).boxed(),
        });
        let http = hyper::server::conn::Http::new();
        http.serve_connection(conn, svc).err_into().await
    }

    fn handle_home(
        &self,
        _: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        future::ok(util::template_response(HomeTemplate {
            version: crate::VERSION,
            build_time: crate::BUILD_TIME,
            build_sha: crate::BUILD_SHA,
        }))
    }

    #[cfg(target_os = "macos")]
    fn handle_prof(
        &self,
        _: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        future::ok(Response::builder().status(501).body(Body::from(
            "Profiling is not supported on macOS (HINT: run on Linux with _RJEM_MALLOC_CONF=prof:true)",
        ))?)
    }

    #[cfg(not(target_os = "macos"))]
    fn handle_prof(
        &self,
        req: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        prof::handle(req)
    }

    fn handle_prometheus(
        &self,
        _: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        let metric_families = load_prom_metrics(self.start_time);
        async move {
            let encoder = prometheus::TextEncoder::new();
            let mut buffer = Vec::new();

            let start = Instant::now();
            encoder.encode(&metric_families, &mut buffer)?;
            REQUEST_METRICS_ENCODE.set(Instant::now().duration_since(start).as_micros() as i64);

            Ok(Response::new(Body::from(buffer)))
        }
    }

    fn handle_status(
        &self,
        _: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        let metric_families = load_prom_metrics(self.start_time);

        let desired_metrics = {
            let mut s = BTreeSet::new();
            s.insert("mz_dataflow_events_read_total");
            s.insert("mz_bytes_read_total");
            s.insert("mz_worker_command_queue_size");
            s.insert("mz_command_durations");
            s
        };

        let mut metrics = BTreeMap::new();
        for metric in &metric_families {
            let converted = PromMetric::from_metric_family(metric);
            match converted {
                Ok(m) => {
                    for m in m {
                        match m {
                            PromMetric::Counter { name, .. } => {
                                if desired_metrics.contains(name) {
                                    metrics.insert(name.to_string(), m);
                                }
                            }
                            PromMetric::Gauge { name, .. } => {
                                if desired_metrics.contains(name) {
                                    metrics.insert(name.to_string(), m);
                                }
                            }
                            PromMetric::Histogram {
                                name, ref labels, ..
                            } => {
                                if desired_metrics.contains(name) {
                                    metrics.insert(
                                        format!(
                                            "{}:{}",
                                            name,
                                            labels.get("command").unwrap_or(&"")
                                        ),
                                        m,
                                    );
                                }
                            }
                        }
                    }
                }
                Err(_) => continue,
            };
        }
        let mut query_count = metrics
            .get("mz_command_durations:query")
            .map(|m| {
                if let PromMetric::Histogram { count, .. } = m {
                    *count
                } else {
                    0
                }
            })
            .unwrap_or(0);
        query_count += metrics
            .get("mz_command_durations:execute")
            .map(|m| {
                if let PromMetric::Histogram { count, .. } = m {
                    *count
                } else {
                    0
                }
            })
            .unwrap_or(0);

        future::ok(util::template_response(StatusTemplate {
            version: crate::VERSION,
            query_count,
            start_time: self.start_time,
            metrics: metrics.values().collect(),
        }))
    }

    fn handle_internal_catalog(
        &self,
        _: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let mut cmdq_tx = self.cmdq_tx.clone();
        async move {
            cmdq_tx.send(coord::Command::DumpCatalog { tx }).await?;
            let dump = rx.await?;
            Ok(Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(dump))
                .unwrap())
        }
    }

    fn handle_static(
        &self,
        req: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        if req.method() == &Method::GET {
            let path = req.uri().path();
            let path = path.strip_prefix("/").unwrap_or(path);
            future::ok(match STATIC_DIR.get_file(path) {
                Some(file) => Response::new(Body::from(file.contents())),
                None => Response::builder()
                    .status(404)
                    .body(Body::from("not found"))
                    .unwrap(),
            })
        } else {
            future::ok(
                Response::builder()
                    .status(403)
                    .body(Body::from("bad request"))
                    .unwrap(),
            )
        }
    }
}

/// Call [`prometheus::gather`], ensuring that all our metrics are up to date
fn load_prom_metrics(start_time: Instant) -> Vec<prometheus::proto::MetricFamily> {
    let before_gather = Instant::now();
    let uptime = before_gather - start_time;
    let (secs, milli_part) = (uptime.as_secs() as f64, uptime.subsec_millis() as f64);
    SERVER_METADATA.set(secs + milli_part / 1_000.0);
    let result = prometheus::gather();

    REQUEST_METRICS_GATHER.set(Instant::now().duration_since(before_gather).as_micros() as i64);
    result
}

#[derive(Debug)]
enum PromMetric<'a> {
    Counter {
        name: &'a str,
        value: f64,
        labels: BTreeMap<&'a str, &'a str>,
    },
    Gauge {
        name: &'a str,
        value: f64,
        labels: BTreeMap<&'a str, &'a str>,
    },
    Histogram {
        name: &'a str,
        count: u64,
        labels: BTreeMap<&'a str, &'a str>,
    },
}

impl fmt::Display for PromMetric<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn fmt(
            f: &mut fmt::Formatter,
            name: &str,
            value: impl fmt::Display,
            labels: &BTreeMap<&str, &str>,
        ) -> fmt::Result {
            write!(f, "{} ", name)?;
            for (n, v) in labels.iter() {
                write!(f, "{}={} ", n, v)?;
            }
            writeln!(f, "{}", value)
        }
        match self {
            PromMetric::Counter {
                name,
                value,
                labels,
            } => fmt(f, name, value, labels),
            PromMetric::Gauge {
                name,
                value,
                labels,
            } => fmt(f, name, value, labels),
            PromMetric::Histogram {
                name,
                count,
                labels,
            } => fmt(f, name, count, labels),
        }
    }
}

impl PromMetric<'_> {
    fn from_metric_family<'a>(
        m: &'a prometheus::proto::MetricFamily,
    ) -> Result<Vec<PromMetric<'a>>, ()> {
        use prometheus::proto::MetricType;
        fn l2m(metric: &prometheus::proto::Metric) -> BTreeMap<&str, &str> {
            metric
                .get_label()
                .iter()
                .map(|lp| (lp.get_name(), lp.get_value()))
                .collect()
        }
        m.get_metric()
            .iter()
            .map(|metric| {
                Ok(match m.get_field_type() {
                    MetricType::COUNTER => PromMetric::Counter {
                        name: m.get_name(),
                        value: metric.get_counter().get_value(),
                        labels: l2m(metric),
                    },
                    MetricType::GAUGE => PromMetric::Gauge {
                        name: m.get_name(),
                        value: metric.get_gauge().get_value(),
                        labels: l2m(metric),
                    },
                    MetricType::HISTOGRAM => PromMetric::Histogram {
                        name: m.get_name(),
                        count: metric.get_histogram().get_sample_count(),
                        labels: l2m(metric),
                    },
                    _ => return Err(()),
                })
            })
            .collect()
    }
}
