// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Actions related to verifying and inspecting prometheus metrics

use core::fmt;
use std::collections::BTreeMap;
use std::fmt::Write;

use async_trait::async_trait;
use reqwest::Client;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

#[derive(Debug)]
pub struct VerifyMetricsAction {
    metric_name: String,
    labels: Option<BTreeMap<String, Option<String>>>,
    value_eq: Option<f64>,
    value_gt: Option<f64>,
}

pub fn build_verify(mut cmd: BuiltinCommand) -> Result<VerifyMetricsAction, String> {
    let metric_name = cmd.args.string("metric-name")?;
    let labels = cmd.args.opt_string("labels").map(|ls| {
        ls.split(',')
            .map(|s| {
                let mut key_val = s.split('=');
                let key = key_val
                    .next()
                    .expect("there should always be at least one string on split")
                    .to_string();
                let val = key_val.next().map(|v| v.to_string());
                (key, val)
            })
            .collect()
    });
    let mut opt_arg = |name| {
        cmd.args
            .opt_string(name)
            .map(|v| fast_float::parse(v))
            .transpose()
            .map_err(|e| e.to_string())
    };
    let value_eq = opt_arg("value-eq")?;
    let value_gt = opt_arg("value-gt")?;

    cmd.args.done()?;

    Ok(VerifyMetricsAction {
        metric_name,
        labels,
        value_eq,
        value_gt,
    })
}

#[async_trait]
impl Action for VerifyMetricsAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let client = Client::default();
        let resp = client
            .get(format!("http://{}/metrics", state.materialized_addr))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let text = resp.text().await.map_err(|e| e.to_string())?;

        let metrics = parse_metrics(&text)?;

        let possible_metrics = metrics
            .inner
            .get(&self.metric_name)
            .ok_or_else(|| format!("Metric not present in prometheus: {}", self.metric_name))?;
        if let Some(labels) = &self.labels {
            let metrics: Vec<Metric> = possible_metrics
                .into_iter()
                .filter(|m| {
                    for (expected_k, expected_v) in labels {
                        if let Some(actual_value) = m.labels.get(&*expected_k) {
                            if let Some(expected_value) = expected_v {
                                return expected_value == actual_value;
                            }
                        } else {
                            return false;
                        }
                    }
                    true
                })
                .cloned()
                .collect();

            if !metrics.is_empty() {
                self.verify_metrics(&*metrics)?;
            } else {
                return Err(format!(
                    "Metric with labels does not exist {}{{{}}}",
                    self.metric_name,
                    labels
                        .into_iter()
                        .map(|(k, v)| if let Some(v) = v {
                            format!("{}={}", k, v)
                        } else {
                            k.clone()
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
        } else {
            self.verify_metrics(&**possible_metrics)?;
        }

        Ok(())
    }
}

impl VerifyMetricsAction {
    fn verify_metrics(&self, metrics: &[Metric]) -> Result<(), String> {
        for metric in metrics {
            let ok = if let Some(val) = self.value_eq {
                // this should only be used for integer literals that have had no math applied to
                // them, it's just that prometheus treats all values as floats
                #[allow(clippy::float_cmp)]
                if val == metric.value {
                    Some(format!("== {}", val))
                } else {
                    None
                }
            } else if let Some(val) = self.value_gt {
                if metric.value > val {
                    Some(format!("> {}", val))
                } else {
                    None
                }
            } else {
                Some(String::new())
            };

            if let Some(op) = ok {
                print!("prometheus: {}{}", self.metric_name, metric);
                if !op.is_empty() {
                    print!(" is {}", op)
                }
                println!();
            } else {
                return Err(format!(
                    "prometheus: {} {} does not satisfy value constraint",
                    self.metric_name, metric
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Metrics {
    inner: BTreeMap<String, Vec<Metric>>,
}

#[derive(Clone, Debug)]
struct Metric {
    labels: BTreeMap<String, String>,
    value: f64,
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('{')?;
        let mut is_first = true;
        for (k, v) in &self.labels {
            if !is_first {
                f.write_char(',')?;
            } else {
                is_first = false;
            }
            f.write_str(k)?;
            f.write_str("=\"")?;
            f.write_str(v)?;
            f.write_char('"')?;
        }
        f.write_str("} ")?;
        write!(f, "{}", self.value)?;

        Ok(())
    }
}

fn parse_metrics(metrics: &str) -> Result<Metrics, String> {
    let mut inner = BTreeMap::new();
    for line in metrics.lines() {
        if line.trim().starts_with('#') || line.trim().is_empty() {
            continue;
        }

        let byteiter = &mut line.as_bytes().iter().cloned();
        let mut is_labelled = true;
        let metric_name = String::from_utf8(
            byteiter
                .take_while(|c| match *c {
                    b'{' => {
                        is_labelled = true;
                        false
                    }
                    b' ' => {
                        is_labelled = false;
                        false
                    }
                    _ => true,
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();

        let labels = if is_labelled {
            get_labels(byteiter)
        } else {
            BTreeMap::new()
        };

        let raw_value = String::from_utf8(byteiter.collect::<Vec<_>>()).unwrap();
        let value = fast_float::parse(raw_value.trim()).map_err(|e| e.to_string())?;

        let metric = Metric { labels, value };

        inner
            .entry(metric_name)
            .or_insert_with(Vec::new)
            .push(metric);
    }

    Ok(Metrics { inner })
}

fn get_labels(iter: &mut impl Iterator<Item = u8>) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();

    let iter = &mut iter.peekable();
    while !&[Some(&b'}'), Some(&b' ')].contains(&iter.peek()) {
        if iter.peek() == Some(&b',') {
            iter.next();
        }
        let key = String::from_utf8(iter.take_while(|c| *c != b'=').collect::<Vec<_>>()).unwrap();

        assert_eq!(iter.next(), Some(b'"'));

        let value = String::from_utf8(iter.take_while(|c| *c != b'"').collect::<Vec<_>>()).unwrap();
        labels.insert(key, value);
    }

    assert!(&[Some(b'}'), Some(b' ')].contains(&iter.next()));

    labels
}
