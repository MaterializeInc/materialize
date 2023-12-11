// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use reqwest::Method;
use serde::{Serialize, Deserialize};

use crate::client::Client;
use crate::error::Error;

/// Represents the metric endpoint params.
#[derive(Serialize)]
pub struct SubmitMetricsParams<'a> {
    /// A list of timeseries to submit to Datadog.
    pub series: Vec<Series<'a>>,
}

/// Represents a single timeserie metric.
#[derive(Serialize)]
pub struct Series<'a> {
    /// The name of the timeseries.
    pub metric: &'a str,
    /// Points relating to a metric.
    pub points: Vec<Point>,
    /// If the type of the metric is rate or count, define the corresponding interval.
    pub interval: Option<i64>,
    /// Metadata for the metric
    pub metadata: Option<Metadata>,
    /// A list of resources to associate with this metric
    pub resources: Option<Vec<Resource<'a>>>,
    /// The source type name.
    pub source_type_name: &'a str,
    /// A list of tags associated with the metric
    pub tags: Option<Vec<&'a str>>,
    /// The type of metric.
    #[serde(rename = "type")]
    pub typ: MetricType,
    /// The unit of point value
    pub unit: Option<&'a str>,
}

/// Represents a single point value in time.
#[derive(Serialize)]
pub struct Point {
    /// The timestamp in seconds.
    pub timestamp: i64,
    /// The numeric value in 64bit float.
    pub value: f64,
}

/// Represents metric's metadata.
#[derive(Serialize)]
pub struct Metadata {
    /// Metric origin information
    origin: Origin
}

/// Represents metric's origin.
#[derive(Serialize)]
pub struct Origin {
    /// The origin metric type code
    pub metric_type: i32,
    /// The origin product code
    pub product: i32,
    /// The origin service code
    pub service: i32,
}

/// Represents metric's resource.
#[derive(Serialize)]
pub struct Resource<'a> {
    /// The name of the resource.
    pub name: &'a str,
    /// The type of the resource.
    #[serde(rename = "type")]
    pub typ: &'a str,
}

#[derive(Serialize)]
/// Represents the mutliple metric types accepted by Datadog.
pub enum MetricType {
    /// Represents the unspecified metric type.
    Unspecified = 0,
    /// Represents the [count metric type](https://docs.datadoghq.com/metrics/types/?tab=count#metric-types).
    Count = 1,
    /// Represents the [rate metric type](https://docs.datadoghq.com/metrics/types/?tab=rate#metric-types).
    Rate = 2,
    /// Represents the [gauge metric type.](https://docs.datadoghq.com/metrics/types/?tab=gauge#metric-types)
    Gauge = 3,
}

impl<'a> Default for Series<'a> {
    fn default() -> Self {
      Self {
        interval: None,
        metadata: None,
        resources: None,
        tags: None,
        unit: None,
        source_type_name: "default",
        metric: "default",
        points: vec![],
        typ: MetricType::Gauge,
      }
    }
  }


/// Represents Datadog response
///
/// Example for a `202``:
///
/// ```json
/// {
///  "errors": []
/// }
/// ```
///
/// Example for a `404`:
///
/// ```json
/// {
///  "errors": [
///    "Bad Request"
///  ]
/// }
/// ```
#[derive(Deserialize)]
pub struct SubmitMetricsResponse {
    /// Errors returned by Datadog.
    pub errors: Vec<String>
}

impl<'a> Client<'a> {
    /// Submits a metric to Datadog.
    ///
    /// [From Datadog's documentation:](https://docs.datadoghq.com/api/latest/metrics/#submit-metrics)
    ///
    /// The maximum payload size is 500 kilobytes (512000 bytes). Compressed payloads must have a decompressed size of less than 5 megabytes (5242880 bytes).
    ///
    /// If you’re submitting metrics directly to the Datadog API without using DogStatsD, expect:
    /// - 64 bits for the timestamp
    ///
    /// - 64 bits for the value
    ///
    /// - 20 bytes for the metric names
    ///
    /// - 50 bytes for the timeseries
    ///
    /// - The full payload is approximately 100 bytes.
    ///
    pub async fn submit_metric(&self, params: SubmitMetricsParams<'a>) -> Result<SubmitMetricsResponse, Error> {
        let req = self.build_request(Method::GET, ["v2", "series"]).await?;
        let req = req.json(&params);

        Ok(self.send_request(req).await?)
    }
}
