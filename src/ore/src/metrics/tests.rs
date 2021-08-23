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

use super::*;

#[test]
fn metrics_registry() {
    let reg = MetricsRegistry::new();
    let counter: UIntCounter = reg.register(metric!(
        name: "test_counter",
        help: "a counter for testing"
    ));
    counter.inc();

    let readings = reg.gather();
    assert_eq!(readings.len(), 1);
}

#[test]
fn anon_metrics_registry() {
    let reg = MetricsRegistry::new();
    let counter_anon: ThirdPartyMetric<UIntCounter> = reg.register_third_party_visible(metric!(
        name: "test_counter_third_party",
        help: "an third_party counter for testing"
    ));
    let counter_reg: UIntCounter = reg.register(metric!(
        name: "test_counter_normal",
        help: "a regular counter for testing"
    ));
    counter_anon.inc();
    counter_reg.inc();

    let readings = reg.gather();
    assert_eq!(readings.len(), 2);

    let readings_anon = reg.gather_third_party_visible();
    assert_eq!(readings_anon.len(), 1);
    assert_eq!(readings_anon[0].get_name(), "test_counter_third_party");
}

#[test]
fn thirdparty_metric_vecs() {
    let reg = MetricsRegistry::new();
    let cv: ThirdPartyMetric<raw::UIntCounterVec> = reg.register_third_party_visible(metric!(
        name: "test_counter_third_party",
        help: "an third_party counter for testing",
        var_labels: ["label"],
    ));
    let counter = cv
        .get_third_party_metric_with_label_values(&["testing"])
        .unwrap();
    counter.inc();
    let readings = reg.gather_third_party_visible();
    assert_eq!(readings.len(), 1);
    assert_eq!(readings[0].get_name(), "test_counter_third_party");
    let metrics = readings[0].get_metric();
    assert_eq!(metrics.len(), 1);
    assert!((metrics[0].get_counter().get_value() - 1.0).abs() < f64::EPSILON);
}
