// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Conversions between the `AUTO SCALING STRATEGY` cluster option and the
//! structured [`AutoScalingStrategy`] policy.
//!
//! Three representations meet here:
//!
//! - The SQL option in a `CREATE CLUSTER` / `ALTER CLUSTER` statement
//!   (`ClusterAutoScalingStrategyOptionValue` AST).
//! - The structured policy (`mz_sql::plan::AutoScalingStrategy`), used for
//!   drift comparison.
//! - The `strategy` jsonb column of
//!   `mz_internal.mz_cluster_auto_scaling_strategies`, whose serde shape
//!   matches the plan type.

use mz_repr::adt::interval::Interval;
use mz_sql::plan::{AutoScalingStrategy, OnHydration, TryFromValue};
use mz_sql_parser::ast::{
    ClusterAutoScalingStrategyOptionValue, ClusterOption, ClusterOptionName,
    OnHydrationOptionValue, Raw, Value, WithOptionValue,
};
use std::time::Duration;

/// Convert the AST option value into a structured policy. An empty block
/// `AUTO SCALING STRATEGY = ()` maps to `None` (autoscaling disabled), the
/// same normalization the server planner applies.
pub(crate) fn strategy_from_option_value(
    value: &ClusterAutoScalingStrategyOptionValue,
) -> Result<Option<AutoScalingStrategy>, String> {
    let Some(on_hydration) = &value.on_hydration else {
        return Ok(None);
    };

    let hydration_size = String::try_from_value(on_hydration.hydration_size.clone())
        .map_err(|e| format!("invalid HYDRATION SIZE: {}", e))?;

    let linger_duration = on_hydration
        .linger_duration
        .clone()
        .map(Duration::try_from_value)
        .transpose()
        .map_err(|e| format!("invalid LINGER DURATION: {}", e))?;

    Ok(Some(AutoScalingStrategy {
        on_hydration: Some(OnHydration {
            hydration_size,
            linger_duration,
        }),
    }))
}

/// Render a structured policy back into a `CREATE CLUSTER` / `ALTER CLUSTER
/// ... SET` option, e.g.
/// `AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '3200cc', LINGER DURATION = '00:10:00'))`.
pub(crate) fn strategy_to_cluster_option(strategy: &AutoScalingStrategy) -> ClusterOption<Raw> {
    let on_hydration = strategy
        .on_hydration
        .as_ref()
        .map(|on_hydration| OnHydrationOptionValue {
            hydration_size: Value::String(on_hydration.hydration_size.clone()),
            linger_duration: on_hydration.linger_duration.map(|d| {
                // Every linger duration we see was planned from an interval
                // (either by the server, for values read back from the
                // catalog, or by `strategy_from_option_value`, which enforces
                // interval range), so the conversion back cannot fail.
                let interval = Interval::from_duration(&d)
                    .expect("linger duration is convertible back to an interval");
                Value::String(interval.to_string())
            }),
        });
    ClusterOption {
        name: ClusterOptionName::AutoScalingStrategy,
        value: Some(WithOptionValue::ClusterAutoScalingStrategyOptionValue(
            ClusterAutoScalingStrategyOptionValue { on_hydration },
        )),
    }
}

/// Parse the `strategy` jsonb column of
/// `mz_internal.mz_cluster_auto_scaling_strategies` (fetched as text). The
/// column is JSON `null` for a cluster whose policy was just removed while a
/// burst still lingers, which maps to `None` like a missing row.
pub(crate) fn strategy_from_catalog_json(
    json: &str,
) -> Result<Option<AutoScalingStrategy>, String> {
    serde_json::from_str::<Option<AutoScalingStrategy>>(json)
        .map_err(|e| format!("cannot parse autoscaling strategy {:?}: {}", json, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_ore::assert_none;
    use mz_sql_parser::ast::display::AstDisplay;
    use mz_sql_parser::parser::parse_statements;

    fn strategy(hydration_size: &str, linger: Option<Duration>) -> AutoScalingStrategy {
        AutoScalingStrategy {
            on_hydration: Some(OnHydration {
                hydration_size: hydration_size.to_string(),
                linger_duration: linger,
            }),
        }
    }

    /// Extract the AUTO SCALING STRATEGY option value from a CREATE CLUSTER.
    fn option_value_of(sql: &str) -> Option<ClusterAutoScalingStrategyOptionValue> {
        let stmts = parse_statements(sql).unwrap();
        let create = match &stmts[0].ast {
            mz_sql_parser::ast::Statement::CreateCluster(c) => c.clone(),
            other => panic!("expected CREATE CLUSTER, got {:?}", other),
        };
        create
            .options
            .into_iter()
            .find_map(|opt| match (opt.name, opt.value) {
                (
                    ClusterOptionName::AutoScalingStrategy,
                    Some(WithOptionValue::ClusterAutoScalingStrategyOptionValue(v)),
                ) => Some(v),
                _ => None,
            })
    }

    #[mz_ore::test]
    fn test_strategy_from_option_value() {
        let value = option_value_of(
            "CREATE CLUSTER c (SIZE = '25cc', AUTO SCALING STRATEGY = \
             (ON HYDRATION (HYDRATION SIZE = '100cc', LINGER DURATION = '600s')))",
        )
        .unwrap();
        assert_eq!(
            strategy_from_option_value(&value).unwrap(),
            Some(strategy("100cc", Some(Duration::from_secs(600))))
        );

        let value = option_value_of(
            "CREATE CLUSTER c (SIZE = '25cc', AUTO SCALING STRATEGY = \
             (ON HYDRATION (HYDRATION SIZE = '100cc')))",
        )
        .unwrap();
        assert_eq!(
            strategy_from_option_value(&value).unwrap(),
            Some(strategy("100cc", None))
        );

        // An empty block disables autoscaling.
        let value = option_value_of("CREATE CLUSTER c (SIZE = '25cc', AUTO SCALING STRATEGY = ())")
            .unwrap();
        assert_none!(strategy_from_option_value(&value).unwrap());

        // A non-interval linger duration is a conversion error.
        let value = option_value_of(
            "CREATE CLUSTER c (SIZE = '25cc', AUTO SCALING STRATEGY = \
             (ON HYDRATION (HYDRATION SIZE = '100cc', LINGER DURATION = 'bogus')))",
        )
        .unwrap();
        assert!(strategy_from_option_value(&value).is_err());
    }

    #[mz_ore::test]
    fn test_strategy_option_round_trips_through_parser() {
        for strategy in [
            strategy("100cc", Some(Duration::from_secs(600))),
            strategy("100cc", None),
        ] {
            let rendered = strategy_to_cluster_option(&strategy).to_ast_string_simple();
            let sql = format!("CREATE CLUSTER c (SIZE = '25cc', {})", rendered);
            let value = option_value_of(&sql)
                .unwrap_or_else(|| panic!("rendered option did not parse: {}", sql));
            assert_eq!(
                strategy_from_option_value(&value).unwrap(),
                Some(strategy),
                "round trip through {}",
                rendered
            );
        }
    }

    #[mz_ore::test]
    fn test_strategy_from_catalog_json() {
        // The serde shape of the durable AutoScalingStrategy, as surfaced by
        // mz_internal.mz_cluster_auto_scaling_strategies.
        let json = r#"{"on_hydration": {"hydration_size": "100cc", "linger_duration": {"secs": 600, "nanos": 0}}}"#;
        assert_eq!(
            strategy_from_catalog_json(json).unwrap(),
            Some(strategy("100cc", Some(Duration::from_secs(600))))
        );

        let json = r#"{"on_hydration": {"hydration_size": "100cc", "linger_duration": null}}"#;
        assert_eq!(
            strategy_from_catalog_json(json).unwrap(),
            Some(strategy("100cc", None))
        );

        // JSON null: a burst-only row for a just-removed policy.
        assert_none!(strategy_from_catalog_json("null").unwrap());

        assert!(strategy_from_catalog_json("not json").is_err());
    }
}
