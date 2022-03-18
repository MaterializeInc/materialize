use std::{
    collections::BTreeMap,
    fmt,
    time::{Duration, Instant},
};

use mz_dataflow_types::{DataflowDesc, DataflowDescription};
use mz_expr::{CollectionPlan, GlobalId, MirRelationExpr, OptimizedMirRelationExpr};
use mz_repr::{adt::interval::Interval, Datum, Row};
use mz_sql::{
    ast::ExplainStage,
    plan::{ExplainPlan, HirRelationExpr, OptimizerConfig, QueryWhen},
};

use crate::{coord::send_immediate_rows, session::Session, CoordError, ExecuteResponse};

use super::Coordinator;

impl Coordinator {
    pub fn sequence_explain(
        &mut self,
        session: &Session,
        plan: ExplainPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let compute_instance = self
            .catalog
            .resolve_compute_instance(session.vars().cluster())?
            .id;

        let ExplainPlan {
            raw_plan,
            row_set_finishing,
            stage,
            options,
        } = plan;

        struct Timings {
            decorrelation: Option<Duration>,
            optimization: Option<Duration>,
        }

        let mut timings = Timings {
            decorrelation: None,
            optimization: None,
        };

        let decorrelate = |timings: &mut Timings,
                           raw_plan: HirRelationExpr|
         -> Result<MirRelationExpr, CoordError> {
            let start = Instant::now();
            let decorrelated_plan = raw_plan.optimize_and_lower(&OptimizerConfig {
                qgm_optimizations: session.vars().qgm_optimizations(),
            })?;
            timings.decorrelation = Some(start.elapsed());
            Ok(decorrelated_plan)
        };

        let optimize =
            |timings: &mut Timings,
             coord: &mut Self,
             decorrelated_plan: MirRelationExpr|
             -> Result<DataflowDescription<OptimizedMirRelationExpr>, CoordError> {
                let start = Instant::now();
                let optimized_plan = coord.view_optimizer.optimize(decorrelated_plan)?;
                let mut dataflow = DataflowDesc::new(format!("explanation"), GlobalId::Explain);
                coord
                    .dataflow_builder(compute_instance)
                    .import_view_into_dataflow(
                        // TODO: If explaining a view, pipe the actual id of the view.
                        &GlobalId::Explain,
                        &optimized_plan,
                        &mut dataflow,
                    )?;
                mz_transform::optimize_dataflow(
                    &mut dataflow,
                    &coord.index_oracle(compute_instance),
                )?;
                timings.optimization = Some(start.elapsed());
                Ok(dataflow)
            };

        let mut explanation_string = match stage {
            ExplainStage::RawPlan => {
                let catalog = self.catalog.for_session(session);
                let mut explanation = mz_sql::plan::Explanation::new(&raw_plan, &catalog);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                if options.typed {
                    explanation.explain_types(&BTreeMap::new());
                }
                explanation.to_string()
            }
            ExplainStage::QueryGraph => {
                let catalog = self.catalog.for_session(session);
                let mut model = mz_sql::query_model::Model::try_from(raw_plan)?;
                model.as_dot("", &catalog, options.typed)?
            }
            ExplainStage::OptimizedQueryGraph => {
                let catalog = self.catalog.for_session(session);
                let mut model = mz_sql::query_model::Model::try_from(raw_plan)?;
                model.optimize();
                model.as_dot("", &catalog, options.typed)?
            }
            ExplainStage::DecorrelatedPlan => {
                let decorrelated_plan = OptimizedMirRelationExpr::declare_optimized(decorrelate(
                    &mut timings,
                    raw_plan,
                )?);
                let catalog = self.catalog.for_session(session);
                let formatter =
                    mz_dataflow_types::DataflowGraphFormatter::new(&catalog, options.typed);
                let mut explanation =
                    mz_dataflow_types::Explanation::new(&decorrelated_plan, &catalog, &formatter);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                explanation.to_string()
            }
            ExplainStage::OptimizedPlan => {
                let decorrelated_plan = decorrelate(&mut timings, raw_plan)?;
                self.validate_timeline(decorrelated_plan.depends_on())?;
                let dataflow = optimize(&mut timings, self, decorrelated_plan)?;
                let catalog = self.catalog.for_session(session);
                let formatter =
                    mz_dataflow_types::DataflowGraphFormatter::new(&catalog, options.typed);
                let mut explanation = mz_dataflow_types::Explanation::new_from_dataflow(
                    &dataflow, &catalog, &formatter,
                );
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                explanation.to_string()
            }
            ExplainStage::PhysicalPlan => {
                let decorrelated_plan = decorrelate(&mut timings, raw_plan)?;
                self.validate_timeline(decorrelated_plan.depends_on())?;
                let dataflow = optimize(&mut timings, self, decorrelated_plan)?;
                let dataflow_plan =
                    mz_dataflow_types::Plan::<mz_repr::Timestamp>::finalize_dataflow(dataflow)
                        .expect("Dataflow planning failed; unrecoverable error");
                let catalog = self.catalog.for_session(session);
                let mut explanation = mz_dataflow_types::Explanation::new_from_dataflow(
                    &dataflow_plan,
                    &catalog,
                    &mz_dataflow_types::JsonViewFormatter {},
                );
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                explanation.to_string()
            }
            ExplainStage::Timestamp => {
                let decorrelated_plan = decorrelate(&mut timings, raw_plan)?;
                let optimized_plan = self.view_optimizer.optimize(decorrelated_plan)?;
                self.validate_timeline(optimized_plan.depends_on())?;
                let source_ids = optimized_plan.depends_on();
                let id_bundle = self
                    .index_oracle(compute_instance)
                    .sufficient_collections(&source_ids);
                // TODO: determine_timestamp takes a mut self to track table linearizability,
                // so explaining a plan involving tables has side effects. Removing those side
                // effects would be good.
                let timestamp = self.determine_timestamp(
                    &session,
                    &id_bundle,
                    QueryWhen::Immediately,
                    compute_instance,
                )?;
                let since = self
                    .least_valid_read(&id_bundle, compute_instance)
                    .elements()
                    .to_vec();
                let upper = self
                    .least_valid_write(&id_bundle, compute_instance)
                    .elements()
                    .to_vec();
                let has_table = id_bundle.iter().any(|id| self.catalog.uses_tables(id));
                let table_read_ts = if has_table {
                    Some(self.get_local_read_ts())
                } else {
                    None
                };
                let mut sources = Vec::new();
                {
                    let storage = self.dataflow_client.storage();
                    for id in id_bundle.storage_ids.iter() {
                        let state = storage.collection(*id).unwrap();
                        let name = self
                            .catalog
                            .try_get_by_id(*id)
                            .map(|item| item.name().to_string())
                            .unwrap_or_else(|| id.to_string());
                        sources.push(TimestampSource {
                            name: format!("{name} ({id}, storage)"),
                            read_frontier: state.implied_capability.elements().to_vec(),
                            write_frontier: state
                                .write_frontier
                                .frontier()
                                .to_owned()
                                .elements()
                                .to_vec(),
                        });
                    }
                }
                {
                    let compute = self.dataflow_client.compute(compute_instance).unwrap();
                    for id in id_bundle.compute_ids.iter() {
                        let state = compute.collection(*id).unwrap();
                        let name = self
                            .catalog
                            .try_get_by_id(*id)
                            .map(|item| item.name().to_string())
                            .unwrap_or_else(|| id.to_string());
                        sources.push(TimestampSource {
                            name: format!("{name} ({id}, compute)"),
                            read_frontier: state.implied_capability.elements().to_vec(),
                            write_frontier: state
                                .write_frontier
                                .frontier()
                                .to_owned()
                                .elements()
                                .to_vec(),
                        });
                    }
                }
                let explanation = TimestampExplanation {
                    timestamp,
                    since,
                    upper,
                    has_table,
                    table_read_ts,
                    sources,
                };
                explanation.to_string()
            }
        };
        if options.timing {
            if let Some(decorrelation) = &timings.decorrelation {
                explanation_string.push_str(&format!(
                    "\nDecorrelation time: {}",
                    Interval {
                        months: 0,
                        days: 0,
                        micros: decorrelation.as_micros().try_into().unwrap(),
                    }
                ));
            }
            if let Some(optimization) = &timings.optimization {
                explanation_string.push_str(&format!(
                    "\nOptimization time: {}",
                    Interval {
                        months: 0,
                        days: 0,
                        micros: optimization.as_micros().try_into().unwrap(),
                    }
                ));
            }
            if timings.decorrelation.is_some() || timings.optimization.is_some() {
                explanation_string.push_str("\n");
            }
        }
        let rows = vec![Row::pack_slice(&[Datum::from(&*explanation_string)])];
        Ok(send_immediate_rows(rows))
    }
}

/// Information used when determining the timestamp for a query.
pub struct TimestampExplanation<T> {
    /// The chosen timestamp from `determine_timestamp`.
    pub timestamp: T,
    /// Whether the query contains a table.
    pub has_table: bool,
    /// If the query contains a table, the global table read timestamp.
    pub table_read_ts: Option<T>,
    /// The read frontier of all involved sources.
    pub since: Vec<T>,
    /// The write frontier of all involved sources.
    pub upper: Vec<T>,
    /// Details about each source.
    pub sources: Vec<TimestampSource<T>>,
}

pub struct TimestampSource<T> {
    pub name: String,
    pub read_frontier: Vec<T>,
    pub write_frontier: Vec<T>,
}

impl<T: fmt::Display + fmt::Debug> fmt::Display for TimestampExplanation<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "     timestamp: {}", self.timestamp)?;
        writeln!(f, "         since:{:13?}", self.since)?;
        writeln!(f, "         upper:{:13?}", self.upper)?;
        writeln!(f, "     has table: {}", self.has_table)?;
        if let Some(ts) = &self.table_read_ts {
            writeln!(f, " table read ts: {ts}")?;
        }
        for source in &self.sources {
            writeln!(f, "")?;
            writeln!(f, "source {}:", source.name)?;
            writeln!(f, " read frontier:{:13?}", source.read_frontier)?;
            writeln!(f, "write frontier:{:13?}", source.write_frontier)?;
        }
        Ok(())
    }
}
