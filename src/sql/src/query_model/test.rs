// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ast::{Expr, Raw};
use crate::catalog::{
    CatalogConfig, CatalogDatabase, CatalogError, CatalogItem, CatalogItemType, CatalogRole,
    CatalogSchema, SessionCatalog,
};
use crate::func::{Func, MZ_CATALOG_BUILTINS, MZ_INTERNAL_BUILTINS, PG_CATALOG_BUILTINS};
use crate::names::{DatabaseSpecifier, FullName, PartialName};
use crate::plan::query::QueryLifetime;
use crate::plan::{StatementContext, StatementDesc};
use build_info::DUMMY_BUILD_INFO;
use chrono::MIN_DATETIME;
use dataflow_types::sources::SourceConnector;
use expr::{DummyHumanizer, ExprHumanizer, GlobalId, MirScalarExpr};
use expr_test_util::generate_explanation;
use lazy_static::lazy_static;
use lowertest::*;
use ore::now::{EpochMillis, NOW_ZERO};
use repr::{RelationDesc, RelationType, ScalarType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::query_model;

lazy_static! {
    static ref DUMMY_CONFIG: CatalogConfig = CatalogConfig {
        start_time: MIN_DATETIME,
        start_instant: Instant::now(),
        nonce: 0,
        cluster_id: Uuid::from_u128(0),
        session_id: Uuid::from_u128(0),
        experimental_mode: false,
        safe_mode: false,
        build_info: &DUMMY_BUILD_INFO,
        timestamp_frequency: Duration::from_secs(1),
        now: NOW_ZERO.clone(),
        disable_user_indexes: false,
    };
    static ref RTI: ReflectedTypeInfo = {
        let mut rti = ReflectedTypeInfo::default();
        TestCatalogCommand::add_to_reflected_type_info(&mut rti);
        rti
    };
}

/// A dummy [`CatalogItem`] implementation.
///
/// This implementation is suitable for use in tests that plan queries which are
/// not demanding of the catalog, as many methods are unimplemented.
#[derive(Debug)]
pub enum TestCatalogItem {
    /// Represents some kind of data source. Could be a Source/Table/View.
    BaseTable {
        name: FullName,
        id: GlobalId,
        desc: RelationDesc,
    },
    Func(&'static Func),
}

impl CatalogItem for TestCatalogItem {
    fn name(&self) -> &FullName {
        match &self {
            TestCatalogItem::BaseTable { name, .. } => name,
            _ => unimplemented!(),
        }
    }

    fn id(&self) -> GlobalId {
        match &self {
            TestCatalogItem::BaseTable { id, .. } => *id,
            _ => unimplemented!(),
        }
    }

    fn oid(&self) -> u32 {
        unimplemented!()
    }

    fn desc(&self) -> Result<&RelationDesc, CatalogError> {
        match &self {
            TestCatalogItem::BaseTable { desc, .. } => Ok(desc),
            _ => Err(CatalogError::UnknownItem(format!(
                "{:?} does not have a desc() method",
                self
            ))),
        }
    }

    fn func(&self) -> Result<&'static Func, CatalogError> {
        match &self {
            TestCatalogItem::Func(func) => Ok(func),
            _ => Err(CatalogError::UnknownFunction(format!(
                "{:?} does not have a func() method",
                self
            ))),
        }
    }

    fn source_connector(&self) -> Result<&SourceConnector, CatalogError> {
        unimplemented!()
    }

    fn item_type(&self) -> CatalogItemType {
        match &self {
            TestCatalogItem::BaseTable { .. } => CatalogItemType::View,
            TestCatalogItem::Func(_) => CatalogItemType::Func,
        }
    }

    fn create_sql(&self) -> &str {
        unimplemented!()
    }

    fn uses(&self) -> &[GlobalId] {
        unimplemented!()
    }

    fn used_by(&self) -> &[GlobalId] {
        unimplemented!()
    }

    fn index_details(&self) -> Option<(&[MirScalarExpr], GlobalId)> {
        unimplemented!()
    }

    fn table_details(&self) -> Option<&[Expr<Raw>]> {
        unimplemented!()
    }
}

/// A dummy [`SessionCatalog`] implementation.
///
/// This implementation is suitable for use in tests that plan queries which are
/// not demanding of the catalog, as many methods are unimplemented.
#[derive(Debug)]
pub struct TestCatalog {
    /// Contains the Materialize builtin functions.
    funcs: HashMap<&'static str, TestCatalogItem>,
    /// Contains dummy data sources.
    tables: HashMap<String, TestCatalogItem>,
    /// Maps (unique id) -> (name of data source).
    /// Exists to support the `*get_item_by_id` functions.
    id_to_name: HashMap<GlobalId, String>,
}

impl Default for TestCatalog {
    fn default() -> Self {
        let mut funcs = HashMap::new();
        for (name, func) in MZ_INTERNAL_BUILTINS.iter() {
            funcs.insert(*name, TestCatalogItem::Func(func));
        }
        for (name, func) in MZ_CATALOG_BUILTINS.iter() {
            funcs.insert(*name, TestCatalogItem::Func(func));
        }
        for (name, func) in PG_CATALOG_BUILTINS.iter() {
            funcs.insert(*name, TestCatalogItem::Func(func));
        }
        Self {
            funcs,
            tables: HashMap::new(),
            id_to_name: HashMap::new(),
        }
    }
}

impl SessionCatalog for TestCatalog {
    fn search_path(&self, _: bool) -> Vec<&str> {
        vec!["dummy"]
    }

    fn user(&self) -> &str {
        "dummy"
    }

    fn get_prepared_statement_desc(&self, _: &str) -> Option<&StatementDesc> {
        None
    }

    fn default_database(&self) -> &str {
        "dummy"
    }

    fn resolve_database(&self, _: &str) -> Result<&dyn CatalogDatabase, CatalogError> {
        unimplemented!();
    }

    fn resolve_schema(
        &self,
        _: Option<String>,
        _: &str,
    ) -> Result<&dyn CatalogSchema, CatalogError> {
        unimplemented!();
    }

    fn resolve_role(&self, _: &str) -> Result<&dyn CatalogRole, CatalogError> {
        unimplemented!();
    }

    fn resolve_item(&self, partial_name: &PartialName) -> Result<&dyn CatalogItem, CatalogError> {
        if let Some(result) = self.tables.get(&partial_name.item[..]) {
            return Ok(result);
        }
        Err(CatalogError::UnknownItem(partial_name.item.clone()))
    }

    fn resolve_function(
        &self,
        partial_name: &PartialName,
    ) -> Result<&dyn CatalogItem, CatalogError> {
        if let Some(result) = self.funcs.get(&partial_name.item[..]) {
            return Ok(result);
        }
        Err(CatalogError::UnknownFunction(partial_name.item.clone()))
    }

    fn get_item_by_id(&self, id: &GlobalId) -> &dyn CatalogItem {
        &self.tables[&self.id_to_name[id]]
    }

    fn try_get_item_by_id(&self, _: &GlobalId) -> Option<&dyn CatalogItem> {
        unimplemented!();
    }

    fn get_item_by_oid(&self, _: &u32) -> &dyn CatalogItem {
        unimplemented!();
    }

    fn item_exists(&self, _: &FullName) -> bool {
        false
    }

    fn try_get_lossy_scalar_type_by_id(&self, _: &GlobalId) -> Option<ScalarType> {
        None
    }

    fn config(&self) -> &CatalogConfig {
        &DUMMY_CONFIG
    }

    fn now(&self) -> EpochMillis {
        (self.config().now)()
    }
}

impl ExprHumanizer for TestCatalog {
    fn humanize_id(&self, id: GlobalId) -> Option<String> {
        DummyHumanizer.humanize_id(id)
    }

    fn humanize_scalar_type(&self, ty: &ScalarType) -> String {
        DummyHumanizer.humanize_scalar_type(ty)
    }
}

/// Contains the arguments for a command for [TestCatalog].
///
/// See [lowertest] for the command syntax.
#[derive(Debug, Serialize, Deserialize, MzReflect)]
pub enum TestCatalogCommand {
    /// Insert a source into the catalog.
    Defsource {
        source_name: String,
        typ: RelationType,
        #[serde(default)]
        column_names: Vec<String>,
    },
}

impl TestCatalog {
    fn execute_commands(&mut self, spec: &str) -> Result<String, String> {
        let mut stream_iter = tokenize(spec)?.into_iter();
        loop {
            let command: Option<TestCatalogCommand> = deserialize_optional(
                &mut stream_iter,
                "TestCatalogCommand",
                &RTI,
                &mut GenericTestDeserializeContext::default(),
            )?;
            if let Some(command) = command {
                match command {
                    TestCatalogCommand::Defsource {
                        source_name,
                        typ,
                        column_names,
                    } => {
                        assert_eq!(
                            typ.arity(),
                            column_names.len(),
                            "Ensure that there are the right number of column names for source {}",
                            source_name
                        );
                        let id = GlobalId::User(self.tables.len() as u64);
                        self.id_to_name.insert(id, source_name.clone());
                        self.tables.insert(
                            source_name.clone(),
                            TestCatalogItem::BaseTable {
                                name: FullName {
                                    database: DatabaseSpecifier::from(Some(
                                        self.default_database().to_string(),
                                    )),
                                    schema: self.user().to_string(),
                                    item: source_name,
                                },
                                id,
                                desc: RelationDesc::new(typ, column_names.into_iter()),
                            },
                        );
                    }
                }
            } else {
                break;
            }
        }
        Ok("ok\n".to_string())
    }
}

#[test]
fn test_hir_generator() {
    datadriven::walk("tests/querymodel", |f| {
        let mut catalog = TestCatalog::default();

        f.run(move |s| -> String {
            let build_stmt = |stmt, dot_graph, lower| -> String {
                let scx = &StatementContext::new(None, &catalog);
                if let sql_parser::ast::Statement::Select(query) = stmt {
                    let planned_query = match crate::plan::query::plan_root_query(
                        scx,
                        query.query,
                        QueryLifetime::Static,
                    ) {
                        Ok(planned_query) => planned_query,
                        Err(e) => return format!("unable to plan query: {}: {}", s.input, e),
                    };

                    let model = query_model::Model::from(planned_query.expr);

                    let mut output = String::new();

                    if dot_graph {
                        output += &match query_model::dot::DotGenerator::new()
                            .generate(&model, &s.input)
                        {
                            Ok(graph) => graph,
                            Err(e) => return format!("graph generation error: {}", e),
                        };
                    }

                    if lower {
                        output +=
                            &generate_explanation(&catalog, &model.lower(), s.args.get("format"));
                    }

                    output
                } else {
                    format!("invalid query: {}", s.input)
                }
            };

            let execute_command = |dot_graph, lower| -> String {
                let stmts = match sql_parser::parser::parse_statements(&s.input) {
                    Ok(stmts) => stmts,
                    Err(e) => return format!("unable to parse SQL: {}: {}", s.input, e),
                };

                stmts.into_iter().fold("".to_string(), |c, stmt| {
                    c + &build_stmt(stmt, dot_graph, lower)
                })
            };

            match s.directive.as_str() {
                "build" => execute_command(true, false),
                "lower" => execute_command(false, true),
                "cat" => match catalog.execute_commands(&s.input) {
                    Ok(ok) => ok,
                    Err(err) => err,
                },
                _ => panic!("unknown directive: {}", s.directive),
            }
        })
    });
}
