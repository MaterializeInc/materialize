// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL AST traversal.

macro_rules! make_option_ext {
    ($($mut:tt)*) => {
        trait OptionExt<T> {
            fn as_auto_ref(&$($mut)* self) -> Option<&$($mut)* T>;
        }

        impl<T> OptionExt<T> for Option<T> {
            fn as_auto_ref(&$($mut)* self) -> Option<&$($mut)* T> {
                #[allow(clippy::match_as_ref)]
                match *self {
                    Some(ref $($mut)* x) => Some(x),
                    None => None,
                }
            }
        }

        trait AsAutoRef<T> {
            fn as_auto_ref(&$($mut)* self) -> &$($mut)* T;
        }

        impl<T> AsAutoRef<T> for Box<T> {
            fn as_auto_ref(&$($mut)* self) -> &$($mut)* T {
                &$($mut) **self
            }
        }
    }
}

macro_rules! make_visitor {
    ($name:ident: &$($mut:tt)*) => {
        use crate::ast::*;

        make_option_ext!($($mut)*);

        /// A trait that represents a visitor that walks through a SQL AST.
        ///
        /// Each function corresponds to a node in the SQL AST, and has a default
        /// implementation that visits all of its child nodes. Implementors of this
        /// trait can override functions as desired to hook into AST traversal without
        /// writing code to traverse the entire AST.
        pub trait $name<'ast> {
            fn visit_statement(&mut self, statement: &'ast $($mut)* Statement) {
                visit_statement(self, statement)
            }

            fn visit_query(&mut self, query: &'ast $($mut)* Query) {
                visit_query(self, query)
            }

            fn visit_cte(&mut self, cte: &'ast $($mut)* Cte) {
                visit_cte(self, cte)
            }

            fn visit_select(&mut self, select: &'ast $($mut)* Select) {
                visit_select(self, select)
            }

            fn visit_select_item(&mut self, select_item: &'ast $($mut)* SelectItem) {
                visit_select_item(self, select_item)
            }

            fn visit_table_with_joins(&mut self, table_with_joins: &'ast $($mut)* TableWithJoins) {
                visit_table_with_joins(self, table_with_joins)
            }

            fn visit_table_factor(&mut self, table_factor: &'ast $($mut)* TableFactor) {
                visit_table_factor(self, table_factor)
            }

            fn visit_table_table_factor(
                &mut self,
                name: &'ast $($mut)* ObjectName,
                alias: Option<&'ast $($mut)* TableAlias>,
                args: &'ast $($mut)* [Expr],
                with_hints: &'ast $($mut)* [Expr],
            ) {
                visit_table_table_factor(self, name, alias, args, with_hints)
            }

            fn visit_derived_table_factor(
                &mut self,
                lateral: bool,
                subquery: &'ast $($mut)* Query,
                alias: Option<&'ast $($mut)* TableAlias>,
            ) {
                visit_derived_table_factor(self, lateral, subquery, alias)
            }

            fn visit_nested_join_table_factor(&mut self, table_with_joins: &'ast $($mut)* TableWithJoins) {
                visit_nested_join_table_factor(self, table_with_joins)
            }

            fn visit_table_alias(&mut self, alias: &'ast $($mut)* TableAlias) {
                visit_table_alias(self, alias)
            }

            fn visit_join(&mut self, join: &'ast $($mut)* Join) {
                visit_join(self, join)
            }

            fn visit_join_operator(&mut self, op: &'ast $($mut)* JoinOperator) {
                visit_join_operator(self, op)
            }

            fn visit_join_constraint(&mut self, constraint: &'ast $($mut)* JoinConstraint) {
                visit_join_constraint(self, constraint)
            }

            fn visit_where(&mut self, expr: &'ast $($mut)* Expr) {
                visit_where(self, expr)
            }

            fn visit_group_by(&mut self, exprs: &'ast $($mut)* [Expr]) {
                visit_group_by(self, exprs)
            }

            fn visit_having(&mut self, expr: &'ast $($mut)* Expr) {
                visit_having(self, expr)
            }

            fn visit_set_expr(&mut self, set_expr: &'ast $($mut)* SetExpr) {
                visit_set_expr(self, set_expr)
            }

            fn visit_set_operation(
                &mut self,
                left: &'ast $($mut)* SetExpr,
                op: &'ast $($mut)* SetOperator,
                right: &'ast $($mut)* SetExpr,
                all: bool,
            ) {
                visit_set_operation(self, left, op, right, all)
            }

            fn visit_set_operator(&mut self, _operator: &'ast $($mut)* SetOperator) {}

            fn visit_order_by(&mut self, order_by: &'ast $($mut)* OrderByExpr) {
                visit_order_by(self, order_by)
            }

            fn visit_limit(&mut self, expr: &'ast $($mut)* Expr) {
                visit_limit(self, expr)
            }

            fn visit_type(&mut self, _data_type: &'ast $($mut)* DataType) {}

            fn visit_expr(&mut self, expr: &'ast $($mut)* Expr) {
                visit_expr(self, expr)
            }

            fn visit_unnamed_expr(&mut self, expr: &'ast $($mut)* Expr) {
                visit_unnamed_expr(self, expr)
            }

            fn visit_expr_with_alias(&mut self, expr: &'ast $($mut)* Expr, alias: &'ast $($mut)* Ident) {
                visit_expr_with_alias(self, expr, alias)
            }

            fn visit_object_name(&mut self, object_name: &'ast $($mut)* ObjectName) {
                visit_object_name(self, object_name)
            }

            fn visit_ident(&mut self, _ident: &'ast $($mut)* Ident) {}

            fn visit_compound_identifier(&mut self, idents: &'ast $($mut)* [Ident]) {
                visit_compound_identifier(self, idents)
            }

            fn visit_wildcard(&mut self) {}

            fn visit_qualified_wildcard(&mut self, idents: &'ast $($mut)* [Ident]) {
                visit_qualified_wildcard(self, idents)
            }

            fn visit_parameter(&mut self, _n: usize) {}

            fn visit_is_null(&mut self, expr: &'ast $($mut)* Expr) {
                visit_is_null(self, expr)
            }

            fn visit_is_not_null(&mut self, expr: &'ast $($mut)* Expr) {
                visit_is_not_null(self, expr)
            }

            fn visit_in_list(&mut self, expr: &'ast $($mut)* Expr, list: &'ast $($mut)* [Expr], negated: bool) {
                visit_in_list(self, expr, list, negated)
            }

            fn visit_in_subquery(&mut self, expr: &'ast $($mut)* Expr, subquery: &'ast $($mut)* Query, negated: bool) {
                visit_in_subquery(self, expr, subquery, negated)
            }

            fn visit_between(
                &mut self,
                expr: &'ast $($mut)* Expr,
                low: &'ast $($mut)* Expr,
                high: &'ast $($mut)* Expr,
                negated: bool,
            ) {
                visit_between(self, expr, low, high, negated)
            }

            fn visit_binary_op(&mut self, left: &'ast $($mut)* Expr, op: &'ast $($mut)* BinaryOperator, right: &'ast $($mut)* Expr) {
                visit_binary_op(self, left, op, right)
            }

            fn visit_binary_operator(&mut self, _op: &'ast $($mut)* BinaryOperator) {}

            fn visit_unary_op(&mut self, expr: &'ast $($mut)* Expr, op: &'ast $($mut)* UnaryOperator) {
                visit_unary_op(self, expr, op)
            }

            fn visit_unary_operator(&mut self, _op: &'ast $($mut)* UnaryOperator) {}

            fn visit_cast(&mut self, expr: &'ast $($mut)* Expr, data_type: &'ast $($mut)* DataType) {
                visit_cast(self, expr, data_type)
            }

            fn visit_collate(&mut self, expr: &'ast $($mut)* Expr, collation: &'ast $($mut)* ObjectName) {
                visit_collate(self, expr, collation)
            }

            fn visit_extract(&mut self, field: &'ast $($mut)* ExtractField, expr: &'ast $($mut)* Expr) {
                visit_extract(self, field, expr)
            }

            fn visit_date_time_field(&mut self, _field: &'ast $($mut)* DateTimeField) {}

            fn visit_extract_field(&mut self, _field: &'ast $($mut)* ExtractField) {}

            fn visit_nested(&mut self, expr: &'ast $($mut)* Expr) {
                visit_nested(self, expr)
            }

            fn visit_value(&mut self, _val: &'ast $($mut)* Value) {}

            fn visit_function(&mut self, func: &'ast $($mut)* Function) {
                visit_function(self, func)
            }

            fn visit_window_spec(&mut self, window_spec: &'ast $($mut)* WindowSpec) {
                visit_window_spec(self, window_spec)
            }

            fn visit_window_frame(&mut self, window_frame: &'ast $($mut)* WindowFrame) {
                visit_window_frame(self, window_frame)
            }

            fn visit_window_frame_units(&mut self, _window_frame_units: &'ast $($mut)* WindowFrameUnits) {}

            fn visit_window_frame_bound(&mut self, _window_frame_bound: &'ast $($mut)* WindowFrameBound) {}

            fn visit_case(
                &mut self,
                operand: Option<&'ast $($mut)* Expr>,
                conditions: &'ast $($mut)* [Expr],
                results: &'ast $($mut)* [Expr],
                else_result: Option<&'ast $($mut)* Expr>,
            ) {
                visit_case(self, operand, conditions, results, else_result)
            }

            fn visit_exists(&mut self, subquery: &'ast $($mut)* Query) {
                visit_exists(self, subquery)
            }

            fn visit_subquery(&mut self, subquery: &'ast $($mut)* Query) {
                visit_subquery(self, subquery)
            }

            fn visit_any(&mut self, left: &'ast $($mut)* Expr, op: &'ast $($mut)* BinaryOperator, right: &'ast $($mut)* Query) {
                visit_any(self, left, op, right)
            }

            fn visit_all(&mut self, left: &'ast $($mut)* Expr, op: &'ast $($mut)* BinaryOperator, right: &'ast $($mut)* Query) {
                visit_all(self, left, op, right)
            }

            fn visit_insert(
                &mut self,
                table_name: &'ast $($mut)* ObjectName,
                columns: &'ast $($mut)* [Ident],
                source: &'ast $($mut)* Query,
            ) {
                visit_insert(self, table_name, columns, source)
            }

            fn visit_values(&mut self, values: &'ast $($mut)* Values) {
                visit_values(self, values)
            }

            fn visit_values_row(&mut self, row: &'ast $($mut)* [Expr]) {
                visit_values_row(self, row)
            }

            fn visit_copy(
                &mut self,
                table_name: &'ast $($mut)* ObjectName,
                columns: &'ast $($mut)* [Ident],
                values: &'ast $($mut)* [Option<String>],
            ) {
                visit_copy(self, table_name, columns, values)
            }

            fn visit_copy_values(&mut self, values: &'ast $($mut)* [Option<String>]) {
                visit_copy_values(self, values)
            }

            fn visit_copy_values_row(&mut self, _row: Option<&$($mut)* String>) {}

            fn visit_update(
                &mut self,
                table_name: &'ast $($mut)* ObjectName,
                assignments: &'ast $($mut)* [Assignment],
                selection: Option<&'ast $($mut)* Expr>,
            ) {
                visit_update(self, table_name, assignments, selection)
            }

            fn visit_assignment(&mut self, assignment: &'ast $($mut)* Assignment) {
                visit_assignment(self, assignment)
            }

            fn visit_delete(&mut self, table_name: &'ast $($mut)* ObjectName, selection: Option<&'ast $($mut)* Expr>) {
                visit_delete(self, table_name, selection)
            }

            fn visit_literal_string(&mut self, _string: &'ast $($mut)* String) {}

            fn visit_path(&mut self, _path: &'ast $($mut)* PathBuf) {}

            fn visit_create_database(
                &mut self,
                name: &'ast $($mut)* Ident,
                if_not_exists: bool,
            ) {
                visit_create_database(self, name, if_not_exists)
            }

            fn visit_create_schema(
                &mut self,
                name: &'ast $($mut)* ObjectName,
                if_not_exists: bool,
            ) {
                visit_create_schema(self, name, if_not_exists)
            }

            fn visit_create_source(
                &mut self,
                name: &'ast $($mut)* ObjectName,
                connector: &'ast $($mut)* Connector,
                with_options: &'ast $($mut)* [SqlOption],
                format: Option<&'ast $($mut)* Format>,
                envelope: &'ast $($mut)* Envelope,
                if_not_exists: bool,
                materialized: bool,
            ) {
                visit_create_source(self, name, connector, with_options, format, envelope, if_not_exists, materialized)
            }

            fn visit_connector(
                &mut self,
                connector: &'ast $($mut)* Connector,
            ) {
                visit_connector(self, connector)
            }

            fn visit_format(
                &mut self,
                format: &'ast $($mut)* Format,
            ) {
                visit_format(self, format)
            }

            fn visit_source_envelope(
                &mut self,
                _envelope: &'ast $($mut)* Envelope,
            ) { }

            fn visit_avro_schema(
                &mut self,
                avro_schema: &'ast $($mut)* AvroSchema,
            ) {
                visit_avro_schema(self, avro_schema)
            }

            fn visit_csr_seed(
                &mut self,
                csr_seed: &'ast $($mut)* CsrSeed,
            ) {
                visit_csr_seed(self, csr_seed)
            }

            fn visit_schema(
                &mut self,
                schema: &'ast $($mut)* Schema,
            ) {
                visit_schema(self, schema)
            }

            fn visit_create_sink(
                &mut self,
                name: &'ast $($mut)* ObjectName,
                from: &'ast $($mut)* ObjectName,
                connector: &'ast $($mut)* Connector,
                format: &'ast $($mut)* Format,
                if_not_exists: bool,
            ) {
                visit_create_sink(self, name, from, connector, format, if_not_exists)
            }

            fn visit_create_view(
                &mut self,
                name: &'ast $($mut)* ObjectName,
                columns: &'ast $($mut)* [Ident],
                query: &'ast $($mut)* Query,
                materialized: bool,
                if_exists: IfExistsBehavior,
                with_options: &'ast $($mut)* [SqlOption],
            ) {
                visit_create_view(self, name, columns, query, materialized, if_exists, with_options)
            }

            fn visit_create_index(
                &mut self,
                name: &'ast $($mut)* Ident,
                on_name: &'ast $($mut)* ObjectName,
                key_parts: &'ast $($mut)* Vec<Expr>,
                if_not_exists: bool,
            ){
                visit_create_index(self, name, on_name, key_parts, if_not_exists)
            }

            fn visit_create_table(
                &mut self,
                name: &'ast $($mut)* ObjectName,
                columns: &'ast $($mut)* [ColumnDef],
                constraints: &'ast $($mut)* [TableConstraint],
                with_options: &'ast $($mut)* [SqlOption],
                if_not_exists: bool,
            ) {
                visit_create_table(
                    self,
                    name,
                    columns,
                    constraints,
                    with_options,
                    if_not_exists,
                )
            }

            fn visit_column_def(&mut self, column_def: &'ast $($mut)* ColumnDef) {
                visit_column_def(self, column_def)
            }

            fn visit_column_option_def(&mut self, column_option_def: &'ast $($mut)* ColumnOptionDef) {
                visit_column_option_def(self, column_option_def)
            }

            fn visit_column_option(&mut self, column_option: &'ast $($mut)* ColumnOption) {
                visit_column_option(self, column_option)
            }

            fn visit_option(&mut self, option: &'ast $($mut)* SqlOption) {
                visit_option(self, option)
            }

            fn visit_drop_database(
                &mut self,
                name: &'ast $($mut)* Ident,
                if_exists: bool,
            ) {
                visit_drop_database(self, name, if_exists)
            }

            fn visit_drop_objects(
                &mut self,
                object_type: ObjectType,
                if_exists: bool,
                names: &'ast $($mut)* [ObjectName],
                cascade: bool,
            ) {
                visit_drop_objects(self, object_type, if_exists, names, cascade)
            }

            fn visit_object_type(&mut self, _object_type: ObjectType) {}

            fn visit_alter_table(&mut self, name: &'ast $($mut)* ObjectName, operation: &'ast $($mut)* AlterTableOperation) {
                visit_alter_table(self, name, operation)
            }

            fn visit_alter_table_operation(&mut self, operation: &'ast $($mut)* AlterTableOperation) {
                visit_alter_table_operation(self, operation)
            }

            fn visit_alter_add_constraint(&mut self, table_constraint: &'ast $($mut)* TableConstraint) {
                visit_alter_add_constraint(self, table_constraint)
            }

            fn visit_table_constraint(&mut self, table_constraint: &'ast $($mut)* TableConstraint) {
                visit_table_constraint(self, table_constraint)
            }

            fn visit_table_constraint_unique(
                &mut self,
                name: Option<&'ast $($mut)* Ident>,
                columns: &'ast $($mut)* [Ident],
                is_primary: bool,
            ) {
                visit_table_constraint_unique(self, name, columns, is_primary)
            }

            fn visit_table_constraint_foreign_key(
                &mut self,
                name: Option<&'ast $($mut)* Ident>,
                columns: &'ast $($mut)* [Ident],
                foreign_table: &'ast $($mut)* ObjectName,
                referred_columns: &'ast $($mut)* [Ident],
            ) {
                visit_table_constraint_foreign_key(self, name, columns, foreign_table, referred_columns)
            }

            fn visit_table_constraint_check(&mut self, name: Option<&'ast $($mut)* Ident>, expr: &'ast $($mut)* Expr) {
                visit_table_constraint_check(self, name, expr)
            }

            fn visit_alter_drop_constraint(&mut self, name: &'ast $($mut)* Ident) {
                visit_alter_drop_constraint(self, name)
            }

            fn visit_set_variable(
                &mut self,
                local: bool,
                variable: &'ast $($mut)* Ident,
                value: &'ast $($mut)* SetVariableValue,
            ) {
                visit_set_variable(self, local, variable, value)
            }

            fn visit_set_variable_value(&mut self, value: &'ast $($mut)* SetVariableValue) {
                visit_set_variable_value(self, value)
            }

            fn visit_show_variable(&mut self, variable: &'ast $($mut)* Ident) {
                visit_show_variable(self, variable)
            }

            fn visit_show_databases(&mut self, filter: Option<&'ast $($mut)* ShowStatementFilter>) {
                visit_show_databases(self, filter)
            }

            fn visit_show_objects(
                &mut self,
                extended: bool,
                full: bool,
                materialized: bool,
                object_type: ObjectType,
                from: Option<&'ast $($mut)* ObjectName>,
                filter: Option<&'ast $($mut)* ShowStatementFilter>
            ) {
                visit_show_objects(self, extended, full, materialized, object_type, from, filter)
            }

            fn visit_show_indexes(&mut self, extended: bool, table_name: &'ast $($mut)* ObjectName, filter: Option<&'ast $($mut)* ShowStatementFilter>) {
                visit_show_indexes(self, extended, table_name, filter)
            }

            fn visit_show_columns(
                &mut self,
                extended: bool,
                full: bool,
                table_name: &'ast $($mut)* ObjectName,
                filter: Option<&'ast $($mut)* ShowStatementFilter>,
            ) {
                visit_show_columns(self, extended, full, table_name, filter)
            }

            fn visit_show_create_view(
                &mut self,
                view_name: &'ast $($mut)* ObjectName,
            ) {
                visit_show_create_view(self, view_name)
            }

            fn visit_show_create_source(
                &mut self,
                source_name: &'ast $($mut)* ObjectName,
            ) {
                visit_show_create_source(self, source_name)
            }

            fn visit_show_create_sink(
                &mut self,
                sink_name: &'ast $($mut)* ObjectName,
            ) {
                visit_show_create_sink(self, sink_name)
            }

            fn visit_show_statement_filter(&mut self, filter: &'ast $($mut)* ShowStatementFilter) {
                visit_show_statement_filter(self, filter)
            }

            fn visit_start_transaction(&mut self, modes: &'ast $($mut)* [TransactionMode]) {
                visit_start_transaction(self, modes)
            }

            fn visit_set_transaction(&mut self, modes: &'ast $($mut)* [TransactionMode]) {
                visit_set_transaction(self, modes)
            }

            fn visit_transaction_mode(&mut self, mode: &'ast $($mut)* TransactionMode) {
                visit_transaction_mode(self, mode)
            }

            fn visit_transaction_access_mode(&mut self, _access_mode: &'ast $($mut)* TransactionAccessMode) {}

            fn visit_transaction_isolation_level(
                &mut self,
                _isolation_level: &'ast $($mut)* TransactionIsolationLevel,
            ) {

            }

            fn visit_commit(&mut self, _chain: bool) {}

            fn visit_rollback(&mut self, _chain: bool) {}

            fn visit_tail(&mut self, name: &'ast $($mut)* ObjectName) {
                visit_tail(self, name)
            }

            fn visit_explain(&mut self, stage: &'ast $($mut)* Stage, query: &'ast $($mut)* Query) {
                visit_explain(self, stage, query)
            }
        }

        pub fn visit_statement<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, statement: &'ast $($mut)* Statement) {
            match statement {
                Statement::Query(query) => visitor.visit_query(query),
                Statement::Insert {
                    table_name,
                    columns,
                    source,
                } => visitor.visit_insert(table_name, columns, source),
                Statement::Copy {
                    table_name,
                    columns,
                    values,
                } => visitor.visit_copy(table_name, columns, values),
                Statement::Update {
                    table_name,
                    assignments,
                    selection,
                } => visitor.visit_update(table_name, assignments, selection.as_auto_ref()),
                Statement::Delete {
                    table_name,
                    selection,
                } => visitor.visit_delete(table_name, selection.as_auto_ref()),
                Statement::CreateDatabase { name, if_not_exists } => {
                    visitor.visit_create_database(name, *if_not_exists)
                }
                Statement::CreateSchema { name, if_not_exists } => {
                    visitor.visit_create_schema(name, *if_not_exists)
                }
                Statement::CreateSource {
                    name,
                    connector,
                    with_options,
                    format,
                    envelope,
                    if_not_exists,
                    materialized,
                } => visitor.visit_create_source(name, connector, with_options, format.as_auto_ref(), envelope, *if_not_exists, *materialized),
                Statement::CreateSink {
                    name,
                    from,
                    connector,
                    format,
                    if_not_exists,
                } => visitor.visit_create_sink(name, from, connector, format, *if_not_exists),
                Statement::CreateView {
                    name,
                    columns,
                    query,
                    materialized,
                    if_exists,
                    with_options,
                } => visitor.visit_create_view(name, columns, query, *materialized, *if_exists, with_options),
                Statement::CreateIndex {
                    name,
                    on_name,
                    key_parts,
                    if_not_exists,
                } => visitor.visit_create_index(name, on_name, key_parts, *if_not_exists),
                Statement::DropDatabase { name, if_exists } => visitor.visit_drop_database(name, *if_exists),
                Statement::DropObjects {
                    object_type,
                    if_exists,
                    names,
                    cascade,
                } => visitor.visit_drop_objects(*object_type, *if_exists, names, *cascade),
                Statement::CreateTable {
                    name,
                    columns,
                    constraints,
                    with_options,
                    if_not_exists,
                } => visitor.visit_create_table(
                    name,
                    columns,
                    constraints,
                    with_options,
                    *if_not_exists,
                ),
                Statement::AlterTable { name, operation } => visitor.visit_alter_table(name, operation),
                Statement::SetVariable {
                    local,
                    variable,
                    value,
                } => visitor.visit_set_variable(*local, variable, value),
                Statement::ShowVariable { variable } => visitor.visit_show_variable(variable),
                Statement::ShowDatabases { filter } => {
                    visitor.visit_show_databases(filter.as_auto_ref())
                }
                Statement::ShowObjects { object_type, extended, full, materialized, from, filter } => {
                    visitor.visit_show_objects(*extended, *full, *materialized, *object_type, from.as_auto_ref(), filter.as_auto_ref())
                }
                Statement::ShowIndexes { table_name, extended, filter } => {
                    visitor.visit_show_indexes(*extended, table_name, filter.as_auto_ref())
                }
                Statement::ShowColumns {
                    extended,
                    full,
                    table_name,
                    filter,
                } => visitor.visit_show_columns(*extended, *full, table_name, filter.as_auto_ref()),
                Statement::ShowCreateView { view_name } => visitor.visit_show_create_view(view_name),
                Statement::ShowCreateSource { source_name } => visitor.visit_show_create_source(source_name),
                Statement::ShowCreateSink { sink_name } => visitor.visit_show_create_sink(sink_name),
                Statement::StartTransaction { modes } => visitor.visit_start_transaction(modes),
                Statement::SetTransaction { modes } => visitor.visit_set_transaction(modes),
                Statement::Commit { chain } => visitor.visit_commit(*chain),
                Statement::Rollback { chain } => visitor.visit_rollback(*chain),
                Statement::Tail { name } => {
                    visitor.visit_tail(name);
                }
                Statement::Explain { stage, query } => visitor.visit_explain(stage, query),
            }
        }

        pub fn visit_query<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, query: &'ast $($mut)* Query) {
            for cte in &$($mut)* query.ctes {
                visitor.visit_cte(cte);
            }
            visitor.visit_set_expr(&$($mut)* query.body);
            for order_by in &$($mut)* query.order_by {
                visitor.visit_order_by(order_by);
            }
            if let Some(limit) = &$($mut)* query.limit {
                visitor.visit_limit(limit);
            }
        }

        pub fn visit_cte<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, cte: &'ast $($mut)* Cte) {
            visitor.visit_table_alias(&$($mut)* cte.alias);
            visitor.visit_query(&$($mut)* cte.query);
        }

        pub fn visit_select<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, select: &'ast $($mut)* Select) {
            for select_item in &$($mut)* select.projection {
                visitor.visit_select_item(select_item)
            }
            for table_with_joins in &$($mut)* select.from {
                visitor.visit_table_with_joins(table_with_joins)
            }
            if let Some(selection) = &$($mut)* select.selection {
                visitor.visit_where(selection);
            }
            if !select.group_by.is_empty() {
                visitor.visit_group_by(&$($mut)* select.group_by);
            }
            if let Some(having) = &$($mut)* select.having {
                visitor.visit_having(having);
            }
        }

        pub fn visit_select_item<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            select_item: &'ast $($mut)* SelectItem,
        ) {
            match select_item {
                SelectItem::UnnamedExpr(expr) => visitor.visit_unnamed_expr(expr),
                SelectItem::ExprWithAlias { expr, alias } => visitor.visit_expr_with_alias(expr, alias),
                SelectItem::QualifiedWildcard(object_name) => {
                    visitor.visit_qualified_wildcard(&$($mut)* object_name.0)
                }
                SelectItem::Wildcard => visitor.visit_wildcard(),
            }
        }

        pub fn visit_table_with_joins<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            table_with_joins: &'ast $($mut)* TableWithJoins,
        ) {
            visitor.visit_table_factor(&$($mut)* table_with_joins.relation);
            for join in &$($mut)* table_with_joins.joins {
                visitor.visit_join(join);
            }
        }

        pub fn visit_table_factor<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            table_factor: &'ast $($mut)* TableFactor,
        ) {
            match table_factor {
                TableFactor::Table {
                    name,
                    alias,
                    args,
                    with_hints,
                } => visitor.visit_table_table_factor(name, alias.as_auto_ref(), args, with_hints),
                TableFactor::Derived {
                    lateral,
                    subquery,
                    alias,
                } => visitor.visit_derived_table_factor(*lateral, subquery, alias.as_auto_ref()),
                TableFactor::NestedJoin(table_with_joins) => {
                    visitor.visit_nested_join_table_factor(table_with_joins)
                }
            }
        }

        pub fn visit_table_table_factor<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: &'ast $($mut)* ObjectName,
            alias: Option<&'ast $($mut)* TableAlias>,
            args: &'ast $($mut)* [Expr],
            with_hints: &'ast $($mut)* [Expr],
        ) {
            visitor.visit_object_name(name);
            for expr in args {
                visitor.visit_expr(expr);
            }
            if let Some(alias) = alias {
                visitor.visit_table_alias(alias);
            }
            for expr in with_hints {
                visitor.visit_expr(expr);
            }
        }

        pub fn visit_derived_table_factor<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            _lateral: bool,
            subquery: &'ast $($mut)* Query,
            alias: Option<&'ast $($mut)* TableAlias>,
        ) {
            visitor.visit_subquery(subquery);
            if let Some(alias) = alias {
                visitor.visit_table_alias(alias);
            }
        }

        pub fn visit_nested_join_table_factor<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            table_with_joins: &'ast $($mut)* TableWithJoins,
        ) {
            visitor.visit_table_with_joins(table_with_joins);
        }

        pub fn visit_table_alias<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, alias: &'ast $($mut)* TableAlias) {
            visitor.visit_ident(&$($mut)* alias.name);
            for column in &$($mut)* alias.columns {
                visitor.visit_ident(column);
            }
        }

        pub fn visit_join<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, join: &'ast $($mut)* Join) {
            visitor.visit_table_factor(&$($mut)* join.relation);
            visitor.visit_join_operator(&$($mut)* join.join_operator);
        }

        pub fn visit_join_operator<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, op: &'ast $($mut)* JoinOperator) {
            match op {
                JoinOperator::Inner(constraint) => visitor.visit_join_constraint(constraint),
                JoinOperator::LeftOuter(constraint) => visitor.visit_join_constraint(constraint),
                JoinOperator::RightOuter(constraint) => visitor.visit_join_constraint(constraint),
                JoinOperator::FullOuter(constraint) => visitor.visit_join_constraint(constraint),
                JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => (),
            }
        }

        pub fn visit_join_constraint<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            constraint: &'ast $($mut)* JoinConstraint,
        ) {
            match constraint {
                JoinConstraint::On(expr) => visitor.visit_expr(expr),
                JoinConstraint::Using(idents) => {
                    for ident in idents {
                        visitor.visit_ident(ident);
                    }
                }
                JoinConstraint::Natural => (),
            }
        }

        pub fn visit_where<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, expr: &'ast $($mut)* Expr) {
            visitor.visit_expr(expr);
        }

        pub fn visit_group_by<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, exprs: &'ast $($mut)* [Expr]) {
            for expr in exprs {
                visitor.visit_expr(expr);
            }
        }

        pub fn visit_having<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, expr: &'ast $($mut)* Expr) {
            visitor.visit_expr(expr);
        }

        pub fn visit_set_expr<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, set_expr: &'ast $($mut)* SetExpr) {
            match set_expr {
                SetExpr::Select(select) => visitor.visit_select(select),
                SetExpr::Query(query) => visitor.visit_query(query),
                SetExpr::Values(values) => visitor.visit_values(values),
                SetExpr::SetOperation {
                    left,
                    op,
                    right,
                    all,
                } => visitor.visit_set_operation(left, op, right, *all),
            }
        }

        pub fn visit_set_operation<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            left: &'ast $($mut)* SetExpr,
            op: &'ast $($mut)* SetOperator,
            right: &'ast $($mut)* SetExpr,
            _all: bool,
        ) {
            visitor.visit_set_expr(left);
            visitor.visit_set_operator(op);
            visitor.visit_set_expr(right);
        }

        pub fn visit_order_by<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, order_by: &'ast $($mut)* OrderByExpr) {
            visitor.visit_expr(&$($mut)* order_by.expr);
        }

        pub fn visit_limit<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, expr: &'ast $($mut)* Expr) {
            visitor.visit_expr(expr)
        }

        pub fn visit_expr<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, expr: &'ast $($mut)* Expr) {
            match expr {
                Expr::Identifier(ident) => visitor.visit_ident(ident),
                Expr::Wildcard => visitor.visit_wildcard(),
                Expr::QualifiedWildcard(idents) => visitor.visit_qualified_wildcard(idents),
                Expr::CompoundIdentifier(idents) => visitor.visit_compound_identifier(idents),
                Expr::Parameter(n) => visitor.visit_parameter(*n),
                Expr::IsNull(expr) => visitor.visit_is_null(expr),
                Expr::IsNotNull(expr) => visitor.visit_is_not_null(expr),
                Expr::InList {
                    expr,
                    list,
                    negated,
                } => visitor.visit_in_list(expr, list, *negated),
                Expr::InSubquery {
                    expr,
                    subquery,
                    negated,
                } => visitor.visit_in_subquery(expr, subquery, *negated),
                Expr::Between {
                    expr,
                    negated,
                    low,
                    high,
                } => visitor.visit_between(expr, low, high, *negated),
                Expr::BinaryOp { left, op, right } => visitor.visit_binary_op(left, op, right),
                Expr::UnaryOp { expr, op } => visitor.visit_unary_op(expr, op),
                Expr::Cast { expr, data_type } => visitor.visit_cast(expr, data_type),
                Expr::Collate { expr, collation } => visitor.visit_collate(expr, collation),
                Expr::Extract { field, expr } => visitor.visit_extract(field, expr),
                Expr::Nested(expr) => visitor.visit_nested(expr),
                Expr::Value(val) => visitor.visit_value(val),
                Expr::Function(func) => visitor.visit_function(func),
                Expr::Case {
                    operand,
                    conditions,
                    results,
                    else_result,
                } => visitor.visit_case(
                    operand.as_auto_ref().map(|o| o.as_auto_ref()),
                    conditions,
                    results,
                    else_result.as_auto_ref().map(|r| r.as_auto_ref()),
                ),
                Expr::Exists(query) => visitor.visit_exists(query),
                Expr::Subquery(query) => visitor.visit_subquery(query),
                Expr::Any{left, op, right, some: _} => visitor.visit_any(left, op, right),
                Expr::All{left, op, right} => visitor.visit_all(left, op, right),
            }
        }

        pub fn visit_unnamed_expr<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, expr: &'ast $($mut)* Expr) {
            visitor.visit_expr(expr);
        }

        pub fn visit_expr_with_alias<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            expr: &'ast $($mut)* Expr,
            alias: &'ast $($mut)* Ident,
        ) {
            visitor.visit_expr(expr);
            visitor.visit_ident(alias);
        }

        pub fn visit_object_name<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            object_name: &'ast $($mut)* ObjectName,
        ) {
            for ident in &$($mut)* object_name.0 {
                visitor.visit_ident(ident)
            }
        }

        pub fn visit_compound_identifier<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            idents: &'ast $($mut)* [Ident],
        ) {
            for ident in idents {
                visitor.visit_ident(ident);
            }
        }

        pub fn visit_qualified_wildcard<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            idents: &'ast $($mut)* [Ident],
        ) {
            for ident in idents {
                visitor.visit_ident(ident);
            }
        }

        pub fn visit_parameter<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            n: usize,
        ) {
            visitor.visit_parameter(n)
        }

        pub fn visit_is_null<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, expr: &'ast $($mut)* Expr) {
            visitor.visit_expr(expr);
        }

        pub fn visit_is_not_null<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, expr: &'ast $($mut)* Expr) {
            visitor.visit_expr(expr);
        }

        pub fn visit_in_list<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            expr: &'ast $($mut)* Expr,
            list: &'ast $($mut)* [Expr],
            _negated: bool,
        ) {
            visitor.visit_expr(expr);
            for e in list {
                visitor.visit_expr(e);
            }
        }

        pub fn visit_in_subquery<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            expr: &'ast $($mut)* Expr,
            subquery: &'ast $($mut)* Query,
            _negated: bool,
        ) {
            visitor.visit_expr(expr);
            visitor.visit_query(subquery);
        }

        pub fn visit_between<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            expr: &'ast $($mut)* Expr,
            low: &'ast $($mut)* Expr,
            high: &'ast $($mut)* Expr,
            _negated: bool,
        ) {
            visitor.visit_expr(expr);
            visitor.visit_expr(low);
            visitor.visit_expr(high);
        }

        pub fn visit_binary_op<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            left: &'ast $($mut)* Expr,
            op: &'ast $($mut)* BinaryOperator,
            right: &'ast $($mut)* Expr,
        ) {
            visitor.visit_expr(left);
            visitor.visit_binary_operator(op);
            visitor.visit_expr(right);
        }

        pub fn visit_unary_op<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            expr: &'ast $($mut)* Expr,
            op: &'ast $($mut)* UnaryOperator,
        ) {
            visitor.visit_expr(expr);
            visitor.visit_unary_operator(op);
        }

        pub fn visit_cast<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            expr: &'ast $($mut)* Expr,
            data_type: &'ast $($mut)* DataType,
        ) {
            visitor.visit_expr(expr);
            visitor.visit_type(data_type);
        }

        pub fn visit_collate<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            expr: &'ast $($mut)* Expr,
            collation: &'ast $($mut)* ObjectName,
        ) {
            visitor.visit_expr(expr);
            visitor.visit_object_name(collation);
        }

        pub fn visit_extract<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            field: &'ast $($mut)* ExtractField,
            expr: &'ast $($mut)* Expr,
        ) {
            visitor.visit_extract_field(field);
            visitor.visit_expr(expr);
        }

        pub fn visit_nested<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, expr: &'ast $($mut)* Expr) {
            visitor.visit_expr(expr);
        }

        pub fn visit_function<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, func: &'ast $($mut)* Function) {
            visitor.visit_object_name(&$($mut)* func.name);
            for arg in &$($mut)* func.args {
                visitor.visit_expr(arg);
            }
            if let Some(over) = &$($mut)* func.over {
                visitor.visit_window_spec(over);
            }
        }

        pub fn visit_window_spec<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            window_spec: &'ast $($mut)* WindowSpec,
        ) {
            for expr in &$($mut)* window_spec.partition_by {
                visitor.visit_expr(expr);
            }
            for order_by in &$($mut)* window_spec.order_by {
                visitor.visit_order_by(order_by);
            }
            if let Some(window_frame) = &$($mut)* window_spec.window_frame {
                visitor.visit_window_frame(window_frame);
            }
        }

        pub fn visit_window_frame<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            window_frame: &'ast $($mut)* WindowFrame,
        ) {
            visitor.visit_window_frame_units(&$($mut)* window_frame.units);
            visitor.visit_window_frame_bound(&$($mut)* window_frame.start_bound);
            if let Some(end_bound) = &$($mut)* window_frame.end_bound {
                visitor.visit_window_frame_bound(end_bound);
            }
        }

        pub fn visit_case<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            operand: Option<&'ast $($mut)* Expr>,
            conditions: &'ast $($mut)* [Expr],
            results: &'ast $($mut)* [Expr],
            else_result: Option<&'ast $($mut)* Expr>,
        ) {
            if let Some(operand) = operand {
                visitor.visit_expr(operand);
            }
            for cond in conditions {
                visitor.visit_expr(cond);
            }
            for res in results {
                visitor.visit_expr(res);
            }
            if let Some(else_result) = else_result {
                visitor.visit_expr(else_result);
            }
        }

        pub fn visit_exists<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, subquery: &'ast $($mut)* Query) {
            visitor.visit_query(subquery)
        }

        pub fn visit_subquery<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, subquery: &'ast $($mut)* Query) {
            visitor.visit_query(subquery)
        }

        pub fn visit_any<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, left: &'ast $($mut)* Expr, op: &'ast $($mut)* BinaryOperator, right: &'ast $($mut)* Query) {
            visitor.visit_expr(left);
            visitor.visit_binary_operator(op);
            visitor.visit_query(right);
        }

        pub fn visit_all<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, left: &'ast $($mut)* Expr, op: &'ast $($mut)* BinaryOperator, right: &'ast $($mut)* Query) {
            visitor.visit_expr(left);
            visitor.visit_binary_operator(op);
            visitor.visit_query(right);
        }

        pub fn visit_insert<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            table_name: &'ast $($mut)* ObjectName,
            columns: &'ast $($mut)* [Ident],
            source: &'ast $($mut)* Query,
        ) {
            visitor.visit_object_name(table_name);
            for column in columns {
                visitor.visit_ident(column);
            }
            visitor.visit_query(source);
        }

        pub fn visit_values<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, values: &'ast $($mut)* Values) {
            for row in &$($mut)* values.0 {
                visitor.visit_values_row(row)
            }
        }

        pub fn visit_values_row<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, row: &'ast $($mut)* [Expr]) {
            for expr in row {
                visitor.visit_expr(expr)
            }
        }

        pub fn visit_copy<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            table_name: &'ast $($mut)* ObjectName,
            columns: &'ast $($mut)* [Ident],
            values: &'ast $($mut)* [Option<String>],
        ) {
            visitor.visit_object_name(table_name);
            for column in columns {
                visitor.visit_ident(column);
            }
            visitor.visit_copy_values(values);
        }

        pub fn visit_copy_values<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            values: &'ast $($mut)* [Option<String>],
        ) {
            for value in values {
                visitor.visit_copy_values_row(value.as_auto_ref());
            }
        }

        pub fn visit_update<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            table_name: &'ast $($mut)* ObjectName,
            assignments: &'ast $($mut)* [Assignment],
            selection: Option<&'ast $($mut)* Expr>,
        ) {
            visitor.visit_object_name(table_name);
            for assignment in assignments {
                visitor.visit_assignment(assignment);
            }
            if let Some(selection) = selection {
                visitor.visit_where(selection);
            }
        }

        pub fn visit_assignment<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            assignment: &'ast $($mut)* Assignment,
        ) {
            visitor.visit_ident(&$($mut)* assignment.id);
            visitor.visit_expr(&$($mut)* assignment.value);
        }

        pub fn visit_delete<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            table_name: &'ast $($mut)* ObjectName,
            selection: Option<&'ast $($mut)* Expr>,
        ) {
            visitor.visit_object_name(table_name);
            if let Some(selection) = selection {
                visitor.visit_where(selection);
            }
        }

        pub fn visit_create_database<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: &'ast $($mut)* Ident,
            _if_not_exists: bool,
        ) {
            visitor.visit_ident(name);
        }

        pub fn visit_create_schema<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: &'ast $($mut)* ObjectName,
            _if_not_exists: bool,
        ) {
            visitor.visit_object_name(name);
        }

        pub fn visit_create_source<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: &'ast $($mut)* ObjectName,
            connector: &'ast $($mut)* Connector,
            with_options: &'ast $($mut)* [SqlOption],
            format: Option<&'ast $($mut)* Format>,
            envelope: &'ast $($mut)* Envelope,
            _if_not_exists: bool,
            _materialized: bool,
        ) {
            visitor.visit_object_name(name);
            visitor.visit_connector(connector);
            for option in with_options {
                visitor.visit_option(option);
            }
            if let Some(format) = format {
                visitor.visit_format(format);
            }
            visitor.visit_source_envelope(envelope);
        }

        pub fn visit_connector<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            connector: &'ast $($mut)* Connector,
        ) {
            match connector {
                Connector::File { path } | Connector::AvroOcf { path } => {
                    visitor.visit_literal_string(path);
                }
                Connector::Kafka { broker, topic } => {
                    visitor.visit_literal_string(broker);
                    visitor.visit_literal_string(topic);
                }
                Connector::Kinesis { arn } => {
                    visitor.visit_literal_string(arn);
                }
            }
        }

        pub fn visit_format<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            format: &'ast $($mut)* Format,
        ) {
            use Format::*;
            match format {
                Bytes | Json | Text | Csv { .. } => (),
                Avro(avro_schema) => visitor.visit_avro_schema(avro_schema),
                Protobuf {message_name, schema} => {
                    visitor.visit_literal_string(message_name);
                    visitor.visit_schema(schema);
                },
                Regex(regex) => visitor.visit_literal_string(regex),
            }
        }

        pub fn visit_avro_schema<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            avro_schema: &'ast $($mut)* AvroSchema,
        ) {
            match avro_schema {
                AvroSchema::CsrUrl { url, seed } => {
                    visitor.visit_literal_string(url);
                    if let Some(seed) = seed {
                        visitor.visit_csr_seed(seed);
                    }
                }
                AvroSchema::Schema(schema) => visitor.visit_schema(schema),
            }
        }

        pub fn visit_csr_seed<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            csr_seed: &'ast $($mut)* CsrSeed,
        ) {
            if let Some(key_schema) = &$($mut)* csr_seed.key_schema {
                visitor.visit_literal_string(key_schema);
            }
            visitor.visit_literal_string(&$($mut)* csr_seed.value_schema);
        }

        pub fn visit_schema<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            schema: &'ast $($mut)* Schema,
        ) {
            match schema {
                Schema::File(pb) => visitor.visit_path(pb),
                Schema::Inline(inner) => visitor.visit_literal_string(inner),
            }
        }

        pub fn visit_create_sink<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: &'ast $($mut)* ObjectName,
            from: &'ast $($mut)* ObjectName,
            connector: &'ast $($mut)* Connector,
            format: &'ast $($mut)* Format,
            _if_not_exists: bool,
        ) {
            visitor.visit_object_name(name);
            visitor.visit_object_name(from);
            visitor.visit_connector(connector);
            visitor.visit_format(format);
        }

        pub fn visit_drop_database<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: &'ast $($mut)* Ident,
            _if_exists: bool,
        ) {
            visitor.visit_ident(name);
        }

        pub fn visit_drop_objects<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            object_type: ObjectType,
            _if_exists: bool,
            names: &'ast $($mut)* [ObjectName],
            _cascade: bool,
        ) {
            visitor.visit_object_type(object_type);
            for name in names {
                visitor.visit_object_name(name);
            }
        }

        pub fn visit_create_view<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: &'ast $($mut)* ObjectName,
            columns: &'ast $($mut)* [Ident],
            query: &'ast $($mut)* Query,
            _materialized: bool,
            _if_exists: IfExistsBehavior,
            with_options: &'ast $($mut)* [SqlOption],
        ) {
            visitor.visit_object_name(name);
            for column in columns {
                visitor.visit_ident(column);
            }
            for option in with_options {
                visitor.visit_option(option);
            }
            visitor.visit_query(query);
        }

        pub fn visit_create_index<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: &'ast $($mut)* Ident,
            on_name: &'ast $($mut)* ObjectName,
            key_parts: &'ast $($mut)* Vec<Expr>,
            _if_not_exists: bool,
        ) {
            visitor.visit_ident(name);
            visitor.visit_object_name(on_name);
            for key_part in key_parts {
                visitor.visit_expr(key_part);
            }
        }

        pub fn visit_create_table<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: &'ast $($mut)* ObjectName,
            columns: &'ast $($mut)* [ColumnDef],
            constraints: &'ast $($mut)* [TableConstraint],
            with_options: &'ast $($mut)* [SqlOption],
            _if_not_exists: bool,
        ) {
            visitor.visit_object_name(name);
            for column in columns {
                visitor.visit_column_def(column);
            }
            for constraint in constraints {
                visitor.visit_table_constraint(constraint);
            }
            for option in with_options {
                visitor.visit_option(option);
            }
        }

        pub fn visit_column_def<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            column_def: &'ast $($mut)* ColumnDef,
        ) {
            visitor.visit_ident(&$($mut)* column_def.name);
            visitor.visit_type(&$($mut)* column_def.data_type);
            for option in &$($mut)* column_def.options {
                visitor.visit_column_option_def(option);
            }
        }

        pub fn visit_column_option_def<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            column_option_def: &'ast $($mut)* ColumnOptionDef,
        ) {
            if let Some(name) = &$($mut)* column_option_def.name {
                visitor.visit_ident(name);
            }
            visitor.visit_column_option(&$($mut)* column_option_def.option)
        }

        pub fn visit_column_option<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            column_option: &'ast $($mut)* ColumnOption,
        ) {
            match column_option {
                ColumnOption::Null | ColumnOption::NotNull | ColumnOption::Unique { .. } => (),
                ColumnOption::Default(expr) | ColumnOption::Check(expr) => visitor.visit_expr(expr),
                ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                } => {
                    visitor.visit_object_name(foreign_table);
                    for column in referred_columns {
                        visitor.visit_ident(column);
                    }
                }
            }
        }

        pub fn visit_option<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, option: &'ast $($mut)* SqlOption) {
            visitor.visit_ident(&$($mut)* option.name);
            visitor.visit_value(&$($mut)* option.value);
        }

        pub fn visit_alter_table<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: &'ast $($mut)* ObjectName,
            operation: &'ast $($mut)* AlterTableOperation,
        ) {
            visitor.visit_object_name(name);
            visitor.visit_alter_table_operation(operation);
        }

        pub fn visit_alter_table_operation<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            operation: &'ast $($mut)* AlterTableOperation,
        ) {
            match operation {
                AlterTableOperation::AddConstraint(table_constraint) => {
                    visitor.visit_alter_add_constraint(table_constraint)
                }
                AlterTableOperation::DropConstraint { name } => visitor.visit_alter_drop_constraint(name),
            }
        }

        pub fn visit_alter_add_constraint<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            table_constraint: &'ast $($mut)* TableConstraint,
        ) {
            visitor.visit_table_constraint(table_constraint);
        }

        pub fn visit_table_constraint<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            table_constraint: &'ast $($mut)* TableConstraint,
        ) {
            match table_constraint {
                TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                } => visitor.visit_table_constraint_unique(name.as_auto_ref(), columns, *is_primary),
                TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                } => visitor.visit_table_constraint_foreign_key(
                    name.as_auto_ref(),
                    columns,
                    foreign_table,
                    referred_columns,
                ),
                TableConstraint::Check { name, expr } => {
                    visitor.visit_table_constraint_check(name.as_auto_ref(), expr)
                }
            }
        }

        pub fn visit_table_constraint_unique<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: Option<&'ast $($mut)* Ident>,
            columns: &'ast $($mut)* [Ident],
            _is_primary: bool,
        ) {
            if let Some(name) = name {
                visitor.visit_ident(name);
            }
            for column in columns {
                visitor.visit_ident(column);
            }
        }

        pub fn visit_table_constraint_foreign_key<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: Option<&'ast $($mut)* Ident>,
            columns: &'ast $($mut)* [Ident],
            foreign_table: &'ast $($mut)* ObjectName,
            referred_columns: &'ast $($mut)* [Ident],
        ) {
            if let Some(name) = name {
                visitor.visit_ident(name);
            }
            for column in columns {
                visitor.visit_ident(column);
            }
            visitor.visit_object_name(foreign_table);
            for column in referred_columns {
                visitor.visit_ident(column);
            }
        }

        pub fn visit_table_constraint_check<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: Option<&'ast $($mut)* Ident>,
            expr: &'ast $($mut)* Expr,
        ) {
            if let Some(name) = name {
                visitor.visit_ident(name);
            }
            visitor.visit_expr(expr);
        }

        pub fn visit_alter_drop_constraint<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            name: &'ast $($mut)* Ident,
        ) {
            visitor.visit_ident(name);
        }

        pub fn visit_set_variable<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            _local: bool,
            variable: &'ast $($mut)* Ident,
            value: &'ast $($mut)* SetVariableValue,
        ) {
            visitor.visit_ident(variable);
            visitor.visit_set_variable_value(value);
        }

        pub fn visit_set_variable_value<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            value: &'ast $($mut)* SetVariableValue,
        ) {
            match value {
                SetVariableValue::Ident(ident) => visitor.visit_ident(ident),
                SetVariableValue::Literal(value) => visitor.visit_value(value),
            }
        }

        pub fn visit_show_variable<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, variable: &'ast $($mut)* Ident) {
            visitor.visit_ident(variable);
        }

        pub fn visit_show_databases<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            filter: Option<&'ast $($mut)* ShowStatementFilter>
        ) {
            if let Some(filter) = filter {
                visitor.visit_show_statement_filter(filter);
            }
        }

        pub fn visit_show_objects<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            _extended: bool,
            _full: bool,
            _materialized: bool,
            object_type: ObjectType,
            from: Option<&'ast $($mut)* ObjectName>,
            filter: Option<&'ast $($mut)* ShowStatementFilter>
        ) {
            visitor.visit_object_type(object_type);
            if let Some(from) = from {
                visitor.visit_object_name(from);
            }
            if let Some(filter) = filter {
                visitor.visit_show_statement_filter(filter);
            }
        }

        pub fn visit_show_indexes<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            _extended: bool,
            table_name: &'ast $($mut)* ObjectName,
            filter: Option<&'ast $($mut)* ShowStatementFilter>
        ) {
            visitor.visit_object_name(table_name);
            if let Some(filter) = filter {
                visitor.visit_show_statement_filter(filter);
            }
        }

        pub fn visit_show_columns<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            _extended: bool,
            _full: bool,
            table_name: &'ast $($mut)* ObjectName,
            filter: Option<&'ast $($mut)* ShowStatementFilter>,
        ) {
            visitor.visit_object_name(table_name);
            if let Some(filter) = filter {
                visitor.visit_show_statement_filter(filter);
            }
        }

        pub fn visit_show_create_view<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            view_name: &'ast $($mut)* ObjectName,
        ) {
            visitor.visit_object_name(view_name);
        }

        pub fn visit_show_create_source<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            source_name: &'ast $($mut)* ObjectName
        ) {
            visitor.visit_object_name(source_name);
        }

        pub fn visit_show_create_sink<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            sink_name: &'ast $($mut)* ObjectName
        ) {
            visitor.visit_object_name(sink_name);
        }

        pub fn visit_show_statement_filter<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            filter: &'ast $($mut)* ShowStatementFilter,
        ) {
            match filter {
                ShowStatementFilter::Like(pattern) => visitor.visit_literal_string(pattern),
                ShowStatementFilter::Where(expr) => visitor.visit_expr(expr),
            }
        }

        pub fn visit_start_transaction<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            modes: &'ast $($mut)* [TransactionMode],
        ) {
            for mode in modes {
                visitor.visit_transaction_mode(mode)
            }
        }

        pub fn visit_set_transaction<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            modes: &'ast $($mut)* [TransactionMode],
        ) {
            for mode in modes {
                visitor.visit_transaction_mode(mode)
            }
        }

        pub fn visit_transaction_mode<'ast, V: $name<'ast> + ?Sized>(
            visitor: &mut V,
            mode: &'ast $($mut)* TransactionMode,
        ) {
            match mode {
                TransactionMode::AccessMode(access_mode) => {
                    visitor.visit_transaction_access_mode(access_mode)
                }
                TransactionMode::IsolationLevel(isolation_level) => {
                    visitor.visit_transaction_isolation_level(isolation_level)
                }
            }
        }

        pub fn visit_tail<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, name: &'ast $($mut)* ObjectName) {
            visitor.visit_object_name(name);
        }

        pub fn visit_explain<'ast, V: $name<'ast> + ?Sized>(visitor: &mut V, _stage: &'ast $($mut)* Stage, query: &'ast $($mut)* Query) {
            visitor.visit_query(query);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::visit::Visit;
    use crate::ast::visit_mut::VisitMut;
    use crate::ast::Ident;
    use crate::parser::Parser;
    use std::error::Error;

    #[test]
    fn test_basic_visitor() -> Result<(), Box<dyn Error>> {
        struct Visitor<'a> {
            seen_idents: Vec<&'a str>,
        }

        impl<'a> Visit<'a> for Visitor<'a> {
            fn visit_ident(&mut self, ident: &'a Ident) {
                self.seen_idents.push(&ident.value);
            }
        }

        struct VisitorMut<'a> {
            seen_idents: Vec<&'a str>,
        }

        impl<'a> VisitMut<'a> for VisitorMut<'a> {
            fn visit_ident(&mut self, ident: &'a mut Ident) {
                self.seen_idents.push(&ident.value);
            }
        }

        let mut stmts = Parser::parse_sql(
            r#"
            WITH a01 AS (SELECT 1)
                SELECT *, a02.*, a03 AS a04
                FROM (SELECT * FROM a05) a06 (a07)
                JOIN a08 ON a09.a10 = a11.a12
                WHERE a13
                GROUP BY a14
                HAVING a15
            UNION ALL
                SELECT a16 IS NULL
                    AND a17 IS NOT NULL
                    AND a18 IN (a19)
                    AND a20 IN (SELECT * FROM a21)
                    AND CAST(a22 AS int)
                    AND (a23)
                    AND NOT a24
                    AND a25(a26)
                    AND CASE a27 WHEN a28 THEN a29 ELSE a30 END
                    AND a31 BETWEEN a32 AND a33
                    AND a34 COLLATE a35 = a36
                    AND EXTRACT(YEAR FROM a37)
                    AND (SELECT a38)
                    AND EXISTS (SELECT a39)
                FROM a40(a41) AS a42 WITH (a43)
                LEFT JOIN a44 ON false
                RIGHT JOIN a45 ON false
                FULL JOIN a46 ON false
                JOIN a47 (a48) USING (a49)
                NATURAL JOIN (a50 NATURAL JOIN a51)
            EXCEPT
                (SELECT a52(a53) OVER (PARTITION BY a54 ORDER BY a55 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING))
            ORDER BY a56
            LIMIT 1;
            UPDATE b01 SET b02 = b03 WHERE b04;
            INSERT INTO c01 (c02) VALUES (c03);
            INSERT INTO c04 SELECT * FROM c05;
            DELETE FROM d01 WHERE d02;
            CREATE TABLE e01 (
                e02 INT PRIMARY KEY DEFAULT e03 CHECK (e04),
                CHECK (e05)
            ) WITH (e06 = 1);
            CREATE VIEW f01 (f02) WITH (f03 = 1) AS SELECT * FROM f04;
            ALTER TABLE g01 ADD CONSTRAINT g02 PRIMARY KEY (g03);
            ALTER TABLE h01 ADD CONSTRAINT h02 FOREIGN KEY (h03) REFERENCES h04 (h05);
            ALTER TABLE i01 ADD CONSTRAINT i02 UNIQUE (i03);
            DROP TABLE j01;
            DROP VIEW k01;
            COPY l01 (l02) FROM stdin;
            START TRANSACTION READ ONLY;
            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
            COMMIT;
            ROLLBACK;
"#
            .into(),
        )?;

        #[rustfmt::skip]  // rustfmt loses the structure of the expected vector by wrapping all lines
        let expected = vec![
            "a01", "a02", "a03", "a04", "a05", "a06", "a07", "a08", "a09", "a10", "a11", "a12",
            "a13", "a14", "a15", "a16", "a17", "a18", "a19", "a20", "a21", "a22", "a23", "a24",
            "a25", "a26", "a27", "a28", "a29", "a30", "a31", "a32", "a33", "a34", "a35", "a36",
            "a37", "a38", "a39", "a40", "a41", "a42", "a43", "a44", "a45", "a46", "a47", "a48",
            "a49", "a50", "a51", "a52", "a53", "a54", "a55", "a56",
            "b01", "b02", "b03", "b04",
            "c01", "c02", "c03", "c04", "c05",
            "d01", "d02",
            "e01", "e02", "e03", "e04", "e05", "e06",
            "f01", "f02", "f03", "f04",
            "g01", "g02", "g03",
            "h01", "h02", "h03", "h04", "h05",
            "i01", "i02", "i03",
            "j01",
            "k01",
            "l01", "l02",
        ];

        let mut visitor = Visitor {
            seen_idents: Vec::new(),
        };
        for stmt in &stmts {
            Visit::visit_statement(&mut visitor, stmt);
        }
        assert_eq!(visitor.seen_idents, expected);

        let mut visitor = VisitorMut {
            seen_idents: Vec::new(),
        };
        for stmt in &mut stmts {
            VisitMut::visit_statement(&mut visitor, stmt);
        }
        assert_eq!(visitor.seen_idents, expected);

        Ok(())
    }
}
