// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/*#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Default)]
pub struct Tagged {}

impl AstInfo for Tagged {
    type NestedStatement = Statement<Raw>;
    type ObjectName = ResolvedObjectName;
    type SchemaName = ResolvedSchemaName;
    type DatabaseName = ResolvedDatabaseName;
    type ClusterName = ResolvedClusterName;
    type DataType = ResolvedDataType;
    type CteId = LocalId;
    type WildcardId = u64;
    type TableFactorId = u64;
    type NaturalJoinId = u64;
}*/

#[derive(Debug, Clone)]
pub struct StatementTagger {
    next_wildcard_id: u64,
    next_table_factor_id: u64,
    next_natural_join_id: u64,
}

impl Default for StatementTagger {
    fn default() -> Self {
        Self {
            next_wildcard_id: 0,
            next_table_factor_id: 0,
            next_natural_join_id: 0,
        }
    }
}

impl StatementTagger {
    /// TODO(jkosh44)
    pub fn wildcard_id(&mut self) -> u64 {
        let id = self.next_wildcard_id;
        self.next_wildcard_id += 1;
        id
    }

    /// TODO(jkosh44)
    pub fn table_factor_id(&mut self) -> u64 {
        let id = self.next_table_factor_id;
        self.next_table_factor_id += 1;
        id
    }

    /// TODO(jkosh44)
    pub fn natural_join_id(&mut self) -> u64 {
        let id = self.next_natural_join_id;
        self.next_natural_join_id += 1;
        id
    }
}

/*impl Fold<Aug, Tagged> for StatementTagger {
    fn fold_wildcard_id(
        &mut self,
        _wildcard_id: <Aug as AstInfo>::WildcardId,
    ) -> <Tagged as AstInfo>::WildcardId {
        self.wildcard_id()
    }
    fn fold_table_factor_id(
        &mut self,
        _table_factor_id: <Aug as AstInfo>::TableFactorId,
    ) -> <Tagged as AstInfo>::TableFactorId {
        self.table_factor_id()
    }
    fn fold_natural_join_id(
        &mut self,
        _natural_join_id: <Aug as AstInfo>::NaturalJoinId,
    ) -> <Tagged as AstInfo>::NaturalJoinId {
        self.natural_join_id()
    }
    fn fold_nested_statement(
        &mut self,
        nested_statement: <Aug as AstInfo>::NestedStatement,
    ) -> <Tagged as AstInfo>::NestedStatement {
        nested_statement
    }
    fn fold_object_name(
        &mut self,
        object_name: <Aug as AstInfo>::ObjectName,
    ) -> <Tagged as AstInfo>::ObjectName {
        object_name
    }
    fn fold_schema_name(
        &mut self,
        schema_name: <Aug as AstInfo>::SchemaName,
    ) -> <Tagged as AstInfo>::SchemaName {
        schema_name
    }
    fn fold_database_name(
        &mut self,
        database_name: <Aug as AstInfo>::DatabaseName,
    ) -> <Tagged as AstInfo>::DatabaseName {
        database_name
    }
    fn fold_cluster_name(
        &mut self,
        cluster_name: <Aug as AstInfo>::ClusterName,
    ) -> <Tagged as AstInfo>::ClusterName {
        cluster_name
    }
    fn fold_data_type(
        &mut self,
        data_type: <Aug as AstInfo>::DataType,
    ) -> <Tagged as AstInfo>::DataType {
        data_type
    }
    fn fold_cte_id(&mut self, id: <Aug as AstInfo>::CteId) -> <Tagged as AstInfo>::CteId {
        id
    }
}

impl Fold<Tagged, Aug> for StatementTagger {
    fn fold_wildcard_id(
        &mut self,
        _wildcard_id: <Tagged as AstInfo>::WildcardId,
    ) -> <Aug as AstInfo>::WildcardId {
        ()
    }
    fn fold_table_factor_id(
        &mut self,
        _table_factor_id: <Tagged as AstInfo>::TableFactorId,
    ) -> <Aug as AstInfo>::TableFactorId {
        ()
    }
    fn fold_natural_join_id(
        &mut self,
        _natural_join_id: <Tagged as AstInfo>::NaturalJoinId,
    ) -> <Aug as AstInfo>::NaturalJoinId {
        ()
    }
    fn fold_nested_statement(
        &mut self,
        nested_statement: <Tagged as AstInfo>::NestedStatement,
    ) -> <Aug as AstInfo>::NestedStatement {
        nested_statement
    }
    fn fold_object_name(
        &mut self,
        object_name: <Tagged as AstInfo>::ObjectName,
    ) -> <Aug as AstInfo>::ObjectName {
        object_name
    }
    fn fold_schema_name(
        &mut self,
        schema_name: <Tagged as AstInfo>::SchemaName,
    ) -> <Aug as AstInfo>::SchemaName {
        schema_name
    }
    fn fold_database_name(
        &mut self,
        database_name: <Tagged as AstInfo>::DatabaseName,
    ) -> <Aug as AstInfo>::DatabaseName {
        database_name
    }
    fn fold_cluster_name(
        &mut self,
        cluster_name: <Tagged as AstInfo>::ClusterName,
    ) -> <Aug as AstInfo>::ClusterName {
        cluster_name
    }
    fn fold_data_type(
        &mut self,
        data_type: <Tagged as AstInfo>::DataType,
    ) -> <Aug as AstInfo>::DataType {
        data_type
    }
    fn fold_cte_id(&mut self, id: <Tagged as AstInfo>::CteId) -> <Aug as AstInfo>::CteId {
        id
    }
}*/
