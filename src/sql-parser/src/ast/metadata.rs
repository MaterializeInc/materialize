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

use std::fmt::{self, Debug};
use std::hash::Hash;

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::fold::{Fold, FoldNode};
use crate::ast::{
    Ident, Statement, UnresolvedDatabaseName, UnresolvedItemName, UnresolvedObjectName,
    UnresolvedSchemaName, Version,
};

/// This represents the metadata that lives next to an AST, as we take it through
/// various stages in the planning process.
///
/// Conceptually, when we first receive an AST from the parsing process, it only
/// represents the syntax that the user input, and has no semantic information
/// embedded in it. Later in this process, we want to be able to walk the tree
/// and add additional information to it piecemeal, perhaps without going down
/// the full planning pipeline. AstInfo represents various bits of information
/// that get stored in the tree: for instance, at first, table names are only
/// represented by the names the user input (in the `Raw` implementor of this
/// trait), but later on, we replace them with both the name along with the ID
/// that it gets resolved to.
///
/// Currently this process brings an `Ast<Raw>` to `Ast<Aug>`, and lives in
/// sql/src/names.rs:resolve.
pub trait AstInfo: Clone {
    /// The type used for nested statements.
    type NestedStatement: AstDisplay + Clone + Hash + Debug + Eq;
    /// The type used for item references. Items are the subset of objects that are namespaced by a
    /// database and schema.
    type ItemName: AstDisplay + Clone + Hash + Debug + Eq + Ord;
    /// The type used to specify a column.
    ///
    /// n.b. when implementing visitors, you likely want to build the visitor to
    /// visit [`crate::ast::ColumnName`] instead of visiting this struct
    /// directly. The visitor on this should usually just return an error.
    type ColumnReference: AstDisplay + Clone + Hash + Debug + Eq + Ord;
    /// The type used for schema names.
    type SchemaName: AstDisplay + Clone + Hash + Debug + Eq + Ord;
    /// The type used for database names.
    type DatabaseName: AstDisplay + Clone + Hash + Debug + Eq + Ord;
    /// The type used for cluster names.
    type ClusterName: AstDisplay + Clone + Hash + Debug + Eq + Ord;
    /// The type used for data types.
    type DataType: AstDisplay + Clone + Hash + Debug + Eq + Ord;
    /// The type stored next to CTEs for their assigned ID.
    type CteId: Clone + Hash + Debug + Eq + Ord;
    /// The type used for role references.
    type RoleName: AstDisplay + Clone + Hash + Debug + Eq + Ord;
    /// The type used for network policy references.
    type NetworkPolicyName: AstDisplay + Clone + Hash + Debug + Eq + Ord;
    /// They type used for any object names. Objects are the superset of all objects in Materialize.
    type ObjectName: AstDisplay + Clone + Hash + Debug + Eq + Ord;
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Default)]
pub struct Raw;

impl AstInfo for Raw {
    type NestedStatement = Statement<Raw>;
    type ItemName = RawItemName;
    type ColumnReference = Ident;
    type SchemaName = UnresolvedSchemaName;
    type DatabaseName = UnresolvedDatabaseName;
    type ClusterName = RawClusterName;
    type DataType = RawDataType;
    type CteId = ();
    type RoleName = Ident;
    type NetworkPolicyName = RawNetworkPolicyName;
    type ObjectName = UnresolvedObjectName;
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
pub enum RawItemName {
    Name(UnresolvedItemName),
    Id(String, UnresolvedItemName, Option<Version>),
}

impl RawItemName {
    pub fn name(&self) -> &UnresolvedItemName {
        match self {
            RawItemName::Name(name) => name,
            RawItemName::Id(_, name, _) => name,
        }
    }

    pub fn name_mut(&mut self) -> &mut UnresolvedItemName {
        match self {
            RawItemName::Name(name) => name,
            RawItemName::Id(_, name, _) => name,
        }
    }
}

impl AstDisplay for RawItemName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            RawItemName::Name(o) => f.write_node(o),
            RawItemName::Id(id, o, v) => {
                f.write_str(format!("[{} AS ", id));
                f.write_node(o);
                if let Some(v) = v {
                    f.write_str(" VERSION ");
                    f.write_node(v);
                }
                f.write_str("]");
            }
        }
    }
}
impl_display!(RawItemName);

impl<T> FoldNode<Raw, T> for RawItemName
where
    T: AstInfo,
{
    type Folded = T::ItemName;

    fn fold<F>(self, f: &mut F) -> Self::Folded
    where
        F: Fold<Raw, T>,
    {
        f.fold_item_name(self)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
pub enum RawClusterName {
    Unresolved(Ident),
    Resolved(String),
}

impl AstDisplay for RawClusterName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            RawClusterName::Unresolved(id) => f.write_node(id),
            RawClusterName::Resolved(id) => {
                f.write_str(format!("[{}]", id));
            }
        }
    }
}
impl_display!(RawClusterName);

impl<T> FoldNode<Raw, T> for RawClusterName
where
    T: AstInfo,
{
    type Folded = T::ClusterName;

    fn fold<F>(self, f: &mut F) -> Self::Folded
    where
        F: Fold<Raw, T>,
    {
        f.fold_cluster_name(self)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
pub enum RawNetworkPolicyName {
    Unresolved(Ident),
    Resolved(String),
}

impl AstDisplay for RawNetworkPolicyName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            RawNetworkPolicyName::Unresolved(id) => f.write_node(id),
            RawNetworkPolicyName::Resolved(id) => {
                f.write_str(format!("[{}]", id));
            }
        }
    }
}
impl_display!(RawNetworkPolicyName);

impl<T> FoldNode<Raw, T> for RawNetworkPolicyName
where
    T: AstInfo,
{
    type Folded = T::NetworkPolicyName;

    fn fold<F>(self, f: &mut F) -> Self::Folded
    where
        F: Fold<Raw, T>,
    {
        f.fold_network_policy_name(self)
    }
}

/// SQL data types
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RawDataType {
    /// Array
    Array(Box<RawDataType>),
    /// List
    List(Box<RawDataType>),
    /// Map
    Map {
        key_type: Box<RawDataType>,
        value_type: Box<RawDataType>,
    },
    /// Types who don't embed other types, e.g. INT
    Other {
        name: RawItemName,
        /// Typ modifiers appended to the type name, e.g. `numeric(38,0)`.
        typ_mod: Vec<i64>,
    },
}

impl AstDisplay for RawDataType {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            RawDataType::Array(ty) => {
                f.write_node(&ty);
                f.write_str("[]");
            }
            RawDataType::List(ty) => {
                f.write_node(&ty);
                f.write_str(" list");
            }
            RawDataType::Map {
                key_type,
                value_type,
            } => {
                f.write_str("map[");
                f.write_node(&key_type);
                f.write_str("=>");
                f.write_node(&value_type);
                f.write_str("]");
            }
            RawDataType::Other { name, typ_mod } => {
                f.write_node(name);
                if typ_mod.len() > 0 {
                    f.write_str("(");
                    f.write_node(&display::comma_separated(typ_mod));
                    f.write_str(")");
                }
            }
        }
    }
}
impl_display!(RawDataType);

impl<T> FoldNode<Raw, T> for RawDataType
where
    T: AstInfo,
{
    type Folded = T::DataType;

    fn fold<F>(self, f: &mut F) -> Self::Folded
    where
        F: Fold<Raw, T>,
    {
        f.fold_data_type(self)
    }
}
