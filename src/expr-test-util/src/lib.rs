// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use lazy_static::lazy_static;
use proc_macro2::TokenTree;

use expr::explain::ViewExplanation;
use expr::*;
use lowertest::*;
use ore::result::ResultExt;
use repr::{ColumnType, Datum, RelationType, Row, ScalarType};
use repr_test_util::{get_datum_from_str, get_scalar_type_or_default};

gen_reflect_info_func!(
    produce_rti,
    [
        BinaryFunc,
        NullaryFunc,
        UnaryFunc,
        VariadicFunc,
        MirScalarExpr,
        ScalarType,
        TableFunc,
        AggregateFunc,
        MirRelationExpr,
        JoinImplementation
    ],
    [AggregateExpr, ColumnOrder, ColumnType, RelationType]
);

lazy_static! {
    pub static ref RTI: ReflectedTypeInfo = produce_rti();
}

/// Builds a `MirScalarExpr` from a string.
///
/// See [lowertest::to_json] for the syntax.
pub fn build_scalar(s: &str) -> Result<MirScalarExpr, String> {
    deserialize(
        &mut parse_str(s)?.into_iter(),
        "MirScalarExpr",
        &RTI,
        &mut MirScalarExprDeserializeContext::default(),
    )
}

/// Builds a `MirRelationExpr` from a string.
///
/// See [lowertest::to_json] for the syntax.
pub fn build_rel(s: &str, catalog: &TestCatalog) -> Result<MirRelationExpr, String> {
    deserialize(
        &mut parse_str(s)?.into_iter(),
        "MirRelationExpr",
        &RTI,
        &mut MirRelationExprDeserializeContext::new(catalog),
    )
}

/// A catalog that holds types of objects previously created for the unit test.
///
/// This is for the purpose of allowing `MirRelationExpr`s can refer to them
/// later.
#[derive(Debug, Default)]
pub struct TestCatalog {
    objects: HashMap<String, (GlobalId, RelationType)>,
    names: HashMap<GlobalId, String>,
}

impl<'a> TestCatalog {
    fn insert(&mut self, name: &str, typ: RelationType) {
        // TODO(justin): error on dup name?
        let id = GlobalId::User(self.objects.len() as u64);
        self.objects.insert(name.to_string(), (id, typ));
        self.names.insert(id, name.to_string());
    }

    fn get(&'a self, name: &str) -> Option<&'a (GlobalId, RelationType)> {
        self.objects.get(name)
    }

    /// Pretty-print the MirRelationExpr.
    ///
    /// If format contains "types", then add types to the pretty-printed
    /// `MirRelationExpr`.
    pub fn generate_explanation(
        &self,
        rel: &MirRelationExpr,
        format: Option<&Vec<String>>,
    ) -> String {
        let mut explanation = ViewExplanation::new(rel, self);
        if let Some(format) = format {
            if format.contains(&"types".to_string()) {
                explanation.explain_types();
            }
        }
        explanation.to_string()
    }

    /// Handles instructions to modify the catalog.
    ///
    /// Currently supported commands:
    /// * `(defsource [types_of_cols] [[optional_sets_of_key_cols]])`
    ///   insert a source into the catalog.
    pub fn handle_test_command(&mut self, spec: &str) -> Result<(), String> {
        let mut stream_iter = parse_str(spec)?.into_iter();
        match stream_iter.next() {
            Some(TokenTree::Group(group)) => {
                let mut inner_iter = group.stream().into_iter().peekable();
                match inner_iter.next() {
                    Some(TokenTree::Ident(ident)) if &ident.to_string()[..] == "defsource" => {
                        let name = match inner_iter.next() {
                            Some(TokenTree::Ident(ident)) => Ok(ident.to_string()),
                            invalid_token => {
                                Err(format!("invalid source name: {:?}", invalid_token))
                            }
                        }?;

                        let mut ctx = GenericTestDeserializeContext::default();
                        let typ: RelationType =
                            deserialize(&mut inner_iter, "RelationType", &RTI, &mut ctx)?;

                        self.insert(&name, typ);
                        Ok(())
                    }
                    s => Err(format!("not a valid catalog command: {:?}", s)),
                }
            }
            s => Err(format!("not a valid catalog command spec: {:?}", s)),
        }
    }
}

impl ExprHumanizer for TestCatalog {
    fn humanize_id(&self, id: GlobalId) -> Option<String> {
        self.names.get(&id).map(|s| s.to_string())
    }

    fn humanize_scalar_type(&self, ty: &ScalarType) -> String {
        DummyHumanizer.humanize_scalar_type(ty)
    }
}

/// Extends the test case syntax to support `MirScalarExpr`s
///
/// The following variants of `MirScalarExpr` have non-standard syntax:
/// Literal -> the syntax is `(<literal> <scalar type>)` or `<literal>`.
/// If `<scalar type>` is not specified, then literals will be assigned
/// default types:
/// * true/false become Bool
/// * numbers become Int64
/// * strings become String
/// Column -> the syntax is `#n`, where n is the column number.
#[derive(Default)]
pub struct MirScalarExprDeserializeContext;

impl MirScalarExprDeserializeContext {
    fn build_column(&mut self, token: Option<TokenTree>) -> Result<MirScalarExpr, String> {
        if let Some(TokenTree::Literal(literal)) = token {
            return Ok(MirScalarExpr::Column(
                literal.to_string().parse::<usize>().map_err_to_string()?,
            ));
        }
        Err(format!("Invalid column specification {:?}", token))
    }

    fn build_literal_if_able<I>(
        &mut self,
        first_arg: TokenTree,
        rest_of_stream: &mut I,
    ) -> Result<Option<MirScalarExpr>, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        match extract_literal_string(&first_arg, rest_of_stream)? {
            Some(litval) => {
                let littyp = get_scalar_type_or_default(&litval[..], rest_of_stream)?;
                Ok(Some(MirScalarExpr::literal_ok(
                    get_datum_from_str(&litval[..], &littyp)?,
                    littyp,
                )))
            }
            None => Ok(None),
        }
    }
}

impl TestDeserializeContext for MirScalarExprDeserializeContext {
    fn override_syntax<I>(
        &mut self,
        first_arg: TokenTree,
        rest_of_stream: &mut I,
        type_name: &str,
        _rti: &ReflectedTypeInfo,
    ) -> Result<Option<String>, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        let result = if type_name == "MirScalarExpr" {
            match first_arg {
                TokenTree::Punct(punct) if punct.as_char() == '#' => {
                    Some(self.build_column(rest_of_stream.next())?)
                }
                TokenTree::Group(_) => None,
                symbol => self.build_literal_if_able(symbol, rest_of_stream)?,
            }
        } else {
            None
        };
        match result {
            Some(result) => Ok(Some(serde_json::to_string(&result).map_err_to_string()?)),
            None => Ok(None),
        }
    }
}

/// Extends the test case syntax to support `MirRelationExpr`s
///
/// A new context should be created for the deserialization of each
/// `MirRelationExpr` because the context stores state local to
/// each `MirRelationExpr`.
///
/// Includes all the test case syntax extensions to support
/// `MirScalarExpr`s.
///
/// The following variants of `MirRelationExpr` have non-standard syntax:
/// Let -> the syntax is `(let x <value> <body>)` where x is an ident that
///        should not match any existing ident in any Let statement in
///        `<value>`.
/// Get -> the syntax is `(get x)`, where x is an ident that refers to a
///        pre-defined source or an ident defined in a let.
/// Union -> the syntax is `(union <input1> .. <inputn>)`.
/// Constant -> the syntax is
/// ```ignore
/// (constant
///    [[<row1literal1>..<row1literaln>]..[<rowiliteral1>..<rowiliteraln>]]
///    [<scalartype1> .. <scalartypen>]
/// )
/// ```
///
/// For convenience, a usize can be alternately specified as `#n`.
/// We recommend specifying a usize as `#n` instead of `n` when the usize
/// is a column reference.
pub struct MirRelationExprDeserializeContext<'a> {
    inner_ctx: MirScalarExprDeserializeContext,
    catalog: &'a TestCatalog,
    scope: Scope,
}

impl<'a> MirRelationExprDeserializeContext<'a> {
    pub fn new(catalog: &'a TestCatalog) -> Self {
        Self {
            inner_ctx: MirScalarExprDeserializeContext::default(),
            catalog,
            scope: Scope::default(),
        }
    }

    fn build_constant<I>(&mut self, stream_iter: &mut I) -> Result<MirRelationExpr, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        let raw_rows = stream_iter
            .next()
            .ok_or_else(|| format!("Constant is empty"))?;
        // Deserialize the types of each column first
        // in order to refer to column types when constructing the `Datum`
        // objects in each row.
        let scalar_types: Vec<ScalarType> =
            deserialize(stream_iter, "Vec<ScalarType>", &RTI, self)?;

        let mut rows = Vec::new();
        match raw_rows {
            TokenTree::Group(group) => {
                let mut row_iter = group.stream().into_iter().peekable();
                while row_iter.peek().is_some() {
                    match row_iter.next() {
                        Some(TokenTree::Group(group)) => {
                            let mut inner_iter = group.stream().into_iter();
                            let mut parsed_data = Vec::new();
                            while let Some(symbol) = inner_iter.next() {
                                match extract_literal_string(&symbol, &mut inner_iter)? {
                                    Some(dat) => parsed_data.push(dat),
                                    None => {
                                        return Err(format!(
                                            "{:?} cannot be interpreted as a literal.",
                                            symbol
                                        ));
                                    }
                                }
                            }
                            let row = parsed_data
                                .iter()
                                .zip(scalar_types.iter())
                                .map(|(dat, typ)| get_datum_from_str(&dat[..], typ))
                                .collect::<Result<Vec<Datum>, String>>()?;
                            rows.push((Row::pack_slice(&row), 1));
                        }
                        invalid => {
                            return Err(format!("invalid row spec for constant {:?}", invalid))
                        }
                    }
                }
            }
            invalid => return Err(format!("invalid rows spec for constant {:?}", invalid)),
        };
        Ok(MirRelationExpr::Constant {
            rows: Ok(rows),
            typ: RelationType::new(
                scalar_types
                    .into_iter()
                    .map(|t| t.nullable(true))
                    .collect::<Vec<_>>(),
            ),
        })
    }

    fn build_get(&mut self, token: Option<TokenTree>) -> Result<MirRelationExpr, String> {
        match token {
            Some(TokenTree::Ident(ident)) => {
                let name = ident.to_string();
                match self.scope.get(&name) {
                    Some((id, typ)) => Ok(MirRelationExpr::Get { id, typ }),
                    None => match self.catalog.get(&name) {
                        None => Err(format!("no catalog object named {}", name)),
                        Some((id, typ)) => Ok(MirRelationExpr::Get {
                            id: Id::Global(*id),
                            typ: typ.clone(),
                        }),
                    },
                }
            }
            invalid_token => Err(format!("Invalid get specification {:?}", invalid_token)),
        }
    }

    fn build_let<I>(&mut self, stream_iter: &mut I) -> Result<MirRelationExpr, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        let name = match stream_iter.next() {
            Some(TokenTree::Ident(ident)) => Ok(ident.to_string()),
            invalid_token => Err(format!("Invalid let specification {:?}", invalid_token)),
        }?;

        let value: MirRelationExpr = deserialize(stream_iter, "MirRelationExpr", &RTI, self)?;

        let (id, prev) = self.scope.insert(&name, value.typ());

        let body: MirRelationExpr = deserialize(stream_iter, "MirRelationExpr", &RTI, self)?;

        if let Some((old_id, old_val)) = prev {
            self.scope.set(&name, old_id, old_val);
        } else {
            self.scope.remove(&name)
        }

        Ok(MirRelationExpr::Let {
            id,
            value: Box::new(value),
            body: Box::new(body),
        })
    }

    fn build_union<I>(&mut self, stream_iter: &mut I) -> Result<MirRelationExpr, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        let mut inputs: Vec<MirRelationExpr> =
            deserialize(stream_iter, "Vec<MirRelationExpr>", &RTI, self)?;
        Ok(MirRelationExpr::Union {
            base: Box::new(inputs.remove(0)),
            inputs,
        })
    }

    fn build_special_mir_if_able<I>(
        &mut self,
        first_arg: TokenTree,
        rest_of_stream: &mut I,
    ) -> Result<Option<MirRelationExpr>, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        if let TokenTree::Ident(ident) = first_arg {
            return Ok(match &ident.to_string()[..] {
                "constant" => Some(self.build_constant(rest_of_stream)?),
                "get" => Some(self.build_get(rest_of_stream.next())?),
                "let" => Some(self.build_let(rest_of_stream)?),
                "union" => Some(self.build_union(rest_of_stream)?),
                _ => None,
            });
        }
        Ok(None)
    }
}

impl<'a> TestDeserializeContext for MirRelationExprDeserializeContext<'a> {
    fn override_syntax<I>(
        &mut self,
        first_arg: TokenTree,
        rest_of_stream: &mut I,
        type_name: &str,
        rti: &ReflectedTypeInfo,
    ) -> Result<Option<String>, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        match self
            .inner_ctx
            .override_syntax(first_arg.clone(), rest_of_stream, type_name, rti)?
        {
            Some(result) => Ok(Some(result)),
            None => {
                if type_name == "MirRelationExpr" {
                    if let Some(result) =
                        self.build_special_mir_if_able(first_arg, rest_of_stream)?
                    {
                        return Ok(Some(serde_json::to_string(&result).map_err_to_string()?));
                    }
                } else if type_name == "usize" {
                    if let TokenTree::Punct(punct) = first_arg {
                        if punct.as_char() == '#' {
                            match rest_of_stream.next() {
                                Some(TokenTree::Literal(literal)) => {
                                    return Ok(Some(literal.to_string()))
                                }
                                invalid => {
                                    return Err(format!("invalid column value {:?}", invalid))
                                }
                            }
                        }
                    }
                }
                Ok(None)
            }
        }
    }
}

/// Stores the values of `let` statements that way they can be accessed
/// in the body of the `let`.
#[derive(Debug, Default)]
struct Scope {
    objects: HashMap<String, (Id, RelationType)>,
    names: HashMap<Id, String>,
}

impl<'a> Scope {
    fn insert(&mut self, name: &str, typ: RelationType) -> (LocalId, Option<(Id, RelationType)>) {
        let old_val = self.get(name);
        let id = LocalId::new(self.objects.len() as u64);
        self.set(name, Id::Local(id), typ);
        (id, old_val)
    }

    fn set(&mut self, name: &str, id: Id, typ: RelationType) {
        self.objects.insert(name.to_string(), (id, typ));
        self.names.insert(id, name.to_string());
    }

    fn remove(&mut self, name: &str) {
        self.objects.remove(name);
    }

    fn get(&self, name: &str) -> Option<(Id, RelationType)> {
        self.objects.get(name).cloned()
    }
}

fn extract_literal_string<I>(
    first_arg: &TokenTree,
    rest_of_stream: &mut I,
) -> Result<Option<String>, String>
where
    I: Iterator<Item = TokenTree>,
{
    match first_arg {
        TokenTree::Ident(ident) => {
            if ["true", "false", "null"].contains(&&ident.to_string()[..]) {
                Ok(Some(ident.to_string()))
            } else {
                Ok(None)
            }
        }
        TokenTree::Literal(literal) => Ok(Some(literal.to_string())),
        TokenTree::Punct(punct) if punct.as_char() == '-' => {
            match rest_of_stream.next() {
                Some(TokenTree::Literal(literal)) => {
                    Ok(Some(format!("{}{}", punct.as_char(), literal.to_string())))
                }
                None => Ok(None),
                // Must error instead of handling the tokens using default
                // behavior since `stream_iter` has advanced.
                Some(other) => Err(format!(
                    "{}{:?} is not a valid literal",
                    punct.as_char(),
                    other
                )),
            }
        }
        _ => Ok(None),
    }
}
