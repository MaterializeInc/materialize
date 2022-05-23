// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use once_cell::sync::Lazy;
use proc_macro2::TokenTree;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use mz_expr::explain::ViewExplanation;
use mz_expr::{
    DummyHumanizer, EvalError, ExprHumanizer, Id, LocalId, MirRelationExpr, MirScalarExpr,
};
use mz_lowertest::*;
use mz_ore::result::ResultExt;
use mz_ore::str::separated;
use mz_repr::{ColumnType, GlobalId, RelationType, Row, ScalarType};
use mz_repr_test_util::*;

/// Contains the type information required to build a [MirRelationExpr] and
/// all types it depends on.
pub static RTI: Lazy<ReflectedTypeInfo> = Lazy::new(|| {
    let mut rti = ReflectedTypeInfo::default();
    EvalError::add_to_reflected_type_info(&mut rti);
    MirRelationExpr::add_to_reflected_type_info(&mut rti);
    TestCatalogCommand::add_to_reflected_type_info(&mut rti);
    rti
});

/// Builds a [MirScalarExpr] from a string.
///
/// See [mz_lowertest::to_json] for the syntax.
pub fn build_scalar(s: &str) -> Result<MirScalarExpr, String> {
    deserialize(
        &mut tokenize(s)?.into_iter(),
        "MirScalarExpr",
        &RTI,
        &mut MirScalarExprDeserializeContext::default(),
    )
}

/// Builds a [MirRelationExpr] from a string.
///
/// See [mz_lowertest::to_json] for the syntax.
pub fn build_rel(s: &str, catalog: &TestCatalog) -> Result<MirRelationExpr, String> {
    deserialize(
        &mut tokenize(s)?.into_iter(),
        "MirRelationExpr",
        &RTI,
        &mut MirRelationExprDeserializeContext::new(catalog),
    )
}

/// Pretty-print the [MirRelationExpr].
///
/// If format contains "types", then add types to the pretty-printed
/// [MirRelationExpr].
pub fn generate_explanation(
    humanizer: &dyn ExprHumanizer,
    rel: &MirRelationExpr,
    format: Option<&Vec<String>>,
) -> String {
    let mut explanation = ViewExplanation::new(rel, humanizer);
    if let Some(format) = format {
        if format.contains(&"types".to_string()) {
            explanation.explain_types();
        }
    }
    explanation.to_string()
}

/// Turns the json version of a [MirRelationExpr] into the [mz_lowertest::to_json]
/// syntax.
///
/// The return value is a tuple of:
/// 1. The translated [MirRelationExpr].
/// 2. The commands to register sources referenced by the [MirRelationExpr] with
///    the test catalog.
pub fn json_to_spec(rel_json: &str, catalog: &TestCatalog) -> (String, Vec<String>) {
    let mut ctx = MirRelationExprDeserializeContext::new(&catalog);
    let spec = from_json(
        &serde_json::from_str(rel_json).unwrap(),
        "MirRelationExpr",
        &RTI,
        &mut ctx,
    );
    let mut source_defs = ctx
        .list_scope_references()
        .map(|(name, typ)| {
            format!(
                "(defsource {} {})",
                name,
                from_json(
                    &serde_json::to_value(typ).unwrap(),
                    "RelationType",
                    &RTI,
                    &mut GenericTestDeserializeContext::default()
                )
            )
        })
        .collect::<Vec<_>>();
    source_defs.sort();
    (spec, source_defs)
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

/// Contains the arguments for a command for [TestCatalog].
///
/// See [mz_lowertest] for the command syntax.
#[derive(Debug, Serialize, Deserialize, MzReflect)]
enum TestCatalogCommand {
    /// Insert a source into the catalog.
    Defsource { name: String, typ: RelationType },
}

impl<'a> TestCatalog {
    /// Registers an object in the catalog.
    ///
    /// Specifying `transient` as true allows the object to be deleted by
    /// [Self::remove_transient_objects].
    ///
    /// Returns the GlobalId assigned by the catalog to the object.
    ///
    /// Errors if an object of the same name is already in the catalog.
    pub fn insert(
        &mut self,
        name: &str,
        typ: RelationType,
        transient: bool,
    ) -> Result<GlobalId, String> {
        if self.objects.contains_key(name) {
            return Err(format!("Object {} already exists in catalog", name));
        }
        let id = if transient {
            GlobalId::Transient(self.objects.len() as u64)
        } else {
            GlobalId::User(self.objects.len() as u64)
        };
        self.objects.insert(name.to_string(), (id, typ));
        self.names.insert(id, name.to_string());
        Ok(id)
    }

    fn get(&'a self, name: &str) -> Option<&'a (GlobalId, RelationType)> {
        self.objects.get(name)
    }

    /// Looks up the name of the object referred to as `id`.
    pub fn get_source_name(&'a self, id: &GlobalId) -> Option<&'a String> {
        self.names.get(id)
    }

    /// Handles instructions to modify the catalog.
    ///
    /// Currently supported commands:
    /// * `(defsource [types_of_cols] [[optional_sets_of_key_cols]])` -
    ///   insert a source into the catalog.
    pub fn handle_test_command(&mut self, spec: &str) -> Result<(), String> {
        let mut stream_iter = tokenize(spec)?.into_iter();
        while let Some(command) = deserialize_optional::<TestCatalogCommand, _, _>(
            &mut stream_iter,
            "TestCatalogCommand",
            &RTI,
            &mut GenericTestDeserializeContext::default(),
        )? {
            match command {
                TestCatalogCommand::Defsource { name, typ } => {
                    self.insert(&name, typ, false)?;
                }
            }
        }
        Ok(())
    }

    /// Clears all transient objects from the catalog.
    pub fn remove_transient_objects(&mut self) {
        self.objects.retain(|_, (id, _)| {
            if let GlobalId::Transient(_) = id {
                false
            } else {
                true
            }
        });
        self.names.retain(|k, _| {
            if let GlobalId::Transient(_) = k {
                false
            } else {
                true
            }
        });
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
/// Literal -> the syntax is `(ok <literal> <scalar type>)`, `<literal>`
/// or (err <eval error> <scalar type>). Note that `ok` token can be omitted.
/// If `<scalar type>` is not specified, then literals will be assigned
/// default types:
/// * true/false become Bool
/// * numbers become Int64
/// * strings become String
/// * Bool for literal errors
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
        rti: &ReflectedTypeInfo,
    ) -> Result<Option<MirScalarExpr>, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        match &first_arg {
            TokenTree::Ident(i) if i.to_string().to_ascii_lowercase() == "ok" => {
                // literal definition is mandatory after OK token
                let first_arg = if let Some(first_arg) = rest_of_stream.next() {
                    first_arg
                } else {
                    return Err(format!("expected literal after {:?}", i));
                };
                match self.build_literal_ok_if_able(first_arg, rest_of_stream) {
                    Ok(Some(l)) => Ok(Some(l)),
                    _ => Err(format!("expected literal after {:?}", i)),
                }
            }
            TokenTree::Ident(i) if i.to_string().to_ascii_lowercase() == "err" => {
                let error = deserialize(
                    rest_of_stream,
                    "EvalError",
                    rti,
                    &mut GenericTestDeserializeContext::default(),
                )?;
                let typ: Option<ScalarType> = deserialize_optional(
                    rest_of_stream,
                    "ScalarType",
                    rti,
                    &mut GenericTestDeserializeContext::default(),
                )?;
                Ok(Some(MirScalarExpr::literal(
                    Err(error),
                    typ.unwrap_or(ScalarType::Bool),
                )))
            }
            _ => self.build_literal_ok_if_able(first_arg, rest_of_stream),
        }
    }

    fn build_literal_ok_if_able<I>(
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
                Ok(Some(MirScalarExpr::Literal(
                    Ok(test_spec_to_row(std::iter::once((&litval[..], &littyp)))?),
                    littyp.nullable(matches!(&litval[..], "null")),
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
        rti: &ReflectedTypeInfo,
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
                symbol => self.build_literal_if_able(symbol, rest_of_stream, rti)?,
            }
        } else {
            None
        };
        match result {
            Some(result) => Ok(Some(serde_json::to_string(&result).map_err_to_string()?)),
            None => Ok(None),
        }
    }

    fn reverse_syntax_override(
        &mut self,
        json: &Value,
        type_name: &str,
        rti: &ReflectedTypeInfo,
    ) -> Option<String> {
        if type_name == "MirScalarExpr" {
            let map = json.as_object().unwrap();
            // Each enum instance only belows to one variant.
            assert_eq!(map.len(), 1);
            for (variant, data) in map.iter() {
                match &variant[..] {
                    "Column" => return Some(format!("#{}", data.as_u64().unwrap())),
                    "Literal" => {
                        let column_type: ColumnType =
                            serde_json::from_value(data.as_array().unwrap()[1].clone()).unwrap();
                        let obj = data.as_array().unwrap()[0].as_object().unwrap();
                        if let Some(inner_data) = obj.get("Ok") {
                            let row: Row = serde_json::from_value(inner_data.clone()).unwrap();
                            let result = format!(
                                "({} {})",
                                datum_to_test_spec(row.unpack_first()),
                                from_json(
                                    &serde_json::to_value(&column_type.scalar_type).unwrap(),
                                    "ScalarType",
                                    rti,
                                    self
                                )
                            );
                            return Some(result);
                        } else if let Some(inner_data) = obj.get("Err") {
                            let result = format!(
                                "(err {} {})",
                                from_json(&inner_data, "EvalError", rti, self),
                                from_json(
                                    &serde_json::to_value(&column_type.scalar_type).unwrap(),
                                    "ScalarType",
                                    rti,
                                    self
                                ),
                            );
                            return Some(result);
                        } else {
                            unreachable!("unexpected JSON data: {:?}", obj);
                        }
                    }
                    _ => {}
                }
            }
        }
        None
    }
}

/// Extends the test case syntax to support [MirRelationExpr]s
///
/// A new context should be created for the deserialization of each
/// [MirRelationExpr] because the context stores state local to
/// each [MirRelationExpr].
///
/// Includes all the test case syntax extensions to support
/// [MirScalarExpr]s.
///
/// The following variants of [MirRelationExpr] have non-standard syntax:
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
///    <RelationType>
/// )
/// ```
///
/// For convenience, a usize can be alternately specified as `#n`.
/// We recommend specifying a usize as `#n` instead of `n` when the usize
/// is a column reference.
pub struct MirRelationExprDeserializeContext<'a> {
    inner_ctx: MirScalarExprDeserializeContext,
    catalog: &'a TestCatalog,
    /// Tracks local references when converting spec to JSON.
    /// Tracks global references not found in the catalog when converting from
    /// JSON to spec.
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

    pub fn list_scope_references(&self) -> impl Iterator<Item = (&String, &RelationType)> {
        self.scope.iter()
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
        let typ: RelationType = deserialize(stream_iter, "RelationType", &RTI, self)?;

        let mut rows = Vec::new();
        match raw_rows {
            TokenTree::Group(group) => {
                let mut inner_iter = group.stream().into_iter();
                while let Some(token) = inner_iter.next() {
                    let row = test_spec_to_row(
                        parse_vec_of_literals(&token)?
                            .iter()
                            .zip(&typ.column_types)
                            .map(|(dat, col_typ)| (&dat[..], &col_typ.scalar_type)),
                    )?;
                    rows.push((row, 1));
                }
            }
            invalid => return Err(format!("invalid rows spec for constant {:?}", invalid)),
        };
        Ok(MirRelationExpr::Constant {
            rows: Ok(rows),
            typ,
        })
    }

    fn build_constant_err<I>(&mut self, stream_iter: &mut I) -> Result<MirRelationExpr, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        let error: EvalError = deserialize(
            stream_iter,
            "EvalError",
            &RTI,
            &mut GenericTestDeserializeContext::default(),
        )?;
        let typ: RelationType = deserialize(stream_iter, "RelationType", &RTI, self)?;

        Ok(MirRelationExpr::Constant {
            rows: Err(error),
            typ,
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
            return Ok(match &ident.to_string().to_lowercase()[..] {
                "constant" => Some(self.build_constant(rest_of_stream)?),
                "constant_err" => Some(self.build_constant_err(rest_of_stream)?),
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

    fn reverse_syntax_override(
        &mut self,
        json: &Value,
        type_name: &str,
        rti: &ReflectedTypeInfo,
    ) -> Option<String> {
        match self.inner_ctx.reverse_syntax_override(json, type_name, rti) {
            Some(result) => Some(result),
            None => {
                if type_name == "MirRelationExpr" {
                    let map = json.as_object().unwrap();
                    // Each enum instance only belows to one variant.
                    assert_eq!(
                        map.len(),
                        1,
                        "Multivariant instance {:?} found for MirRelationExpr",
                        map
                    );
                    for (variant, data) in map.iter() {
                        let inner_map = data.as_object().unwrap();
                        match &variant[..] {
                            "Let" => {
                                let id: LocalId =
                                    serde_json::from_value(inner_map["id"].clone()).unwrap();
                                return Some(format!(
                                    "(let {} {} {})",
                                    id,
                                    from_json(&inner_map["value"], "MirRelationExpr", rti, self),
                                    from_json(&inner_map["body"], "MirRelationExpr", rti, self),
                                ));
                            }
                            "Get" => {
                                let id: Id =
                                    serde_json::from_value(inner_map["id"].clone()).unwrap();
                                return Some(match id {
                                    Id::Global(global) => {
                                        match self.catalog.get_source_name(&global) {
                                            // Replace the GlobalId with the
                                            // name of the source.
                                            Some(source) => format!("(get {})", source),
                                            // Treat the GlobalId
                                            None => {
                                                let typ: RelationType = serde_json::from_value(
                                                    inner_map["typ"].clone(),
                                                )
                                                .unwrap();
                                                self.scope.insert(&id.to_string(), typ);
                                                format!("(get {})", id)
                                            }
                                        }
                                    }
                                    _ => {
                                        format!("(get {})", id)
                                    }
                                });
                            }
                            "Constant" => {
                                if let Some(row_vec) = inner_map["rows"].get("Ok") {
                                    let mut rows = Vec::new();
                                    for inner_array in row_vec.as_array().unwrap() {
                                        let row: Row =
                                            serde_json::from_value(inner_array[0].clone()).unwrap();
                                        let diff = inner_array[1].as_u64().unwrap();
                                        for _ in 0..diff {
                                            rows.push(format!(
                                                "[{}]",
                                                separated(" ", row.iter().map(datum_to_test_spec))
                                            ))
                                        }
                                    }
                                    return Some(format!(
                                        "(constant [{}] {})",
                                        separated(" ", rows),
                                        from_json(&inner_map["typ"], "RelationType", rti, self)
                                    ));
                                } else if let Some(inner_data) = inner_map["rows"].get("Err") {
                                    return Some(format!(
                                        "(constant_err {} {})",
                                        from_json(&inner_data, "EvalError", rti, self),
                                        from_json(&inner_map["typ"], "RelationType", rti, self)
                                    ));
                                } else {
                                    unreachable!("unexpected JSON data: {:?}", inner_map);
                                }
                            }
                            "Union" => {
                                let mut inputs = inner_map["inputs"].as_array().unwrap().to_owned();
                                inputs.insert(0, inner_map["base"].clone());
                                return Some(format!(
                                    "(union {})",
                                    from_json(
                                        &Value::Array(inputs),
                                        "Vec<MirRelationExpr>",
                                        rti,
                                        self
                                    )
                                ));
                            }
                            _ => {}
                        }
                    }
                }
                None
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

impl Scope {
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

    fn iter(&self) -> impl Iterator<Item = (&String, &RelationType)> {
        self.objects.iter().map(|(s, (_, typ))| (s, typ))
    }
}
