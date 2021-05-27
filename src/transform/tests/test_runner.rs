// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// This module defines a small language for directly constructing RelationExprs and running
/// various optimizations on them. It uses datadriven, so the output of each test can be rewritten
/// by setting the REWRITE environment variable.
/// TODO(justin):
/// * It's currently missing a mechanism to run just a single test file
/// * There is some duplication between this and the SQL planner
/// * Not all operators supported (Reduce)

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt::Write;

    use anyhow::{anyhow, Error};
    use expr::explain::ViewExplanation;
    use expr::*;
    use lazy_static::lazy_static;
    use lowertest::*;
    use proc_macro2::{TokenStream, TokenTree};
    use repr::{ColumnType, Datum, RelationType, Row, ScalarType};
    use transform::{Optimizer, Transform, TransformArgs};

    gen_reflect_info_func!(
        produce_rti,
        [BinaryFunc, NullaryFunc, UnaryFunc, VariadicFunc, MirScalarExpr, ScalarType
        TableFunc, AggregateFunc, MirRelationExpr, JoinImplementation]
    );

    lazy_static! {
        static ref RTI: ReflectedTypeInfo = produce_rti();
    }

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum TestType {
        Build,
        Opt,
        Steps,
    }

    #[derive(Debug)]
    struct Scope {
        objects: HashMap<String, (Id, RelationType)>,
        names: HashMap<Id, String>,
    }

    impl<'a> Scope {
        fn insert(
            &mut self,
            name: &str,
            typ: RelationType,
        ) -> (LocalId, Option<(Id, RelationType)>) {
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

    #[derive(Debug)]
    struct TestCatalog {
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
    }

    impl ExprHumanizer for TestCatalog {
        fn humanize_id(&self, id: GlobalId) -> Option<String> {
            self.names.get(&id).map(|s| s.to_string())
        }

        fn humanize_scalar_type(&self, ty: &ScalarType) -> String {
            DummyHumanizer.humanize_scalar_type(ty)
        }

        fn humanize_column_type(&self, ty: &ColumnType) -> String {
            DummyHumanizer.humanize_column_type(ty)
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
    struct MirScalarExprDeserializeContext;

    impl MirScalarExprDeserializeContext {
        fn build_column(&mut self, token: Option<TokenTree>) -> Result<MirScalarExpr, String> {
            if let Some(TokenTree::Literal(literal)) = token {
                return Ok(MirScalarExpr::Column(
                    literal
                        .to_string()
                        .parse::<usize>()
                        .map_err(|s| s.to_string())?,
                ));
            }
            Err(format!("Invalid column specification {:?}", token))
        }

        fn build_literal_inner(
            &mut self,
            litval: &str,
            littyp: ScalarType,
        ) -> Result<MirScalarExpr, String> {
            Ok(MirScalarExpr::literal_ok(
                get_datum_from_str(&litval.to_string()[..], &littyp)?,
                littyp,
            ))
        }

        fn build_literal<I>(
            &mut self,
            litval: &str,
            stream_iter: &mut I,
        ) -> Result<MirScalarExpr, String>
        where
            I: Iterator<Item = TokenTree>,
        {
            let littyp: Option<ScalarType> =
                deserialize_optional(stream_iter, "ScalarType", &RTI, self)?;
            match littyp {
                Some(littyp) => self.build_literal_inner(litval, littyp),
                None => {
                    let littyp = if ["true", "false", "null"].contains(&litval) {
                        ScalarType::Bool
                    } else if litval.starts_with('\"') {
                        ScalarType::String
                    } else {
                        ScalarType::Int64
                    };
                    self.build_literal_inner(litval, littyp)
                }
            }
        }

        fn build_literal_if_able<I>(
            &mut self,
            stream_iter: &mut I,
            symbol: TokenTree,
        ) -> Result<Option<MirScalarExpr>, String>
        where
            I: Iterator<Item = TokenTree>,
        {
            match extract_literal_string(stream_iter, &symbol)? {
                Some(litval) => Ok(Some(self.build_literal(&litval[..], stream_iter)?)),
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
                    symbol => self.build_literal_if_able(rest_of_stream, symbol)?,
                }
            } else {
                None
            };
            match result {
                Some(result) => Ok(Some(
                    serde_json::to_string(&result).map_err(|e| e.to_string())?,
                )),
                None => Ok(None),
            }
        }
    }

    /// Extends the test case syntax to support `MirRelationExpr`s
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
    /// ```
    /// (constant
    ///    [[<row1literal1>..<row1literaln>]..[<rowiliteral1>..<rowiliteraln>]]
    ///    [<scalartype1> .. <scalartypen>]
    /// )
    /// ```
    ///
    /// For convenience, a usize can be alternately specified as `#n`.
    /// We recommend specifying a usize as `#n` instead of `n` when the usize
    /// is a column reference.
    struct MirRelationExprDeserializeContext<'a> {
        inner_ctx: MirScalarExprDeserializeContext,
        catalog: &'a TestCatalog,
        scope: &'a mut Scope,
    }

    impl<'a> MirRelationExprDeserializeContext<'a> {
        fn new(catalog: &'a TestCatalog, scope: &'a mut Scope) -> Self {
            Self {
                inner_ctx: MirScalarExprDeserializeContext::default(),
                catalog,
                scope,
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
                                    match extract_literal_string(&mut inner_iter, &symbol)? {
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
            match self.inner_ctx.override_syntax(
                first_arg.clone(),
                rest_of_stream,
                type_name,
                rti,
            )? {
                Some(result) => Ok(Some(result)),
                None => {
                    if type_name == "MirRelationExpr" {
                        if let Some(result) =
                            self.build_special_mir_if_able(first_arg, rest_of_stream)?
                        {
                            return Ok(Some(
                                serde_json::to_string(&result).map_err(|e| e.to_string())?,
                            ));
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

    /* #region helper methods common to creating a MirScalarExpr::Literal and a
    MirRelationExpr::Constant */

    fn parse_litval<'a, F>(litval: &'a str, littyp: &str) -> Result<F, String>
    where
        F: std::str::FromStr,
    {
        litval
            .parse::<F>()
            .map_err(|_| format!("{} cannot be parsed into {}", litval, littyp))
    }

    fn get_datum_from_str<'a>(litval: &'a str, littyp: &ScalarType) -> Result<Datum<'a>, String> {
        if litval == "null" {
            return Ok(Datum::Null);
        }
        match littyp {
            ScalarType::Bool => Ok(Datum::from(parse_litval::<bool>(litval, "bool")?)),
            ScalarType::Decimal(_, _) => Ok(Datum::from(parse_litval::<i128>(litval, "i128")?)),
            ScalarType::Int32 => Ok(Datum::from(parse_litval::<i32>(litval, "i32")?)),
            ScalarType::Int64 => Ok(Datum::from(parse_litval::<i64>(litval, "i64")?)),
            ScalarType::String => Ok(Datum::from(litval.trim_matches('"'))),
            _ => Err(format!("Unsupported literal type {:?}", littyp)),
        }
    }

    fn extract_literal_string<I>(
        stream_iter: &mut I,
        symbol: &TokenTree,
    ) -> Result<Option<String>, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        match symbol {
            TokenTree::Ident(ident) => {
                if ["true", "false", "null"].contains(&&ident.to_string()[..]) {
                    Ok(Some(ident.to_string()))
                } else {
                    Ok(None)
                }
            }
            TokenTree::Literal(literal) => Ok(Some(literal.to_string())),
            TokenTree::Punct(punct) if punct.as_char() == '-' => {
                match stream_iter.next() {
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

    /* #endregion */
    /* #region */

    fn handle_cat(spec: String, cat: &mut TestCatalog) -> Result<(), Error> {
        let input_stream: TokenStream = spec
            .parse()
            .map_err(|e: proc_macro2::LexError| anyhow!(e.to_string()))?;
        let mut stream_iter = input_stream.into_iter();
        match stream_iter.next() {
            Some(TokenTree::Group(group)) => {
                let mut inner_iter = group.stream().into_iter().peekable();
                match inner_iter.next() {
                    // syntax for defining a source is
                    // `(defsource [types_of_cols] [[optional_sets_of_key_cols]])`
                    Some(TokenTree::Ident(ident)) if &ident.to_string()[..] == "defsource" => {
                        let name = match inner_iter.next() {
                            Some(TokenTree::Ident(ident)) => Ok(ident.to_string()),
                            invalid_token => {
                                Err(anyhow!("invalid source name: {:?}", invalid_token))
                            }
                        }?;

                        let mut ctx = GenericTestDeserializeContext::default();
                        let scalar_types: Vec<ScalarType> =
                            deserialize(&mut inner_iter, "Vec<ScalarType>", &RTI, &mut ctx)
                                .map_err(|s| anyhow!(s))?;

                        let mut typ = RelationType::new(
                            scalar_types
                                .into_iter()
                                .map(|t| t.nullable(true))
                                .collect::<Vec<_>>(),
                        );

                        let keys: Option<Vec<Vec<usize>>> = deserialize_optional(
                            &mut inner_iter,
                            "Vec<Vec<usize>>",
                            &RTI,
                            &mut ctx,
                        )
                        .map_err(|s| anyhow!(s))?;
                        if let Some(keys) = keys {
                            typ = typ.with_keys(keys)
                        }

                        cat.insert(&name, typ);
                        Ok(())
                    }
                    s => Err(anyhow!("not a valid catalog command: {:?}", s)),
                }
            }
            s => Err(anyhow!("not a valid catalog command spec: {:?}", s)),
        }
    }

    fn generate_explanation(
        rel: &MirRelationExpr,
        cat: &TestCatalog,
        format: Option<&Vec<String>>,
    ) -> String {
        let mut explanation = ViewExplanation::new(rel, cat);
        if let Some(format) = format {
            if format.contains(&"types".to_string()) {
                explanation.explain_types();
            }
        }
        explanation.to_string()
    }

    fn build_scalar(s: &str) -> Result<MirScalarExpr, Error> {
        deserialize(
            &mut s
                .parse::<TokenStream>()
                .map_err(|e| anyhow!(e.to_string()))?
                .into_iter(),
            "MirScalarExpr",
            &RTI,
            &mut MirScalarExprDeserializeContext::default(),
        )
        .map_err(|s| anyhow!(s))
    }

    fn build_rel(
        s: &str,
        catalog: &TestCatalog,
        scope: &mut Scope,
    ) -> Result<MirRelationExpr, Error> {
        deserialize(
            &mut s
                .parse::<TokenStream>()
                .map_err(|e| anyhow!(e.to_string()))?
                .into_iter(),
            "MirRelationExpr",
            &RTI,
            &mut MirRelationExprDeserializeContext::new(catalog, scope),
        )
        .map_err(|s| anyhow!(s))
    }

    /* #endregion */

    fn run_testcase(
        s: &str,
        cat: &TestCatalog,
        args: &HashMap<String, Vec<String>>,
        test_type: TestType,
    ) -> Result<String, Error> {
        let mut scope = Scope {
            objects: HashMap::new(),
            names: HashMap::new(),
        };

        let mut rel = build_rel(s, cat, &mut scope)?;

        let mut id_gen = Default::default();
        let indexes = HashMap::new();
        for t in args.get("apply").cloned().unwrap_or_else(Vec::new).iter() {
            get_transform(t)?.transform(
                &mut rel,
                TransformArgs {
                    id_gen: &mut id_gen,
                    indexes: &indexes,
                },
            )?;
        }

        match test_type {
            TestType::Opt => {
                let mut opt: Optimizer = Default::default();
                rel = opt.optimize(rel, &HashMap::new()).unwrap().into_inner();

                Ok(generate_explanation(&rel, &cat, args.get("format")))
            }
            TestType::Build => Ok(generate_explanation(&rel, &cat, args.get("format"))),
            TestType::Steps => {
                // TODO(justin): this thing does not currently peek into fixpoints, so it's not
                // that helpful for optimizations that involve those (which is most of them).
                let opt: Optimizer = Default::default();
                let mut out = String::new();
                // Buffer of the names of the transformations that have been applied with no changes.
                let mut no_change: Vec<String> = Vec::new();

                writeln!(
                    out,
                    "{}",
                    generate_explanation(&rel, &cat, args.get("format"))
                )?;
                writeln!(out, "====")?;

                for transform in opt.transforms.iter() {
                    let prev = rel.clone();
                    transform.transform(
                        &mut rel,
                        TransformArgs {
                            id_gen: &mut id_gen,
                            indexes: &indexes,
                        },
                    )?;

                    if rel != prev {
                        if no_change.len() > 0 {
                            write!(out, "No change:")?;
                            let mut sep = " ";
                            for t in no_change {
                                write!(out, "{}{}", sep, t)?;
                                sep = ", ";
                            }
                            writeln!(out, "\n====")?;
                        }
                        no_change = vec![];

                        write!(out, "Applied {:?}:", transform)?;
                        writeln!(
                            out,
                            "\n{}",
                            generate_explanation(&rel, &cat, args.get("format"))
                        )?;
                        writeln!(out, "====")?;
                    } else {
                        no_change.push(format!("{:?}", transform));
                    }
                }

                if no_change.len() > 0 {
                    write!(out, "No change:")?;
                    let mut sep = " ";
                    for t in no_change {
                        write!(out, "{}{}", sep, t)?;
                        sep = ", ";
                    }
                    writeln!(out, "\n====")?;
                }

                writeln!(out, "Final:")?;
                writeln!(
                    out,
                    "{}",
                    generate_explanation(&rel, &cat, args.get("format"))
                )?;
                writeln!(out, "====")?;

                Ok(out)
            }
        }
    }

    fn get_transform(name: &str) -> Result<Box<dyn Transform>, Error> {
        // TODO(justin): is there a way to just extract these from the Optimizer list of
        // transforms?
        match name {
            "ColumnKnowledge" => Ok(Box::new(transform::column_knowledge::ColumnKnowledge)),
            "Demand" => Ok(Box::new(transform::demand::Demand)),
            "JoinFusion" => Ok(Box::new(transform::fusion::join::Join)),
            "LiteralLifting" => Ok(Box::new(transform::map_lifting::LiteralLifting)),
            "NonNullRequirements" => Ok(Box::new(
                transform::nonnull_requirements::NonNullRequirements,
            )),
            "PredicatePushdown" => Ok(Box::new(transform::predicate_pushdown::PredicatePushdown)),
            "ProjectionExtraction" => Ok(Box::new(
                transform::projection_extraction::ProjectionExtraction,
            )),
            "ProjectionLifting" => Ok(Box::new(transform::projection_lifting::ProjectionLifting)),
            "ReductionPushdown" => Ok(Box::new(transform::reduction_pushdown::ReductionPushdown)),
            _ => Err(anyhow!(
                "no transform named {} (you might have to add it to get_transform)",
                name
            )),
        }
    }

    #[test]
    fn run() {
        datadriven::walk("tests/testdata", |f| {
            let mut catalog = TestCatalog {
                objects: HashMap::new(),
                names: HashMap::new(),
            };
            f.run(move |s| -> String {
                match s.directive.as_str() {
                    "cat" => match handle_cat(s.input.clone(), &mut catalog) {
                        Ok(()) => String::from("ok\n"),
                        Err(err) => format!("error: {}\n", err),
                    },
                    // tests that we can build `MirScalarExpr`s
                    "buildscalar" => match build_scalar(&s.input) {
                        Ok(msg) => format!("{}\n", msg.to_string().trim_end()),
                        Err(err) => format!("error: {}\n", err),
                    },
                    "build" => match run_testcase(&s.input, &catalog, &s.args, TestType::Build) {
                        // Generally, explanations for fully optimized queries
                        // are not allowed to have whitespace at the end;
                        // however, a partially optimized query can.
                        // Since clippy rejects test results with trailing
                        // whitespace, remove whitespace before comparing results.
                        Ok(msg) => format!("{}\n", msg.trim_end().to_string()),
                        Err(err) => format!("error: {}\n", err),
                    },
                    "opt" => match run_testcase(&s.input, &catalog, &s.args, TestType::Opt) {
                        Ok(msg) => msg,
                        Err(err) => format!("error: {}\n", err),
                    },
                    "steps" => match run_testcase(&s.input, &catalog, &s.args, TestType::Steps) {
                        Ok(msg) => msg,
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
