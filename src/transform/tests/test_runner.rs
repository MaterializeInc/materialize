// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use anyhow::{anyhow, Error};

/// This module defines a small language for directly constructing RelationExprs and running
/// various optimizations on them. It uses datadriven, so the output of each test can be rewritten
/// by setting the REWRITE environment variable.
/// TODO(justin):
/// * It's currently missing a mechanism to run just a single test file
/// * There is some duplication between this and the SQL planner
/// * Not all operators supported (Reduce)

#[derive(Debug, Clone)]
enum Sexp {
    List(Vec<Sexp>),
    Atom(String),
}

impl fmt::Display for Sexp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Sexp::Atom(s) => write!(f, "{}", s),
            Sexp::List(es) => {
                write!(f, "(")?;
                let mut split = "";
                for e in es {
                    write!(f, "{}{}", split, e)?;
                    split = " ";
                }
                write!(f, ")")
            }
        }
    }
}

struct SexpParser {
    s: Vec<char>,
    i: usize,
}

impl SexpParser {
    fn peek(&self) -> Option<char> {
        if self.i < self.s.len() {
            Some(self.s[self.i])
        } else {
            None
        }
    }

    fn munch(&mut self) {
        loop {
            match self.peek() {
                Some(ch) if ch.is_whitespace() => self.i += 1,
                _ => break,
            }
        }
    }

    fn closer(ch: char) -> Option<char> {
        match ch {
            '(' => Some(')'),
            '[' => Some(']'),
            '{' => Some('}'),
            _ => None,
        }
    }

    fn is_atom_char(ch: char) -> bool {
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ('0'..='9').contains(&ch)
            || ch == '-'
            || ch == '_'
            || ch == '#'
    }

    fn parse(&mut self) -> Result<Sexp, Error> {
        self.munch();
        match self.peek() {
            None => Err(anyhow!(String::from("unexpected end of sexp"))),
            Some(e @ '(') | Some(e @ '[') | Some(e @ '{') => {
                self.i += 1;
                let mut result = Vec::new();

                while self.peek() != SexpParser::closer(e) {
                    result.push(self.parse()?);
                    self.munch();
                }
                self.i += 1;

                Ok(Sexp::List(result))
            }
            Some(ch) if SexpParser::is_atom_char(ch) => {
                let start = self.i;
                while let Some(ch) = self.peek() {
                    if !SexpParser::is_atom_char(ch) {
                        break;
                    }
                    self.i += 1;
                }
                let end = self.i;
                self.munch();
                let word: String = self.s[start..end].iter().collect();
                Ok(Sexp::Atom(word))
            }
            Some(ch) => Err(anyhow!("unexpected: {}", ch)),
        }
    }

    fn parse_sexp(s: String) -> Result<Sexp, Error> {
        let mut p = SexpParser {
            s: s.chars().collect(),
            i: 0,
        };

        p.parse()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt::Write;

    use anyhow::{anyhow, bail, Error};

    use expr::explain::ViewExplanation;
    use expr::{
        DummyHumanizer, ExprHumanizer, GlobalId, Id, JoinImplementation, LocalId, MirRelationExpr,
        MirScalarExpr,
    };
    use repr::{ColumnType, Datum, RelationType, Row, ScalarType};
    use transform::{Optimizer, Transform, TransformArgs};

    use super::{Sexp, SexpParser};

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

    fn nth(s: &Sexp, n: usize) -> Result<Sexp, Error> {
        match s {
            Sexp::List(l) => {
                if n >= l.len() {
                    Err(anyhow!("can't take {}[{}]", s, n))
                } else {
                    Ok(l[n].clone())
                }
            }
            _ => Err(anyhow!("can't take {}[{}]", s, n)),
        }
    }

    fn try_list(s: Sexp) -> Result<Vec<Sexp>, Error> {
        match s {
            Sexp::List(s) => Ok(s),
            _ => Err(anyhow!("expected {} to be a list", s)),
        }
    }

    fn try_atom(s: &Sexp) -> Result<String, Error> {
        match s {
            Sexp::Atom(s) => Ok(s.clone()),
            _ => Err(anyhow!("expected {} to be an atom", s)),
        }
    }

    fn try_list_of_atoms(s: &Sexp) -> Result<Vec<String>, Error> {
        match s {
            Sexp::List(s) => s.iter().map(|s| try_atom(s)).collect(),
            _ => Err(anyhow!("expected {} to be a list", s)),
        }
    }

    /// Extracts a column reference expression from a Sexp.
    fn extract_idx(s: Sexp) -> Result<usize, Error> {
        match s {
            Sexp::Atom(a) if a.starts_with('#') => {
                Ok(a.chars().skip(1).collect::<String>().parse()?)
            }
            s => Err(anyhow!("expected {} to be a column reference", s)),
        }
    }

    fn build_rel(
        s: Sexp,
        catalog: &TestCatalog,
        scope: &mut Scope,
    ) -> Result<MirRelationExpr, Error> {
        // TODO(justin): cleaner destructuring of a sexp here: this is too lenient at the moment,
        // since extra arguments to an operator are ignored.
        match try_atom(&nth(&s, 0)?)?.as_str() {
            // (get <name>)
            "get" => {
                let name = try_atom(&nth(&s, 1)?)?;
                match scope.get(&name) {
                    Some((id, typ)) => Ok(MirRelationExpr::Get { id, typ }),
                    None => match catalog.get(&name) {
                        None => Err(anyhow!("no catalog object named {}", name)),
                        Some((id, typ)) => Ok(MirRelationExpr::Get {
                            id: Id::Global(*id),
                            typ: typ.clone(),
                        }),
                    },
                }
            }
            // (let <name> <value> <body>)
            "let" => {
                let name = try_atom(&nth(&s, 1)?)?;
                let value = build_rel(nth(&s, 2)?, catalog, scope)?;

                let (id, prev) = scope.insert(&name, value.typ());

                let body = build_rel(nth(&s, 3)?, catalog, scope)?;

                if let Some((old_id, old_val)) = prev {
                    scope.set(&name, old_id, old_val);
                } else {
                    scope.remove(&name)
                }

                Ok(MirRelationExpr::Let {
                    id,
                    value: Box::new(value),
                    body: Box::new(body),
                })
            }
            // (map <input> [expressions])
            "map" => Ok(MirRelationExpr::Map {
                input: Box::new(build_rel(nth(&s, 1)?, catalog, scope)?),
                scalars: build_scalar_list(nth(&s, 2)?)?,
            }),
            // (project <input> [<col refs>])
            "project" => Ok(MirRelationExpr::Project {
                input: Box::new(build_rel(nth(&s, 1)?, catalog, scope)?),
                outputs: try_list(nth(&s, 2)?)?
                    .into_iter()
                    .map(extract_idx)
                    .collect::<Result<Vec<usize>, Error>>()?,
            }),
            // (constant [<rows>] [<types>])
            "constant" => {
                // TODO(justin): ...fix this.
                let rows: Vec<(Row, isize)> = try_list(nth(&s, 1)?)?
                    .into_iter()
                    .map(try_list)
                    .collect::<Result<Vec<Vec<Sexp>>, Error>>()?
                    .into_iter()
                    .map(|e| {
                        e.into_iter()
                            .map(build_scalar)
                            .collect::<Result<Vec<MirScalarExpr>, Error>>()
                    })
                    .collect::<Result<Vec<Vec<MirScalarExpr>>, Error>>()?
                    .iter()
                    .map(move |exprs| {
                        Ok(Row::pack_slice(
                            &exprs
                                .iter()
                                .map(|e| match e {
                                    MirScalarExpr::Literal(r, _) => {
                                        Ok(r.as_ref().unwrap().iter().next().unwrap().clone())
                                    }
                                    _ => bail!("exprs in constant must be literals"),
                                })
                                .collect::<Result<Vec<Datum>, Error>>()?,
                        ))
                    })
                    .collect::<Result<Vec<Row>, Error>>()?
                    .iter()
                    .map(|r| (r.clone(), 1))
                    .collect::<Vec<(Row, isize)>>();

                Ok(MirRelationExpr::Constant {
                    rows: Ok(rows),
                    typ: parse_type_list(nth(&s, 2)?)?,
                })
            }
            // (join [<inputs>] [<equivalences>]])
            "join" => {
                let inputs = try_list(nth(&s, 1)?)?
                    .into_iter()
                    .map(|r| build_rel(r, catalog, scope))
                    .collect::<Result<Vec<MirRelationExpr>, Error>>()?;

                // TODO(justin): is there a way to make this more comprehensible?
                let equivalences: Vec<Vec<MirScalarExpr>> = try_list(nth(&s, 2)?)?
                    .into_iter()
                    .map(try_list)
                    .collect::<Result<Vec<Vec<Sexp>>, Error>>()?
                    .into_iter()
                    .map(|e| e.into_iter().map(build_scalar).collect())
                    .collect::<Result<Vec<Vec<MirScalarExpr>>, Error>>()?;

                Ok(MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    demand: None,
                    implementation: JoinImplementation::Unimplemented,
                })
            }
            // (union [<inputs>])
            "union" => {
                let inputs = try_list(nth(&s, 1)?)?
                    .into_iter()
                    .map(|r| build_rel(r, catalog, scope))
                    .collect::<Result<Vec<MirRelationExpr>, Error>>()?;

                Ok(MirRelationExpr::Union {
                    base: Box::new(inputs[0].clone()),
                    inputs: inputs[1..].to_vec(),
                })
            }
            // (negate <input>)
            "negate" => Ok(MirRelationExpr::Negate {
                input: Box::new(build_rel(nth(&s, 1)?, catalog, scope)?),
            }),
            // (filter <input> <predicate>)
            "filter" => Ok(MirRelationExpr::Filter {
                input: Box::new(build_rel(nth(&s, 1)?, catalog, scope)?),
                predicates: build_scalar_list(nth(&s, 2)?)?,
            }),
            // (arrange-by <input> [<keys>])
            "arrange-by" => Ok(MirRelationExpr::ArrangeBy {
                input: Box::new(build_rel(nth(&s, 1)?, catalog, scope)?),
                keys: try_list(nth(&s, 2)?)?
                    .into_iter()
                    .map(build_scalar_list)
                    .collect::<Result<Vec<Vec<MirScalarExpr>>, Error>>()?,
            }),
            // TODO(justin): add the rest of the operators.
            name => Err(anyhow!("expected {} to be a relational operator", name)),
        }
    }

    fn build_scalar_list(s: Sexp) -> Result<Vec<MirScalarExpr>, Error> {
        try_list(s)?
            .into_iter()
            .map(build_scalar)
            .collect::<Result<Vec<MirScalarExpr>, Error>>()
    }

    // TODO(justin): is there some way to re-use the sql parser/builder for this?
    fn build_scalar(s: Sexp) -> Result<MirScalarExpr, Error> {
        match s {
            // TODO(justin): support more scalar exprs.
            Sexp::Atom(s) => match s.as_str() {
                "true" => Ok(MirScalarExpr::literal(Ok(Datum::True), ScalarType::Bool)),
                "false" => Ok(MirScalarExpr::literal(Ok(Datum::False), ScalarType::Bool)),
                s => {
                    match s.chars().next() {
                        None => {
                            // It shouldn't have parsed as an atom originally.
                            unreachable!();
                        }
                        Some('#') => Ok(MirScalarExpr::Column(extract_idx(Sexp::Atom(
                            s.to_string(),
                        ))?)),
                        Some('0') | Some('1') | Some('2') | Some('3') | Some('4') | Some('5')
                        | Some('6') | Some('7') | Some('8') | Some('9') => {
                            Ok(MirScalarExpr::literal(
                                Ok(Datum::Int64(s.parse::<i64>()?)),
                                ScalarType::Int64,
                            ))
                        }
                        _ => Err(anyhow!("couldn't parse scalar: {}", s)),
                    }
                }
            },
            s => Err(anyhow!("expected {} to be a scalar", s)),
        }
    }

    fn parse_type_list(s: Sexp) -> Result<RelationType, Error> {
        let types = try_list_of_atoms(&s)?;

        let col_types = types
            .iter()
            .map(|e| match e.as_str() {
                "int32" => Ok(ScalarType::Int32.nullable(true)),
                "int64" => Ok(ScalarType::Int64.nullable(true)),
                "bool" => Ok(ScalarType::Bool.nullable(true)),
                _ => Err(anyhow!("unknown type {}", e)),
            })
            .collect::<Result<Vec<ColumnType>, Error>>()?;

        Ok(RelationType::new(col_types))
    }

    fn parse_key_list(s: Sexp) -> Result<Vec<Vec<usize>>, Error> {
        let keys = try_list(s)?
            .into_iter()
            .map(|s2| {
                let result = try_list_of_atoms(&s2)?
                    .into_iter()
                    .map(|e| Ok(e.parse::<usize>()?))
                    .collect::<Result<Vec<usize>, Error>>();
                result
            })
            .collect::<Result<Vec<Vec<usize>>, Error>>()?;

        Ok(keys)
    }

    fn handle_cat(s: Sexp, cat: &mut TestCatalog) -> Result<(), Error> {
        match try_atom(&nth(&s, 0)?)?.as_str() {
            "defsource" => {
                let name = try_atom(&nth(&s, 1)?)?;

                let mut typ = parse_type_list(nth(&s, 2)?)?;

                if let Ok(sexp) = nth(&s, 3) {
                    typ.keys = parse_key_list(sexp)?;
                }
                cat.insert(&name, typ);
                Ok(())
            }
            s => Err(anyhow!("not a valid catalog command: {}", s)),
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
        let mut rel = build_rel(SexpParser::parse_sexp(s.to_string())?, &cat, &mut scope)?;

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
            "PredicatePushdown" => Ok(Box::new(transform::predicate_pushdown::PredicatePushdown)),
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
                    "cat" => {
                        match handle_cat(
                            SexpParser::parse_sexp(s.input.clone()).unwrap(),
                            &mut catalog,
                        ) {
                            Ok(()) => String::from("ok\n"),
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
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
