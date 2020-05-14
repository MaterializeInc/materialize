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
        ch >= 'a' && ch <= 'z'
            || ch >= 'A' && ch <= 'Z'
            || ch >= '0' && ch <= '9'
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
    use super::{Sexp, SexpParser};
    use anyhow::{anyhow, Error};
    use expr::{GlobalId, Id, IdHumanizer, JoinImplementation, RelationExpr, ScalarExpr};
    use repr::{ColumnType, Datum, RelationType, ScalarType};
    use std::collections::HashMap;
    use transform::{Optimizer, Transform, TransformArgs};

    struct TestCatalog {
        objects: HashMap<String, (Id, RelationType)>,
        names: HashMap<Id, String>,
    }

    impl<'a> TestCatalog {
        fn insert(&mut self, name: &str, typ: RelationType) {
            // TODO(justin): error on dup name?
            let id = Id::Global(GlobalId::User(self.objects.len() as u64));
            self.objects.insert(name.to_string(), (id, typ));
            self.names.insert(id, name.to_string());
        }

        fn get(&'a self, name: &str) -> Option<&'a (Id, RelationType)> {
            self.objects.get(name)
        }
    }

    impl IdHumanizer for TestCatalog {
        fn humanize_id(&self, id: Id) -> Option<String> {
            self.names.get(&id).map(|s| s.to_string())
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

    fn extract_idx(s: Sexp) -> Result<usize, Error> {
        match s {
            Sexp::Atom(a) if a.starts_with('#') => {
                Ok(a.chars().skip(1).collect::<String>().parse()?)
            }
            s => Err(anyhow!("expected {} to be a column reference", s)),
        }
    }

    fn build_rel(s: Sexp, catalog: &TestCatalog) -> Result<RelationExpr, Error> {
        // TODO(justin): cleaner destructuring of a sexp here: this is too lenient at the moment,
        // since extra arguments to an operator are ignored.
        match try_atom(&nth(&s, 0)?)?.as_str() {
            "get" => {
                let name = try_atom(&nth(&s, 1)?)?;
                match catalog.get(&name) {
                    None => Err(anyhow!("no catalog object named {}", name)),
                    Some((id, typ)) => Ok(RelationExpr::Get {
                        id: id.clone(),
                        typ: typ.clone(),
                    }),
                }
            }
            "map" => Ok(RelationExpr::Map {
                input: Box::new(build_rel(nth(&s, 1)?, catalog)?),
                scalars: build_scalar_list(nth(&s, 2)?)?,
            }),
            "project" => Ok(RelationExpr::Project {
                input: Box::new(build_rel(nth(&s, 1)?, catalog)?),
                outputs: try_list(nth(&s, 2)?)?
                    .into_iter()
                    .map(extract_idx)
                    .collect::<Result<Vec<usize>, Error>>()?,
            }),
            "join" => {
                let inputs = try_list(nth(&s, 1)?)?
                    .into_iter()
                    .map(|r| build_rel(r, catalog))
                    .collect::<Result<Vec<RelationExpr>, Error>>()?;

                // TODO(justin): ????
                let equivalences: Vec<Vec<ScalarExpr>> = try_list(nth(&s, 2)?)?
                    .into_iter()
                    .map(try_list)
                    .collect::<Result<Vec<Vec<Sexp>>, Error>>()?
                    .into_iter()
                    .map(|e| e.into_iter().map(build_scalar).collect())
                    .collect::<Result<Vec<Vec<ScalarExpr>>, Error>>()?;

                Ok(RelationExpr::Join {
                    inputs,
                    equivalences,
                    demand: None,
                    implementation: JoinImplementation::Unimplemented,
                })
            }
            "filter" => Ok(RelationExpr::Filter {
                input: Box::new(build_rel(nth(&s, 1)?, catalog)?),
                predicates: build_scalar_list(nth(&s, 2)?)?,
            }),
            name => Err(anyhow!("expected {} to be a relational operator", name)),
        }
    }

    fn build_scalar_list(s: Sexp) -> Result<Vec<ScalarExpr>, Error> {
        try_list(s)?
            .into_iter()
            .map(build_scalar)
            .collect::<Result<Vec<ScalarExpr>, Error>>()
    }

    fn build_scalar(s: Sexp) -> Result<ScalarExpr, Error> {
        match s {
            Sexp::Atom(s) => match s.as_str() {
                "true" => Ok(ScalarExpr::literal(
                    Ok(Datum::True),
                    ColumnType {
                        nullable: false,
                        scalar_type: ScalarType::Bool,
                    },
                )),
                "false" => Ok(ScalarExpr::literal(
                    Ok(Datum::False),
                    ColumnType {
                        nullable: false,
                        scalar_type: ScalarType::Bool,
                    },
                )),
                s => Ok(ScalarExpr::Column(extract_idx(Sexp::Atom(s.to_string()))?)),
            },
            s => Err(anyhow!("expected {} to be a scalar", s)),
        }
    }

    fn handle_cat(s: Sexp, cat: &mut TestCatalog) -> Result<(), Error> {
        match try_atom(&nth(&s, 0)?)?.as_str() {
            "defsource" => {
                let name = try_atom(&nth(&s, 1)?)?;
                let types = try_list_of_atoms(&nth(&s, 2)?)?;

                let col_types = types
                    .iter()
                    .map(|e| match e.as_str() {
                        "int32" => Ok(ColumnType::new(ScalarType::Int32)),
                        "int64" => Ok(ColumnType::new(ScalarType::Int64)),
                        _ => Err(anyhow!("unknown type {}", e)),
                    })
                    .collect::<Result<Vec<ColumnType>, Error>>()?;

                let typ = RelationType::new(col_types);

                cat.insert(&name, typ);
                Ok(())
            }
            s => Err(anyhow!("not a valid catalog command: {}", s)),
        }
    }

    fn handle_plan(
        s: &str,
        cat: &TestCatalog,
        args: &HashMap<String, Vec<String>>,
        optimize: bool,
    ) -> Result<String, Error> {
        let mut rel = build_rel(SexpParser::parse_sexp(s.to_string())?, &cat)?;

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

        if optimize {
            let mut opt: Optimizer = Default::default();
            rel = opt.optimize(rel, &HashMap::new()).unwrap().into_inner();
        }

        Ok(rel.explain(cat).to_string())
    }

    fn get_transform(name: &str) -> Result<Box<dyn Transform>, Error> {
        match name {
            "predicate_pushdown" => Ok(Box::new(transform::predicate_pushdown::PredicatePushdown)),
            _ => Err(anyhow!("no transform named {}", name)),
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
                    "build" => match handle_plan(&s.input, &catalog, &s.args, false) {
                        Ok(msg) => msg,
                        Err(err) => format!("error: {}\n", err),
                    },
                    "opt" => match handle_plan(&s.input, &catalog, &s.args, true) {
                        Ok(msg) => msg,
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
