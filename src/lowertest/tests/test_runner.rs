// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(test)]
mod tests {
    use lowertest::*;

    use std::collections::HashMap;

    use lazy_static::lazy_static;
    use proc_macro2::{TokenStream, TokenTree};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize, MzStructReflect)]
    struct TestStruct1(Box<f64>);

    #[derive(Debug, Deserialize, Serialize, MzStructReflect)]
    struct TestStruct2(Vec<(usize, Vec<(String, usize)>, usize)>, String);

    #[derive(Debug, Deserialize, Serialize, MzStructReflect)]
    struct TestStruct3 {
        fizz: Vec<Option<bool>>,
        bizz: Vec<Vec<(TestStruct1, bool)>>,
    }

    #[derive(Debug, Deserialize, Serialize, MzEnumReflect)]
    enum TestEnum {
        SingleNamedField {
            foo: Vec<usize>,
        },
        MultiNamedFields {
            #[serde(default)]
            bar: Option<String>,
            #[serde(default)]
            baz: bool,
        },
        SingleUnnamedField(TestStruct1),
        MultiUnnamedFields(TestStruct2, TestStruct3, Box<TestEnum>),
        Unit,
    }

    gen_reflect_info_func!(
        produce_rti,
        [TestEnum],
        [TestStruct1, TestStruct2, TestStruct3]
    );

    lazy_static! {
        static ref RTI: ReflectedTypeInfo = produce_rti();
    }

    #[derive(Default)]
    struct TestOverrideDeserializeContext;

    impl TestDeserializeContext for TestOverrideDeserializeContext {
        /// This increments all numbers of type "usize" by one.
        /// If a positive f64 has been specified with +<the number>,
        /// ignore the +.
        /// Define an alternate syntax for `TestStruct2`:
        /// * (<usize1> "string") creates
        ///   `TestStruct2([(<usize1>, [("string", <usize1>)], <usize1>)], "string")`
        /// * "string" creates `TestStruct2([], "string")`
        /// * "<usize1>" creates
        ///   `TestStruct2([(<usize1>, [("", <usize1>)], <usize1>)], "")
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
            if type_name == "TestStruct2" {
                if let TokenTree::Literal(literal) = first_arg.clone() {
                    let litval = literal.to_string();
                    if litval.starts_with('"') {
                        return Ok(Some(format!("[[], {}]", litval)));
                    } else {
                        let usize_lit = litval.parse::<usize>().map_err(|_| {
                            format!("{} cannot be parsed as a usize or a string", litval)
                        })?;
                        let mut stream_peek = rest_of_stream.peekable();
                        let str_lit = if stream_peek.peek().is_some() {
                            match stream_peek.next() {
                                Some(TokenTree::Literal(literal)) => literal.to_string(),
                                unexpected => {
                                    return Err(format!(
                                        "unexpected second argument for TestStruct2 {:?}",
                                        unexpected
                                    ))
                                }
                            }
                        } else {
                            "".to_string()
                        };
                        return Ok(Some(
                            serde_json::to_string(&TestStruct2(
                                vec![(usize_lit, vec![(str_lit.clone(), usize_lit)], usize_lit)],
                                str_lit,
                            ))
                            .unwrap(),
                        ));
                    }
                }
            } else if type_name == "f64" {
                if let TokenTree::Punct(punct) = first_arg.clone() {
                    if punct.as_char() == '+' {
                        return to_json(rest_of_stream, type_name, rti, self);
                    }
                }
            } else if type_name == "usize" {
                if let TokenTree::Literal(literal) = first_arg {
                    let litval = literal
                        .to_string()
                        .parse::<usize>()
                        .map_err(|e| e.to_string())?;
                    return Ok(Some(format!("{}", litval + 1)));
                }
            }
            Ok(None)
        }
    }

    fn build(s: &str, args: &HashMap<String, Vec<String>>) -> Result<String, String> {
        let stream = s
            .to_string()
            .parse::<TokenStream>()
            .map_err(|e| e.to_string())?;
        let result: Option<TestEnum> = if args.get("override").is_some() {
            deserialize_optional(
                &mut stream.into_iter(),
                "TestEnum",
                &RTI,
                &mut TestOverrideDeserializeContext::default(),
            )
        } else {
            deserialize_optional(
                &mut stream.into_iter(),
                "TestEnum",
                &RTI,
                &mut GenericTestDeserializeContext::default(),
            )
        }?;
        Ok(format!("{:?}", result))
    }

    #[test]
    fn run() {
        datadriven::walk("tests/testdata", |f| {
            f.run(move |s| -> String {
                match s.directive.as_str() {
                    "build" => match build(&s.input, &s.args) {
                        Ok(msg) => format!("{}\n", msg.trim_end().to_string()),
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
