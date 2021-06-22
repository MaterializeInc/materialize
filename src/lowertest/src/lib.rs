// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for testing lower layers of the Materialize stack.
//!
//! See [README.md].

use std::collections::HashMap;

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use serde::de::DeserializeOwned;

use ore::str::separated;

pub use lowertest_derive::{gen_reflect_info_func, MzEnumReflect, MzStructReflect};

/// A trait for listing the variants of an enum and the fields of each variant.
///
/// The information listed about the fields help build a JSON string that can
/// be correctly deserialized into the enum.
pub trait MzEnumReflect {
    /// Returns a mapping of the variants of an enum to its fields.
    ///
    /// The first vector comprises the names of the variant's fields. It is
    /// empty if the variant has no fields or if the variant's fields are
    /// unnamed.
    ///
    /// The second vector comprises the types of the variant's fields. It is
    /// empty if the variant has no fields.
    fn mz_enum_reflect() -> HashMap<&'static str, (Vec<&'static str>, Vec<&'static str>)>;
}

/// A trait for listing the fields of a struct.
///
/// The information listed about the fields help build a JSON string that can
/// be correctly deserialized into the struct.
pub trait MzStructReflect {
    /// Returns the fields of a struct.
    ///
    /// The first vector comprises the names of the struct's fields. It is
    /// empty if the struct has no fields or if the struct's fields are
    /// unnamed.
    ///
    /// The second vector comprises the types of the struct's fields. It is
    /// empty if the struct has no fields.
    fn mz_struct_reflect() -> (Vec<&'static str>, Vec<&'static str>);
}

pub fn parse_str(s: &str) -> Result<TokenStream, String> {
    s.parse::<TokenStream>().map_err(|e| e.to_string())
}

/// If the `stream_iter` is not empty, deserialize the next `TokenTree` into a `D`.
///
/// See [`to_json`] for the object spec syntax.
///
/// `type_name` should be `D` in string form.
///
/// `stream_iter` will advance by one `TokenTree` no matter the result.
pub fn deserialize_optional<D, I, C>(
    stream_iter: &mut I,
    type_name: &'static str,
    rti: &ReflectedTypeInfo,
    ctx: &mut C,
) -> Result<Option<D>, String>
where
    C: TestDeserializeContext,
    D: DeserializeOwned,
    I: Iterator<Item = TokenTree>,
{
    match to_json(stream_iter, type_name, rti, ctx)? {
        Some(j) => Ok(Some(serde_json::from_str::<D>(&j).map_err(|e| {
            format!("String while serializing: {}\nOriginal JSON: {}", e, j)
        })?)),
        None => Ok(None),
    }
}

/// Deserialize the next `TokenTree` into a `D` object.
///
/// See [`to_json`] for the object spec syntax.
///
/// `type_name` should be `D` in string form.
///
/// `stream_iter` will advance by one `TokenTree` no matter the result.
pub fn deserialize<D, I, C>(
    stream_iter: &mut I,
    type_name: &'static str,
    rti: &ReflectedTypeInfo,
    ctx: &mut C,
) -> Result<D, String>
where
    C: TestDeserializeContext,
    D: DeserializeOwned,
    I: Iterator<Item = TokenTree>,
{
    deserialize_optional(stream_iter, type_name, rti, ctx)?
        .ok_or_else(|| format!("Empty spec for type {}", type_name))
}

/// Converts the next `TokenTree` in the stream into deserializable JSON.
///
/// Returns `None` if end of stream has been reached.
///
/// The JSON string should be deserializable into an object of type
/// `type_name`.
///
/// Default object syntax:
/// * An enum is represented as `(enum_variant_snake_case <arg1> <arg2> ..)`,
///   unless it is a unit enum, in which case it can also be represented as
///   `enum_variant_snake_case`. Enums can have optional arguments, which should
///   all come at the end.
/// * A struct is represented as `(<arg1> <arg2> ..)`
/// * A vec or tuple is represented as `[<elem1> <elem2> ..]`
/// * None/null is represented as `null`
/// * true (resp. false) is represented as `true` (resp. `false`)
/// * Strings are represented as `"something with quotations"`.
/// * A numeric value like -1 or 1.1 is represented as is.
/// * You can delimit arguments and elements using whitespace and/or commas.
///
/// `ctx` will extend and/or override the default syntax.
pub fn to_json<I, C>(
    stream_iter: &mut I,
    type_name: &str,
    rti: &ReflectedTypeInfo,
    ctx: &mut C,
) -> Result<Option<String>, String>
where
    C: TestDeserializeContext,
    I: Iterator<Item = TokenTree>,
{
    // Normalize the type name by stripping whitespace.
    let type_name = type_name.replace(" ", "");
    // Eliminate outer `Option<>` and `Box<>` from type names because they are
    // inconsequential when it comes to creating a correctly deserializable JSON
    // string.
    let type_name = if type_name.starts_with("Option<") && type_name.ends_with('>') {
        &type_name[7..(type_name.len() - 1)]
    } else if type_name.starts_with("Box<") && type_name.ends_with('>') {
        &type_name[4..(type_name.len() - 1)]
    } else {
        &type_name
    };
    if let Some(first_arg) = stream_iter.next() {
        // If the type refers to an enum or struct defined by us, go to a
        // special branch that allows reuse of code paths for the
        // `(<arg1>..<argn>)` syntax as well as the `<only_arg>` syntax.
        // Note that `parse_as_enum_or_struct` also calls
        // `ctx.override_syntax`.
        if let Some(result) =
            parse_as_enum_or_struct(first_arg.clone(), stream_iter, &type_name, rti, ctx)?
        {
            return Ok(Some(result));
        }
        // Resolving types that are not enums or structs defined by us.
        if let Some(result) =
            ctx.override_syntax(first_arg.clone(), stream_iter, &type_name, rti)?
        {
            return Ok(Some(result));
        }
        match first_arg {
            TokenTree::Group(group) => {
                let mut inner_iter = group.stream().into_iter();
                match group.delimiter() {
                    Delimiter::Bracket => {
                        if type_name.starts_with("Vec<") && type_name.ends_with('>') {
                            // This is a Vec<type_name>.
                            Ok(Some(format!(
                                "[{}]",
                                separated(
                                    ",",
                                    parse_as_vec(
                                        &mut inner_iter,
                                        &type_name[4..(type_name.len() - 1)],
                                        rti,
                                        ctx
                                    )?
                                    .iter()
                                )
                            )))
                        } else if type_name.starts_with('(') && type_name.ends_with(')') {
                            Ok(Some(format!(
                                "[{}]",
                                separated(
                                    ",",
                                    parse_as_tuple(
                                        &mut inner_iter,
                                        &type_name[1..(type_name.len() - 1)],
                                        rti,
                                        ctx
                                    )?
                                    .iter()
                                )
                            )))
                        } else {
                            Err(format!(
                                "Object specified with brackets {:?} has unsupported type {}",
                                inner_iter, type_name
                            ))
                        }
                    }
                    delim => Err(format!(
                        "Object spec {:?} (type {}) has unsupported delimiter {:?}",
                        inner_iter.collect::<Vec<_>>(),
                        type_name,
                        delim
                    )),
                }
            }
            TokenTree::Punct(punct) => {
                match punct.as_char() {
                    // Pretend the comma does not exist and process the
                    // next `TokenTree`
                    ',' => to_json(stream_iter, type_name, rti, ctx),
                    // Process the next `TokenTree` and prepend the
                    // punctuation. This enables support for negative
                    // numbers.
                    other => match to_json(stream_iter, type_name, rti, ctx)? {
                        Some(result) => Ok(Some(format!("{}{}", other, result))),
                        None => Ok(Some(other.to_string())),
                    },
                }
            }
            TokenTree::Ident(ident) => Ok(Some(ident.to_string())),
            TokenTree::Literal(literal) => Ok(Some(literal.to_string())),
        }
    } else {
        Ok(None)
    }
}

/// Info that must be combined with a spec to form deserializable JSON.
///
/// To add information about an enum, call
/// `enum_dict.insert("EnumType", EnumType::mz_enum_reflect())`
/// To add information about a struct, call
/// `struct_dict.insert("StructType", StructType::mz_struct_reflect())`
#[derive(Debug)]
pub struct ReflectedTypeInfo {
    pub enum_dict:
        HashMap<&'static str, HashMap<&'static str, (Vec<&'static str>, Vec<&'static str>)>>,
    pub struct_dict: HashMap<&'static str, (Vec<&'static str>, Vec<&'static str>)>,
}

/// A trait for extending and/or overriding the default test case syntax.
///
/// Note when creating an implementation of this trait that the
/// `[proc_macro2::TokenStream]` considers:
/// * null/true/false to be `Ident`s
/// * strings and positive numeric values (like 1 or 1.1) to be `Literal`s.
/// * negative numeric values to be a `Punct('-')` followed by a `Literal`.
pub trait TestDeserializeContext {
    /// Override the way that `first_arg` is resolved.
    ///
    /// `first_arg` is the first `TokenTree` of the `TokenStream`.
    /// `rest_of_stream` contains a reference to the rest of the stream.
    ///
    /// Returns Ok(Some(value)) if `first_arg` has been resolved.
    /// Returns Ok(None) if `first_arg` should be resolved using the default
    /// syntax. If returning Ok(None), the function implementation
    /// promises not to advance `rest_of_stream`.
    fn override_syntax<I>(
        &mut self,
        first_arg: TokenTree,
        rest_of_stream: &mut I,
        type_name: &str,
        rti: &ReflectedTypeInfo,
    ) -> Result<Option<String>, String>
    where
        I: Iterator<Item = TokenTree>;
}

/// Default `TestDeserializeContext`.
///
/// Does not override or extend any of the default syntax.
#[derive(Default)]
pub struct GenericTestDeserializeContext;

impl TestDeserializeContext for GenericTestDeserializeContext {
    fn override_syntax<I>(
        &mut self,
        _first_arg: TokenTree,
        _rest_of_stream: &mut I,
        _type_name: &str,
        _rti: &ReflectedTypeInfo,
    ) -> Result<Option<String>, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        Ok(None)
    }
}

/* #region helper functions */

/// Converts all `TokenTree`s into JSON deserializable to `type_name`.
fn parse_as_vec<I, C>(
    stream_iter: &mut I,
    type_name: &str,
    rti: &ReflectedTypeInfo,
    ctx: &mut C,
) -> Result<Vec<String>, String>
where
    C: TestDeserializeContext,
    I: Iterator<Item = TokenTree>,
{
    let mut result = Vec::new();
    while let Some(element) = to_json(stream_iter, type_name, rti, ctx)? {
        result.push(element);
    }
    Ok(result)
}

/// Converts all `TokenTree`s into JSON.
///
/// `type_name` is assumed to have been stripped of whitespace.
fn parse_as_tuple<I, C>(
    stream_iter: &mut I,
    type_name: &str,
    rti: &ReflectedTypeInfo,
    ctx: &mut C,
) -> Result<Vec<String>, String>
where
    C: TestDeserializeContext,
    I: Iterator<Item = TokenTree>,
{
    let mut next_elem_begin = 0;
    let mut result = Vec::new();
    loop {
        // The elements of the tuple can be a plain type, a nested tuple, or a
        // Box/Vec/Option with the argument being nested tuple.
        // `type1, (type2, type3), Vec<(type4, type5)>`
        // Thus, the type of the next element is whatever comes before the
        // next comma. Unless... a '(' comes from before the next comma, which
        // means it is a nested tuple, and the type of the next element is
        // whatever comes before the comma after the last ')'.
        let mut next_elem_end = type_name[next_elem_begin..]
            .find(',')
            .unwrap_or_else(|| type_name.len());
        if let Some(l_paren_pos) = type_name[next_elem_begin..].find('(') {
            if l_paren_pos < next_elem_end {
                if let Some(r_paren_pos) = type_name[next_elem_begin..].rfind(')') {
                    next_elem_end = next_elem_begin
                        + r_paren_pos
                        + type_name[(next_elem_begin + r_paren_pos)..]
                            .find(',')
                            .unwrap_or_else(|| type_name.len());
                }
            }
        };
        match to_json(
            stream_iter,
            &type_name[next_elem_begin..next_elem_end],
            rti,
            ctx,
        )? {
            Some(elem) => result.push(elem),
            // we have reached the end of the tuple. Assume that any remaining
            // elements of the tuple are optional.
            None => break,
        }
        if next_elem_end < type_name.len() {
            //skip over the comma
            next_elem_begin = next_elem_end + 1;
        } else {
            // assume that that we have reached the end of the tuple.
            break;
        }
    }
    Ok(result)
}

/// Converts stream into JSON if `type_name` refers to an enum or struct
///
/// Returns `Ok(Some(string))` if `type_name` refers to an enum or struct, and
/// there are no stream conversion errors.
/// Returns `Ok(None)` if `type_name` does not refer to an enum or struct.
fn parse_as_enum_or_struct<I, C>(
    first_arg: TokenTree,
    rest_of_stream: &mut I,
    type_name: &str,
    rti: &ReflectedTypeInfo,
    ctx: &mut C,
) -> Result<Option<String>, String>
where
    C: TestDeserializeContext,
    I: Iterator<Item = TokenTree>,
{
    if rti.enum_dict.contains_key(type_name) || rti.struct_dict.contains_key(type_name) {
        // An enum or a struct can be specified as `(arg1 .. argn)` or
        // `only_arg`. The goal here is to feed the enum/struct specification
        // into a common inner method that takes
        // `(first_token_of_spec, rest_of_tokens_comprising_spec)`
        match first_arg {
            TokenTree::Group(group) => {
                let mut inner_iter = group.stream().into_iter();
                match group.delimiter() {
                    Delimiter::Parenthesis => match inner_iter.next() {
                        // the spec is the inner `TokenStream`
                        Some(first_arg) => parse_as_enum_or_struct_inner(
                            first_arg,
                            &mut inner_iter,
                            type_name,
                            rti,
                            ctx,
                        ),
                        None => Ok(None),
                    },
                    _ => Ok(None),
                }
            }
            TokenTree::Punct(punct) => {
                // The spec is all consecutive puncts + the first non-punct
                // symbol. This allows for specifying structs with the first
                // argument being something like -1.1.
                let mut consecutive_punct = Vec::new();
                while let Some(token) = rest_of_stream.next() {
                    consecutive_punct.push(token);
                    match &consecutive_punct[consecutive_punct.len() - 1] {
                        TokenTree::Punct(_) => {}
                        _ => {
                            break;
                        }
                    }
                }
                parse_as_enum_or_struct_inner(
                    TokenTree::Punct(punct),
                    &mut consecutive_punct.into_iter(),
                    type_name,
                    rti,
                    ctx,
                )
            }
            other => {
                // The entire enum/struct is specified by the Ident/Literal,
                // so feed in (the_ident_literal, nothing)
                parse_as_enum_or_struct_inner(other, &mut std::iter::empty(), type_name, rti, ctx)
            }
        }
    } else {
        Ok(None)
    }
}

fn parse_as_enum_or_struct_inner<I, C>(
    first_arg: TokenTree,
    rest_of_stream: &mut I,
    type_name: &str,
    rti: &ReflectedTypeInfo,
    ctx: &mut C,
) -> Result<Option<String>, String>
where
    C: TestDeserializeContext,
    I: Iterator<Item = TokenTree>,
{
    if let Some(result) = ctx.override_syntax(first_arg.clone(), rest_of_stream, type_name, rti)? {
        Ok(Some(result))
    } else if let Some((f_names, f_types)) = rti.struct_dict.get(type_name).map(|r| r.clone()) {
        Ok(Some(to_json_fields(
            &mut (&mut std::iter::once(first_arg)).chain(rest_of_stream),
            f_names,
            f_types,
            rti,
            ctx,
        )?))
    } else if let TokenTree::Ident(ident) = first_arg {
        Ok(Some(to_json_generic_enum(
            ident.to_string(),
            rest_of_stream,
            type_name,
            rti,
            ctx,
        )?))
    } else {
        Ok(None)
    }
}

/// Converts the spec of an enum into deserializable JSON
fn to_json_generic_enum<I, C>(
    variant_snake_case: String,
    rest_of_stream: &mut I,
    type_name: &str,
    rti: &ReflectedTypeInfo,
    ctx: &mut C,
) -> Result<String, String>
where
    C: TestDeserializeContext,
    I: Iterator<Item = TokenTree>,
{
    // Convert the variant from snake_case to CamelCase
    let variant_camel_case = variant_snake_case
        .split('_')
        .map(|s| {
            let mut chars = s.chars();
            let result = chars
                .next()
                .map(|c| c.to_uppercase().chain(chars).collect::<String>())
                .unwrap_or_else(|| String::new());
            result
        })
        .collect::<Vec<_>>()
        .concat();
    let (f_names, f_types) = rti
        .enum_dict
        .get(type_name)
        .unwrap()
        .get(&variant_camel_case[..])
        .map(|v| v.clone())
        .ok_or_else(|| {
            format!(
                "{}::{} is not a supported enum.",
                type_name, variant_camel_case
            )
        })?;
    // If we reach end of stream before getting a value for each field,
    // we assume that the fields we don't have values for are optional.
    if f_types.is_empty() {
        // The JSON for a unit enum is just `"variant"`.
        Ok(format!("\"{}\"", variant_camel_case))
    } else {
        let fields = to_json_fields(rest_of_stream, f_names, f_types, rti, ctx)?;
        Ok(format!("{{\"{}\":{}}}", variant_camel_case, fields))
    }
}

/// Converts the spec for fields of an enum/struct into deserializable JSON.
///
/// `f_names` contains the names of the fields. If the fields are unnamed,
/// `f_names` is empty.
/// `f_types` contains the types of the fields.
fn to_json_fields<I, C>(
    stream_iter: &mut I,
    f_names: Vec<&'static str>,
    f_types: Vec<&'static str>,
    rti: &ReflectedTypeInfo,
    ctx: &mut C,
) -> Result<String, String>
where
    C: TestDeserializeContext,
    I: Iterator<Item = TokenTree>,
{
    let mut f_values = Vec::new();
    for t in f_types.iter() {
        match to_json(stream_iter, t, rti, ctx)? {
            Some(value) => f_values.push(value),
            None => {
                break;
            }
        }
    }
    if !f_names.is_empty() {
        // The JSON for named fields is
        // `{"arg1":<val1>, ..}}`.
        Ok(format!(
            "{{{}}}",
            separated(
                ",",
                f_names
                    .iter()
                    .zip(f_values.into_iter())
                    .map(|(n, v)| format!("\"{}\":{}", n, v))
            )
        ))
    } else {
        // The JSON for unnamed fields is
        // `[<val1>, ..]}`, unless it has only one field,
        // in which case the JSON is `<val1>`
        if f_types.len() == 1 {
            Ok(format!(
                "{}",
                f_values.pop().ok_or_else(|| {
                    "Cannot use default value for enum with unnamed single field".to_string()
                })?
            ))
        } else {
            Ok(format!("[{}]", separated(",", f_values.into_iter())))
        }
    }
}
/* #endregion */
