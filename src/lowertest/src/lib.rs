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
use serde_json::Value;

use mz_ore::{result::ResultExt, str::separated, str::StrExt};

pub use mz_lowertest_derive::MzReflect;

/* #region Parts of the public interface related to collecting information
about the fields of structs and enums. */

/// For [`to_json`] to create deserializable JSON for an instance of an type,
/// the type must derive this trait.
pub trait MzReflect {
    /// Adds names and types of the fields of the struct or enum to `rti`.
    ///
    /// The corresponding implementation of this method will be recursively
    /// called for each type referenced by the struct or enum.
    /// Check out the crate README for more details.
    fn add_to_reflected_type_info(rti: &mut ReflectedTypeInfo);
}

impl<T: MzReflect> MzReflect for Vec<T> {
    fn add_to_reflected_type_info(rti: &mut ReflectedTypeInfo) {
        T::add_to_reflected_type_info(rti);
    }
}

/// Info that must be combined with a spec to form deserializable JSON.
///
/// To add information required to construct a struct or enum,
/// call `Type::add_to_reflected_type_info(enum_dict, struct_dict)`
#[derive(Debug, Default)]
pub struct ReflectedTypeInfo {
    pub enum_dict:
        HashMap<&'static str, HashMap<&'static str, (Vec<&'static str>, Vec<&'static str>)>>,
    pub struct_dict: HashMap<&'static str, (Vec<&'static str>, Vec<&'static str>)>,
}

/* #endregion */

/* #region Public Utilities */

/// Converts `s` into a [proc_macro2::TokenStream]
pub fn tokenize(s: &str) -> Result<TokenStream, String> {
    s.parse::<TokenStream>().map_err_to_string()
}

/// Changes `"\"foo\""` to `"foo"`
pub fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') {
        s[1..(s.len() - 1)].replace("\\\"", "\"")
    } else {
        s.to_string()
    }
}

/* #endregion */

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
    ctx: &mut C,
) -> Result<Option<D>, String>
where
    C: TestDeserializeContext,
    D: DeserializeOwned + MzReflect,
    I: Iterator<Item = TokenTree>,
{
    let mut rti = ReflectedTypeInfo::default();
    D::add_to_reflected_type_info(&mut rti);
    match to_json(stream_iter, type_name, &rti, ctx)? {
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
    ctx: &mut C,
) -> Result<D, String>
where
    C: TestDeserializeContext,
    D: DeserializeOwned + MzReflect,
    I: Iterator<Item = TokenTree>,
{
    deserialize_optional(stream_iter, type_name, ctx)?
        .ok_or_else(|| format!("Empty spec for type {}", type_name))
}

/// Converts the next part of the stream into JSON deserializable into an object
/// of type `type_name`.
///
/// If the object is a zero-arg struct, this method will return
/// `Ok(Some("null"))` without looking at the stream.
///
/// Otherwise, it will try to convert the next `TokenTree` in the stream.
/// If end of stream has been reached, this method returns `Ok(None)`
///
/// The JSON string should be deserializable into an object of type
/// `type_name`.
///
/// Default object syntax:
/// * An enum is represented as `(enum_variant_snake_case <arg1> <arg2> ..)`,
///   unless it is a unit enum, in which case it can also be represented as
///   `enum_variant_snake_case`. Enums can have optional arguments, which should
///   all come at the end.
/// * A struct is represented as `(<arg1> <arg2> ..)`, unless it has no
///   arguments, in which case it is represented by the empty string.
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
    let (type_name, option_found) = normalize_type_name(type_name);

    // If the type is an zero-argument struct, resolve without reading from the stream.
    if let Some((_, f_types)) = rti.struct_dict.get(&type_name[..]) {
        if f_types.is_empty() {
            return Ok(Some("null".to_string()));
        }
    }

    if let Some(first_arg) = stream_iter.next() {
        // If type is `Option<T>`, convert the token to None if it is null,
        // otherwise, try to convert it to an instance of `T`.
        if option_found {
            if let TokenTree::Ident(ident) = &first_arg {
                if *ident == "null" {
                    return Ok(Some("null".to_string()));
                }
            }
        }

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
        if let Some(result) = ctx.override_syntax(first_arg.clone(), stream_iter, &type_name)? {
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
                                inner_iter.collect::<Vec<_>>(),
                                type_name
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
                    ',' => to_json(stream_iter, &type_name, rti, ctx),
                    // Process the next `TokenTree` and prepend the
                    // punctuation. This enables support for negative
                    // numbers.
                    other => match to_json(stream_iter, &type_name, rti, ctx)? {
                        Some(result) => Ok(Some(format!("{}{}", other, result))),
                        None => Ok(Some(other.to_string())),
                    },
                }
            }
            TokenTree::Ident(ident) => {
                // If type_name == "String", then we are trying to construct
                // either a String or Option<String>. Thus, if ident == null,
                // we may be trying to construct a None object.
                if type_name == "String" && ident != "null" {
                    Ok(Some(ident.to_string().quoted().to_string()))
                } else {
                    Ok(Some(ident.to_string()))
                }
            }
            TokenTree::Literal(literal) => Ok(Some(literal.to_string())),
        }
    } else {
        Ok(None)
    }
}

/// A trait for extending and/or overriding the default test case syntax.
///
/// Note when creating an implementation of this trait that the
/// `[proc_macro2::TokenStream]` considers:
/// * null/true/false to be `Ident`s
/// * strings and positive numeric values (like 1 or 1.1) to be `Literal`s.
/// * negative numeric values to be a `Punct('-')` followed by a `Literal`.
pub trait TestDeserializeContext {
    /// Override the way that `first_arg` is resolved to JSON.
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
    ) -> Result<Option<String>, String>
    where
        I: Iterator<Item = TokenTree>;

    /// Converts `json` back to the extended syntax specified by
    /// [TestDeserializeContext::override_syntax<I>].
    ///
    /// Returns `Some(value)` if `json` has been resolved.
    /// Returns `None` is `json` should be resolved in the default manner.
    fn reverse_syntax_override(&mut self, json: &Value, type_name: &str) -> Option<String>;
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
    ) -> Result<Option<String>, String>
    where
        I: Iterator<Item = TokenTree>,
    {
        Ok(None)
    }

    fn reverse_syntax_override(&mut self, _: &Value, _: &str) -> Option<String> {
        None
    }
}

/* #region helper functions for `to_json` */

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
    let mut prev_elem_end = 0;
    let mut result = Vec::new();
    while let Some((next_elem_begin, next_elem_end)) =
        find_next_type_in_tuple(type_name, prev_elem_end)
    {
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
        prev_elem_end = next_elem_end;
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
            TokenTree::Group(group) if group.delimiter() == Delimiter::Parenthesis => {
                let mut inner_iter = group.stream().into_iter();
                match inner_iter.next() {
                    // the spec is the inner `TokenStream`
                    Some(first_arg) => parse_as_enum_or_struct_inner(
                        first_arg,
                        &mut inner_iter,
                        type_name,
                        rti,
                        ctx,
                    ),
                    None => Ok(None),
                }
            }
            TokenTree::Punct(punct) => {
                // The spec is that all consecutive puncts + the first
                // non-punct symbol count as one argument. This allows for
                // specifying structs with the first argument being something
                // like -1.1.
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
                // The entire enum/struct is specified by the
                // Ident/Literal/Vec,
                // so feed in (the_thing, nothing)
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
    if let Some(result) = ctx.override_syntax(first_arg.clone(), rest_of_stream, type_name)? {
        Ok(Some(result))
    } else if let Some((f_names, f_types)) = rti.struct_dict.get(type_name).map(|r| r.clone()) {
        Ok(Some(to_json_fields(
            type_name,
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
                .unwrap_or_else(String::new);
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
        let fields = to_json_fields(
            &variant_camel_case,
            rest_of_stream,
            f_names,
            f_types,
            rti,
            ctx,
        )?;
        Ok(format!("{{\"{}\":{}}}", variant_camel_case, fields))
    }
}

/// Converts the spec for fields of an enum/struct into deserializable JSON.
///
/// `f_names` contains the names of the fields. If the fields are unnamed,
/// `f_names` is empty.
/// `f_types` contains the types of the fields.
fn to_json_fields<I, C>(
    debug_name: &str,
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
                f_values
                    .pop()
                    .ok_or_else(|| { format!("Cannot use default value for {}", debug_name) })?
            ))
        } else {
            Ok(format!("[{}]", separated(",", f_values.into_iter())))
        }
    }
}

/* #endregion */

pub fn serialize<M, C>(json: &Value, type_name: &str, ctx: &mut C) -> String
where
    C: TestDeserializeContext,
    M: MzReflect,
{
    let mut rti = ReflectedTypeInfo::default();
    M::add_to_reflected_type_info(&mut rti);
    from_json(json, type_name, &rti, ctx)
}

/// Converts serialized JSON to the syntax that [to_json] handles.
///
/// `json` is assumed to have been produced by serializing an object of type
/// `type_name`.
/// `ctx` is responsible for converting serialized JSON to any syntax
/// extensions or overrides.
pub fn from_json<C>(json: &Value, type_name: &str, rti: &ReflectedTypeInfo, ctx: &mut C) -> String
where
    C: TestDeserializeContext,
{
    let (type_name, option_found) = normalize_type_name(type_name);
    if let Some(result) = ctx.reverse_syntax_override(json, &type_name) {
        return result;
    }
    // If type is `Option<T>`, convert the value to "null" if it is null,
    // otherwise, try to convert it to a spec corresponding to an object of
    // type `T`.
    if option_found {
        if let Value::Null = json {
            return "null".to_string();
        }
    }
    if let Some((names, types)) = rti.struct_dict.get(&type_name[..]) {
        if types.is_empty() {
            "".to_string()
        } else {
            format!("({})", from_json_fields(json, names, types, rti, ctx))
        }
    } else if let Some(enum_dict) = rti.enum_dict.get(&type_name[..]) {
        match json {
            // A unit enum in JSON is `"variant"`. In the spec it is `variant`.
            Value::String(s) => unquote(s),
            // An enum with fields is `{"variant": <fields>}` in JSON. In the
            // spec it is `(variant field1 .. fieldn).
            Value::Object(map) => {
                // Each enum instance only belongs to one variant.
                assert_eq!(
                    map.len(),
                    1,
                    "Multivariant instance {:?} found for enum {}",
                    map,
                    type_name
                );
                for (variant, data) in map.iter() {
                    if let Some((names, types)) = enum_dict.get(&variant[..]) {
                        return format!(
                            "({} {})",
                            variant,
                            from_json_fields(data, names, types, rti, ctx)
                        );
                    }
                }
                unreachable!()
            }
            _ => unreachable!("Invalid json {:?} for enum type {}", json, type_name),
        }
    } else {
        match json {
            Value::Array(members) => {
                let result = if type_name.starts_with("Vec<") && type_name.ends_with('>') {
                    // This is a Vec<something>.
                    members
                        .iter()
                        .map(|v| from_json(v, &type_name[4..(type_name.len() - 1)], rti, ctx))
                        .collect::<Vec<_>>()
                } else {
                    // This is a tuple.
                    let mut result = Vec::new();
                    let type_name = &type_name[1..(type_name.len() - 1)];
                    let mut prev_elem_end = 0;
                    let mut members_iter = members.into_iter();
                    while let Some((next_elem_begin, next_elem_end)) =
                        find_next_type_in_tuple(type_name, prev_elem_end)
                    {
                        match members_iter.next() {
                            Some(elem) => result.push(from_json(
                                elem,
                                &type_name[next_elem_begin..next_elem_end],
                                rti,
                                ctx,
                            )),
                            // we have reached the end of the tuple.
                            None => break,
                        }
                        prev_elem_end = next_elem_end;
                    }
                    result
                };
                // The spec for both is `[elem1 .. elemn]`
                format!("[{}]", separated(" ", result))
            }
            Value::Object(map) => {
                unreachable!("Invalid map {:?} found for type {}", map, type_name)
            }
            other => other.to_string(),
        }
    }
}

fn from_json_fields<C>(
    v: &Value,
    f_names: &[&'static str],
    f_types: &[&'static str],
    rti: &ReflectedTypeInfo,
    ctx: &mut C,
) -> String
where
    C: TestDeserializeContext,
{
    match v {
        // Named fields are specified as
        // `{"field1_name": field1, .. "fieldn_name": fieldn}`
        // not necessarily in that order because maps are unordered.
        // Thus, when converting named fields to the test spec, it is necessary
        // to retrieve values from the map in the order given by `f_names`.
        Value::Object(map) if !f_names.is_empty() => {
            let mut fields = Vec::with_capacity(f_types.len());
            for (name, typ) in f_names.iter().zip(f_types.iter()) {
                fields.push(from_json(&map[*name], typ, rti, ctx))
            }
            separated(" ", fields).to_string()
        }
        // Multiple unnamed fields are specified as `[field1 .. fieldn]` in
        // JSON.
        Value::Array(inner) if f_types.len() > 1 => {
            let mut fields = Vec::with_capacity(f_types.len());
            for (v, typ) in inner.iter().zip(f_types.iter()) {
                fields.push(from_json(v, typ, rti, ctx))
            }
            separated(" ", fields).to_string()
        }
        // A single unnamed field is specified as `field` in JSON.
        other => from_json(other, f_types.first().unwrap(), rti, ctx),
    }
}

/* #region Helper functions common to both spec-to-JSON and the JSON-to-spec
transformations. */

fn normalize_type_name(type_name: &str) -> (String, bool) {
    // Normalize the type name by stripping whitespace.
    let mut type_name = &type_name.replace(' ', "")[..];
    let mut option_found = false;
    // Eliminate outer `Box<>` from type names because they are inconsequential
    // when it comes to creating a correctly deserializable JSON string.
    // The presence of an `Option<>` is consequential, but `serde_json` cannot
    // distinguish between `None`, `Some(None)`, `Some(Some(None))`, etc., so
    // we strip out all `Option<>`s and return whether we have seen at least one
    // option.
    loop {
        if type_name.starts_with("Option<") && type_name.ends_with('>') {
            option_found = true;
            type_name = &type_name[7..(type_name.len() - 1)]
        } else if type_name.starts_with("Box<") && type_name.ends_with('>') {
            type_name = &type_name[4..(type_name.len() - 1)]
        } else {
            break;
        }
    }

    (type_name.to_string(), option_found)
}

fn find_next_type_in_tuple(type_name: &str, prev_elem_end: usize) -> Option<(usize, usize)> {
    let current_elem_begin = if prev_elem_end > 0 {
        //skip over the comma
        prev_elem_end + 1
    } else {
        prev_elem_end
    };
    if current_elem_begin >= type_name.len() {
        return None;
    }
    // The elements of the tuple can be a plain type, a nested tuple, or a
    // Box/Vec/Option with the argument being nested tuple.
    // `type1, (type2, type3), Vec<(type4, type5)>`
    // Thus, the type of the next element is whatever comes before the
    // next comma. Unless... a '(' comes from before the next comma, which
    // means it is a nested tuple, and the type of the next element is
    // whatever comes before the comma after the last ')'.
    let mut current_elem_end = type_name[current_elem_begin..]
        .find(',')
        .unwrap_or(type_name.len());
    if let Some(l_paren_pos) = type_name[current_elem_begin..].find('(') {
        if l_paren_pos < current_elem_end {
            if let Some(r_paren_pos) = type_name[current_elem_begin..].rfind(')') {
                current_elem_end = current_elem_begin
                    + r_paren_pos
                    + type_name[(current_elem_begin + r_paren_pos)..]
                        .find(',')
                        .unwrap_or(type_name.len());
            }
        }
    };
    return Some((current_elem_begin, current_elem_end));
}

/* #endregion */
