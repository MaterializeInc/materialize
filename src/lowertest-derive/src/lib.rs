// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Macros needed by the `lowertest` crate.
//!
//! TODO: eliminate macros in favor of using `walkabout`?

use proc_macro::{TokenStream, TokenTree};
use proc_macro2::Span;
use quote::{quote, ToTokens};
use syn::{parse, Data, DeriveInput, Fields};

/// Types defined outside of Materialize used to build test objects.
const EXTERNAL_TYPES: &[&str] = &["String", "FixedOffset", "Tz", "NaiveDateTime", "Regex"];
const SUPPORTED_ANGLE_TYPES: &[&str] = &["Vec", "Box", "Option"];

/// Macro generating an implementation for the trait MzReflect
#[proc_macro_derive(MzReflect, attributes(mzreflect))]
pub fn mzreflect_derive(input: TokenStream) -> TokenStream {
    // The intended trait implementation is
    // ```
    // impl MzReflect for #name {
    //    /// Adds the information required to create an object of this type
    //    /// to `enum_dict` if it is an enum and to `struct_dict` if it is a
    //    /// struct.
    //    fn add_to_reflected_type_info(
    //        rti: &mut lowertest::ReflectedTypeInfo
    //    )
    //    {
    //       // if the object is an enum
    //       if rti.enum_dict.contains_key(#name) { return; }
    //       use std::collections::HashMap;
    //       let mut result = HashMap::new();
    //       // repeat line below for all variants
    //       result.insert(variant_name, (<field_names>, <field_types>));
    //       rti.enum_dist.insert(<enum_name>, result);
    //
    //       // if the object is a struct
    //       if rti.struct_dict.contains_key(#name) { return ; }
    //       rti.struct_dict.insert(#name, (<field_names>, <field_types>));
    //
    //       // for all object types, repeat line below for each field type
    //       // that should be recursively added to the reflected type info
    //       <field_type>::add_reflect_type_info(enum_dict, struct_dict);
    //    }
    // }
    // ```
    let ast: DeriveInput = parse(input).unwrap();

    let object_name = &ast.ident;
    let object_name_as_string = object_name.to_string();
    let mut referenced_types = Vec::new();
    let add_object_info = if let Data::Enum(enumdata) = &ast.data {
        let variants = enumdata
            .variants
            .iter()
            .map(|v| {
                let variant_name = v.ident.to_string();
                let (names, types_as_string, mut types_as_syn) = get_fields_names_types(&v.fields);
                referenced_types.append(&mut types_as_syn);
                quote! {
                    result.insert(#variant_name, (vec![#(#names),*], vec![#(#types_as_string),*]));
                }
            })
            .collect::<Vec<_>>();
        quote! {
            if rti.enum_dict.contains_key(#object_name_as_string) { return; }
            use std::collections::HashMap;
            let mut result = HashMap::new();
            #(#variants)*
            rti.enum_dict.insert(#object_name_as_string, result);
        }
    } else if let Data::Struct(structdata) = &ast.data {
        let (names, types_as_string, mut types_as_syn) = get_fields_names_types(&structdata.fields);
        referenced_types.append(&mut types_as_syn);
        quote! {
            if rti.struct_dict.contains_key(#object_name_as_string) { return; }
            rti.struct_dict.insert(#object_name_as_string,
                (vec![#(#names),*], vec![#(#types_as_string),*]));
        }
    } else {
        unreachable!("Not a struct or enum")
    };

    let referenced_types = referenced_types
        .into_iter()
        .flat_map(extract_reflected_type)
        .map(|typ| quote! { #typ::add_to_reflected_type_info(rti); })
        .collect::<Vec<_>>();

    let gen = quote! {
      impl lowertest::MzReflect for #object_name {
        fn add_to_reflected_type_info(
            rti: &mut lowertest::ReflectedTypeInfo
        )
        {
           #add_object_info
           #(#referenced_types)*
        }
      }
    };
    gen.into()
}

/// Generates a function that generates `ReflectedTypeInfo`
///
/// Accepts two comma-separated arguments:
/// * first is the name of the function to generate
/// * second is a list of enums or structs that can be looked up in the
/// `ReflectedTypeInfo`
#[proc_macro]
pub fn gen_reflect_info_func(input: TokenStream) -> TokenStream {
    let mut input_iter = input.into_iter();
    let func_name = if let Some(TokenTree::Ident(ident)) = input_iter.next() {
        syn::Ident::new(&ident.to_string(), Span::call_site())
    } else {
        unreachable!("No function name specified")
    };
    // skip comma
    input_iter.next();
    let enum_dict = input_iter.next();
    // skip comma
    input_iter.next();
    let struct_dict = input_iter.next();
    let add_enums = if let Some(TokenTree::Group(group)) = enum_dict {
        group
            .stream()
            .into_iter()
            .filter_map(|tt| {
                if let TokenTree::Ident(ident) = tt {
                    let type_name = ident.to_string();
                    let typ = syn::Ident::new(&type_name, Span::call_site());
                    Some(quote! { #typ::add_to_reflected_type_info(&mut result) })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    } else {
        vec![]
    };
    let add_structs = if let Some(TokenTree::Group(group)) = struct_dict {
        group
            .stream()
            .into_iter()
            .filter_map(|tt| {
                if let TokenTree::Ident(ident) = tt {
                    let type_name = ident.to_string();
                    let typ = syn::Ident::new(&type_name, Span::call_site());
                    Some(quote! { #typ::add_to_reflected_type_info(&mut result) })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    } else {
        vec![]
    };
    let gen = quote! {
        fn #func_name () -> ReflectedTypeInfo {
            let mut enum_dict = std::collections::HashMap::new();
            let mut struct_dict = std::collections::HashMap::new();
            let mut result =  ReflectedTypeInfo {
                enum_dict,
                struct_dict
            };
            #(#add_enums);* ;
            #(#add_structs);* ;
            result
        }
    };
    gen.into()
}

/* #region Helper methods */

/// Gets the names and the types of the fields of an enum variant or struct.
///
/// The result has three parts:
/// 1. The names of the fields. If the fields are unnamed, this is empty.
/// 2. The types of the fields as strings.
/// 3. The types of the fields as [syn::Type]
///
/// Fields with the attribute `#[mzreflect(ignore)]` are not returned.
fn get_fields_names_types(f: &syn::Fields) -> (Vec<String>, Vec<String>, Vec<&syn::Type>) {
    match f {
        Fields::Named(named_fields) => {
            let (names, types): (Vec<_>, Vec<_>) = named_fields
                .named
                .iter()
                .flat_map(get_field_name_type)
                .unzip();
            let (types_as_string, types_as_syn) = types.into_iter().unzip();
            (names, types_as_string, types_as_syn)
        }
        Fields::Unnamed(unnamed_fields) => {
            let (types_as_string, types_as_syn): (Vec<_>, Vec<_>) = unnamed_fields
                .unnamed
                .iter()
                .flat_map(get_field_name_type)
                .map(|(_, (type_as_string, type_as_syn))| (type_as_string, type_as_syn))
                .unzip();
            (Vec::new(), types_as_string, types_as_syn)
        }
        Fields::Unit => (Vec::new(), Vec::new(), Vec::new()),
    }
}

/// Gets the name and the type of a field of an enum variant or struct.
///
/// The result has three parts:
/// 1. The name of the field. If the field is unnamed, this is empty.
/// 2. The type of the field as a string.
/// 3. The type of the field as [syn::Type].
///
/// Returns None if the field has the attribute `#[mzreflect(ignore)]`.
fn get_field_name_type(f: &syn::Field) -> Option<(String, (String, &syn::Type))> {
    for attr in f.attrs.iter() {
        if let Ok(syn::Meta::List(meta_list)) = attr.parse_meta() {
            if meta_list.path.segments.last().unwrap().ident == "mzreflect" {
                for nested_meta in meta_list.nested.iter() {
                    if let syn::NestedMeta::Meta(syn::Meta::Path(path)) = nested_meta {
                        if path.segments.last().unwrap().ident == "ignore" {
                            return None;
                        }
                    }
                }
            }
        }
    }
    let name = if let Some(name) = f.ident.as_ref() {
        name.to_string()
    } else {
        "".to_string()
    };
    Some((name, (get_type_as_string(&f.ty), &f.ty)))
}

/// Gets the type name from the [`syn::Type`] object
fn get_type_as_string(t: &syn::Type) -> String {
    // convert type back into a token stream and then into a string
    let mut token_stream = proc_macro2::TokenStream::new();
    t.to_tokens(&mut token_stream);
    token_stream.to_string()
}

/// If `t` is a supported type, extracts from `t` types defined in a
/// Materialize package.
///
/// Returns an empty vector if `t` is of an unsupported type.
///
/// Supported types are:
/// A plain path type A -> extracts A
/// Box<A>, Vec<A>, Option<A> -> extracts A
/// Tuple (A, (B, C)) -> extracts A, B, C.
/// Remove A, B, C from expected results if they are primitive types or listed
/// in [EXTERNAL_TYPES].
fn extract_reflected_type(t: &syn::Type) -> Vec<&syn::Type> {
    match t {
        syn::Type::Path(tp) => {
            let last_segment = tp.path.segments.last().unwrap();
            let type_name = last_segment.ident.to_string();
            match &last_segment.arguments {
                syn::PathArguments::None => {
                    if EXTERNAL_TYPES.contains(&&type_name[..])
                        || type_name.starts_with(|c: char| c.is_lowercase())
                    {
                        // Ignore primitive types and types
                        return Vec::new();
                    } else {
                        return vec![t];
                    }
                }
                syn::PathArguments::AngleBracketed(args) => {
                    if SUPPORTED_ANGLE_TYPES.contains(&&type_name[..]) {
                        return args
                            .args
                            .iter()
                            .flat_map(|arg| {
                                if let syn::GenericArgument::Type(typ) = arg {
                                    extract_reflected_type(typ)
                                } else {
                                    Vec::new()
                                }
                            })
                            .collect::<Vec<_>>();
                    }
                }
                _ => {}
            }
        }
        syn::Type::Tuple(tt) => {
            return tt
                .elems
                .iter()
                .flat_map(extract_reflected_type)
                .collect::<Vec<_>>();
        }
        _ => {}
    }
    Vec::new()
}

/* #endregion */
