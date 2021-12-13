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

/// Macro generating an implementation for the trait MzReflect
#[proc_macro_derive(MzReflect)]
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
    let method_impl = if let Data::Enum(enumdata) = &ast.data {
        let variants = enumdata
            .variants
            .iter()
            .map(|v| {
                let variant_name = v.ident.to_string();
                let (names, types) = get_field_names_types(&v.fields);
                quote! {
                    result.insert(#variant_name, (vec![#(#names),*], vec![#(#types),*]));
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
        let (names, types) = get_field_names_types(&structdata.fields);
        quote! {
            if rti.struct_dict.contains_key(#object_name_as_string) { return; }
            rti.struct_dict.insert(#object_name_as_string,
                (vec![#(#names),*], vec![#(#types),*]));
        }
    } else {
        unreachable!("Not a struct or enum")
    };

    let gen = quote! {
      impl lowertest::MzReflect for #object_name {
        fn add_to_reflected_type_info(
            rti: &mut lowertest::ReflectedTypeInfo
        )
        {
           #method_impl
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
fn get_field_names_types(f: &syn::Fields) -> (Vec<String>, Vec<String>) {
    match f {
        Fields::Named(named_fields) => {
            let (names, types): (Vec<_>, Vec<_>) = named_fields
                .named
                .iter()
                .map(|n| {
                    (
                        n.ident.as_ref().unwrap().to_string(),
                        get_type_as_string(&n.ty),
                    )
                })
                .unzip();
            (names, types)
        }
        Fields::Unnamed(unnamed_fields) => {
            let types = unnamed_fields
                .unnamed
                .iter()
                .map(|u| get_type_as_string(&u.ty))
                .collect::<Vec<_>>();
            (Vec::new(), types)
        }
        Fields::Unit => (Vec::new(), Vec::new()),
    }
}

/// Gets the type name from the [`syn::Type`] object
fn get_type_as_string(t: &syn::Type) -> String {
    // convert type back into a token stream and then into a string
    let mut token_stream = proc_macro2::TokenStream::new();
    t.to_tokens(&mut token_stream);
    token_stream.to_string()
}

/* #endregion */
