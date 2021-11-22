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

/// Macro generating an implementation for the trait MzEnumReflect
#[proc_macro_derive(MzEnumReflect)]
pub fn mzenumreflect_derive(input: TokenStream) -> TokenStream {
    // The intended trait implementation is
    // ```
    // impl MzEnumReflect for #name {
    //    fn mz_enum_reflect() ->
    //    std::collections::HashMap<
    //        &'static str,
    //        (Vec<&'static str>, Vec<&'static str>)
    //    >
    //    {
    //       use std::collections::HashMap;
    //       let mut result = HashMap::new();
    //       // repeat line below for all variants
    //       result.add(variant_name, (<field_names>, <field_types>))
    //       result
    //    }
    // }
    // ```
    let ast: DeriveInput = parse(input).unwrap();

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
          use std::collections::HashMap;
          let mut result = HashMap::new();
          #(#variants)*
          result
        }
    } else {
        unreachable!("Not an enum")
    };

    let name = &ast.ident;
    let gen = quote! {
      impl lowertest::MzEnumReflect for #name {
        fn mz_enum_reflect() ->
            std::collections::HashMap<
                &'static str,
                (Vec<&'static str>, Vec<&'static str>)
            >
        {
           #method_impl
        }
      }
    };
    gen.into()
}

/// Macro generating an implementation for the trait MzStructReflect
#[proc_macro_derive(MzStructReflect)]
pub fn mzstructreflect_derive(input: TokenStream) -> TokenStream {
    // The intended trait implementation is
    // ```
    // impl MzStructReflect for #name {
    //    fn mz_struct_reflect() -> (Vec<&'static str>, Vec<&'static str>)
    //    {
    //       (<field_names>, <field_types>))
    //    }
    // }
    // ```
    let ast: DeriveInput = parse(input).unwrap();
    let method_impl = if let Data::Struct(structdata) = &ast.data {
        let (names, types) = get_field_names_types(&structdata.fields);
        quote! {
            (vec![#(#names),*], vec![#(#types),*])
        }
    } else {
        unreachable!("Not a struct")
    };

    let name = &ast.ident;
    let gen = quote! {
      impl lowertest::MzStructReflect for #name {
        fn mz_struct_reflect() -> (Vec<&'static str>, Vec<&'static str>)
        {
           #method_impl
        }
      }
    };
    gen.into()
}

/// Generates a function that generates `ReflectedTypeInfo`
///
/// Accepts three comma-separated arguments:
/// * first is the name of the function to generate
/// * second is a list of enums that can be looked up in the
/// `ReflectedTypeInfo`
/// * third is a list of enums that can be looked up in the
/// `ReflectedTypeInfo`
/// The latter two arguments are optional
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
                    Some(quote! { enum_dict.insert(#type_name, #typ::mz_enum_reflect()) })
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
                    Some(quote! { struct_dict.insert(#type_name, #typ::mz_struct_reflect()) })
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
            #(#add_enums);* ;
            #(#add_structs);* ;
            ReflectedTypeInfo {
                enum_dict,
                struct_dict
            }
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
