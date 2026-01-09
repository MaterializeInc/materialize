// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use darling::FromMeta;
use itertools::Itertools;
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::spanned::Spanned;
use syn::{Expr, Lifetime, Lit, Meta};

/// Modifiers passed as key-value pairs to the `#[sqlfunc]` macro.
#[derive(Debug, Default, darling::FromMeta)]
pub(crate) struct Modifiers {
    /// An optional expression that evaluates to a boolean indicating whether the function is
    /// monotone with respect to its arguments. Defined for unary and binary functions.
    is_monotone: Option<Expr>,
    /// The SQL name for the function. Applies to all functions.
    sqlname: Option<SqlName>,
    /// Whether the function preserves uniqueness. Applies to unary functions.
    preserves_uniqueness: Option<Expr>,
    /// The inverse of the function, if it exists. Applies to unary functions.
    inverse: Option<Expr>,
    /// The negated function, if it exists. Applies to binary functions.
    negate: Option<Expr>,
    /// Whether the function is an infix operator. Applies to binary functions, and needs to
    /// be specified.
    is_infix_op: Option<Expr>,
    /// The output type of the function, if it cannot be inferred. Applies to all functions.
    output_type: Option<syn::Path>,
    /// The output type of the function as an expression. Applies to binary functions.
    output_type_expr: Option<Expr>,
    /// Optional expression evaluating to a boolean indicating whether the function could error.
    /// Applies to all functions.
    could_error: Option<Expr>,
    /// Whether the function propagates nulls. Applies to binary functions.
    propagates_nulls: Option<Expr>,
    /// Whether the function introduces nulls. Applies to all functions.
    introduces_nulls: Option<Expr>,
    /// Function category for documentation purposes.
    category: Option<String>,
    /// Signature of the function if different from the derived signature.
    /// Used for documentation purposes.
    signature: Option<String>,
    /// Optional URL to link in the documentation.
    url: Option<String>,
    /// Optional string describing the version the function was added.
    version_added: Option<String>,
    /// Optional boolean expression to indicate the function is unmaterializable.
    unmaterializable: Option<Expr>,
    /// Optional boolean expression to indicate that the functions needs special time zone casts.
    known_time_zone_limitation_cast: Option<Expr>,
    /// Optional boolean expression to indicate that the function is side effecting.
    side_effecting: Option<Expr>,
    /// Optional alias for documentation purposes.
    alias: Option<Expr>,
}

/// A name for the SQL function. It can be either a literal or a macro, thus we
/// can't use `String` or `syn::Expr` directly.
#[derive(Debug)]
enum SqlName {
    /// A literal string.
    Literal(syn::Lit),
    /// A macro expression.
    Macro(syn::ExprMacro),
}

impl quote::ToTokens for SqlName {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let name = match self {
            SqlName::Literal(lit) => quote! { #lit },
            SqlName::Macro(mac) => quote! { #mac },
        };
        tokens.extend(name);
    }
}

impl darling::FromMeta for SqlName {
    fn from_value(value: &Lit) -> darling::Result<Self> {
        Ok(Self::Literal(value.clone()))
    }
    fn from_expr(expr: &Expr) -> darling::Result<Self> {
        match expr {
            Expr::Lit(lit) => Self::from_value(&lit.lit),
            Expr::Macro(mac) => Ok(Self::Macro(mac.clone())),
            // Syn sometimes inserts groups, see `FromMeta::from_expr` for
            // details.
            Expr::Group(mac) => Self::from_expr(&mac.expr),
            _ => Err(darling::Error::unexpected_expr_type(expr)),
        }
    }
}

/// Implementation for the `#[sqlfunc]` macro. The first parameter is the attribute
/// arguments, the second is the function body. The third parameter indicates
/// whether to include the test function in the output.
///
/// The feature `test` must be enabled to include the test function.
pub fn sqlfunc(
    attr: TokenStream,
    item: TokenStream,
    include_test: bool,
) -> darling::Result<TokenStream> {
    let attr_args = darling::ast::NestedMeta::parse_meta_list(attr.clone())?;
    let modifiers = Modifiers::from_list(&attr_args).unwrap();
    let func = syn::parse2::<syn::ItemFn>(item.clone())?;

    let tokens = match determine_parameters_arena(&func) {
        (1, false) => unary_func(&func, modifiers),
        (1, true) => Err(darling::Error::custom(
            "Unary functions do not yet support RowArena.",
        )),
        (2, arena) => binary_func(&func, modifiers, arena),
        (other, _) => Err(darling::Error::custom(format!(
            "Unsupported function: {} parameters",
            other
        ))),
    }?;

    let test = include_test.then(|| generate_test(attr, item, &func.sig.ident));

    Ok(quote! {
        #tokens
        #test
    })
}

#[cfg(any(feature = "test", test))]
fn generate_test(attr: TokenStream, item: TokenStream, name: &Ident) -> TokenStream {
    let attr = attr.to_string();
    let item = item.to_string();
    let test_name = Ident::new(&format!("test_{}", name), name.span());
    let fn_name = name.to_string();

    quote! {
        #[cfg(test)]
        #[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
        #[mz_ore::test]
        fn #test_name() {
            let (output, input) = mz_expr_derive_impl::test_sqlfunc_str(#attr, #item);
            insta::assert_snapshot!(#fn_name, output, &input);
        }
    }
}

#[cfg(not(any(feature = "test", test)))]
fn generate_test(_attr: TokenStream, _item: TokenStream, _name: &Ident) -> TokenStream {
    quote! {}
}

/// Determines the number of parameters to the function. Returns the number of parameters and
/// whether the last parameter is a `RowArena`.
fn determine_parameters_arena(func: &syn::ItemFn) -> (usize, bool) {
    let last_is_arena = func.sig.inputs.last().map_or(false, |last| {
        if let syn::FnArg::Typed(pat) = last {
            if let syn::Type::Reference(reference) = &*pat.ty {
                if let syn::Type::Path(path) = &*reference.elem {
                    return path.path.is_ident("RowArena");
                }
            }
        }
        false
    });
    let parameters = func.sig.inputs.len();
    if last_is_arena {
        (parameters - 1, true)
    } else {
        (parameters, false)
    }
}

/// Convert an identifier to a camel-cased identifier.
fn camel_case(ident: &Ident) -> Ident {
    let mut result = String::new();
    let mut capitalize_next = true;
    for c in ident.to_string().chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }
    Ident::new(&result, ident.span())
}

/// Determines the argument name of the nth argument of the function.
///
/// Panics if the function has fewer than `nth` arguments. Returns an error if
/// the parameter is a `self` receiver.
fn arg_name(arg: &syn::ItemFn, nth: usize) -> Result<String, syn::Error> {
    match &arg.sig.inputs[nth] {
        syn::FnArg::Typed(pat_ty) => {
            let pat = &pat_ty.pat;
            match pat.as_ref() {
                syn::Pat::Ident(ident) => {
                    let ident = &ident.ident;
                    Ok(quote! {#ident}.to_string())
                }
                _ => Err(syn::Error::new(
                    pat.span(),
                    "Unsupported argument name pattern",
                )),
            }
        }
        _ => Err(syn::Error::new(
            arg.sig.inputs[nth].span(),
            "Unsupported argument name",
        )),
    }
}

/// Determines the argument type of the nth argument of the function.
///
/// Adds a lifetime `'a` to the argument type if it is a reference type.
///
/// Panics if the function has fewer than `nth` arguments. Returns an error if
/// the parameter is a `self` receiver.
fn arg_type(arg: &syn::ItemFn, nth: usize) -> Result<syn::Type, syn::Error> {
    match &arg.sig.inputs[nth] {
        syn::FnArg::Typed(pat) => {
            // Patch lifetimes to be 'a if reference
            if let syn::Type::Reference(r) = &*pat.ty {
                if r.lifetime.is_none() {
                    let ty = syn::Type::Reference(syn::TypeReference {
                        lifetime: Some(Lifetime::new("'a", r.span())),
                        ..r.clone()
                    });
                    return Ok(ty);
                }
            }
            Ok((*pat.ty).clone())
        }
        _ => Err(syn::Error::new(
            arg.sig.inputs[nth].span(),
            "Unsupported argument type",
        )),
    }
}

/// Determine the output type for a function. Returns an error if the function
/// does not return a value.
fn output_type(arg: &syn::ItemFn) -> Result<&syn::Type, syn::Error> {
    match &arg.sig.output {
        syn::ReturnType::Type(_, ty) => Ok(&*ty),
        syn::ReturnType::Default => Err(syn::Error::new(
            arg.sig.output.span(),
            "Function needs to return a value",
        )),
    }
}

/// Extract the documentation string from a function.
fn documentation_string(func: &syn::ItemFn) -> String {
    let mut doc_lines = Vec::new();

    for attr in &func.attrs {
        if attr.path().is_ident("doc") {
            // Ensure it is a NameValue (e.g., #[doc = "..."])
            //  We ignore #[doc(hidden)] or #[doc(alias = ...)] which are Meta::List
            if let Meta::NameValue(meta_nv) = &attr.meta {
                if let Expr::Lit(expr_lit) = &meta_nv.value {
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        let trimmed = lit_str.value().trim().to_string();
                        doc_lines.push(trimmed);
                    } else {
                        panic!("Invalid doc string literal: :{:?}", expr_lit.lit);
                    }
                } else {
                    panic!("Invalid doc string literal: {:?}", meta_nv.value);
                }
            }
        }
    }

    // Join lines with a newline to reconstruct the full block
    doc_lines.join("\n")
}

/// Produce a `EagerUnaryFunc` implementation.
fn unary_func(func: &syn::ItemFn, modifiers: Modifiers) -> darling::Result<TokenStream> {
    let fn_name = &func.sig.ident;
    let struct_name = camel_case(&func.sig.ident);
    let input_ty = arg_type(func, 0)?;
    let output_ty = output_type(func)?;

    let func_doc = generate_function_doc(
        fn_name.to_string(),
        func,
        &struct_name,
        &[&input_ty],
        &[&arg_name(func, 0)?],
        output_ty.clone(),
        &modifiers,
    );

    let Modifiers {
        is_monotone,
        sqlname,
        preserves_uniqueness,
        inverse,
        is_infix_op,
        output_type,
        output_type_expr,
        negate,
        could_error,
        propagates_nulls,
        introduces_nulls,
        category: _,
        signature: _,
        url: _,
        version_added: _,
        unmaterializable: _,
        known_time_zone_limitation_cast: _,
        side_effecting: _,
        alias: _,
    } = modifiers;

    if is_infix_op.is_some() {
        return Err(darling::Error::unknown_field(
            "is_infix_op not supported for unary functions",
        ));
    }
    if output_type_expr.is_some() {
        return Err(darling::Error::unknown_field(
            "output_type_expr not supported for unary functions",
        ));
    }
    if negate.is_some() {
        return Err(darling::Error::unknown_field(
            "negate not supported for unary functions",
        ));
    }
    if propagates_nulls.is_some() {
        return Err(darling::Error::unknown_field(
            "propagates_nulls not supported for unary functions",
        ));
    }

    let preserves_uniqueness_fn = preserves_uniqueness.map(|preserves_uniqueness| {
        quote! {
            fn preserves_uniqueness(&self) -> bool {
                #preserves_uniqueness
            }
        }
    });

    let inverse_fn = inverse.as_ref().map(|inverse| {
        quote! {
            fn inverse(&self) -> Option<crate::UnaryFunc> {
                #inverse
            }
        }
    });

    let is_monotone_fn = is_monotone.map(|is_monotone| {
        quote! {
            fn is_monotone(&self) -> bool {
                #is_monotone
            }
        }
    });

    let name = sqlname
        .as_ref()
        .map_or_else(|| quote! { stringify!(#fn_name) }, |name| quote! { #name });

    let (output_type, mut introduces_nulls_fn) = if let Some(output_type) = output_type {
        let introduces_nulls_fn = quote! {
            fn introduces_nulls(&self) -> bool {
                <#output_type as ::mz_repr::DatumType<'_, ()>>::nullable()
            }
        };
        let output_type = quote! { <#output_type> };
        (output_type, Some(introduces_nulls_fn))
    } else {
        (quote! { Self::Output }, None)
    };

    if let Some(introduces_nulls) = introduces_nulls {
        introduces_nulls_fn = Some(quote! {
            fn introduces_nulls(&self) -> bool {
                #introduces_nulls
            }
        });
    }

    let could_error_fn = could_error.map(|could_error| {
        quote! {
            fn could_error(&self) -> bool {
                #could_error
            }
        }
    });

    let result = quote! {
        #[derive(proptest_derive::Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Hash, mz_lowertest::MzReflect)]
        pub struct #struct_name;

        impl<'a> crate::func::EagerUnaryFunc<'a> for #struct_name {
            type Input = #input_ty;
            type Output = #output_ty;

            fn call(&self, a: Self::Input) -> Self::Output {
                #fn_name(a)
            }

            fn output_type(&self, input_type: mz_repr::SqlColumnType) -> mz_repr::SqlColumnType {
                use mz_repr::AsColumnType;
                let output = #output_type::as_column_type();
                let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
                let nullable = output.nullable;
                // The output is nullable if it is nullable by itself or the input is nullable
                // and this function propagates nulls
                output.nullable(nullable || (propagates_nulls && input_type.nullable))
            }

            #could_error_fn
            #introduces_nulls_fn
            #inverse_fn
            #is_monotone_fn
            #preserves_uniqueness_fn
        }

        impl std::fmt::Display for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(#name)
            }
        }

        #func_doc

        #func
    };
    Ok(result)
}

/// Produce a `EagerBinaryFunc` implementation.
fn binary_func(
    func: &syn::ItemFn,
    modifiers: Modifiers,
    arena: bool,
) -> darling::Result<TokenStream> {
    let fn_name = &func.sig.ident;
    let struct_name = camel_case(&func.sig.ident);
    let input1_ty = arg_type(func, 0)?;
    let input2_ty = arg_type(func, 1)?;
    let output_ty = output_type(func)?;

    let func_doc = generate_function_doc(
        fn_name.to_string(),
        func,
        &struct_name,
        &[&input1_ty, &input2_ty],
        &[&arg_name(func, 0)?, &arg_name(func, 1)?],
        output_ty.clone(),
        &modifiers,
    );

    let Modifiers {
        is_monotone,
        sqlname,
        preserves_uniqueness,
        inverse,
        is_infix_op,
        output_type,
        output_type_expr,
        negate,
        could_error,
        propagates_nulls,
        introduces_nulls,
        category: _,
        signature: _,
        url: _,
        version_added: _,
        unmaterializable: _,
        known_time_zone_limitation_cast: _,
        side_effecting: _,
        alias: _,
    } = modifiers;

    if preserves_uniqueness.is_some() {
        return Err(darling::Error::unknown_field(
            "preserves_uniqueness not supported for binary functions",
        ));
    }
    if inverse.is_some() {
        return Err(darling::Error::unknown_field(
            "inverse not supported for binary functions",
        ));
    }
    if output_type.is_some() && output_type_expr.is_some() {
        return Err(darling::Error::unknown_field(
            "output_type and output_type_expr cannot be used together",
        ));
    }
    if output_type_expr.is_some() && introduces_nulls.is_none() {
        return Err(darling::Error::unknown_field(
            "output_type_expr requires introduces_nulls",
        ));
    }

    let negate_fn = negate.map(|negate| {
        quote! {
            fn negate(&self) -> Option<crate::BinaryFunc> {
                #negate
            }
        }
    });

    let is_monotone_fn = is_monotone.map(|is_monotone| {
        quote! {
            fn is_monotone(&self) -> (bool, bool) {
                #is_monotone
            }
        }
    });

    let name = sqlname
        .as_ref()
        .map_or_else(|| quote! { stringify!(#fn_name) }, |name| quote! { #name });

    let (mut output_type, mut introduces_nulls_fn) = if let Some(output_type) = output_type {
        let introduces_nulls_fn = quote! {
            fn introduces_nulls(&self) -> bool {
                <#output_type as ::mz_repr::DatumType<'_, ()>>::nullable()
            }
        };
        let output_type = quote! { <#output_type>::as_column_type() };
        (output_type, Some(introduces_nulls_fn))
    } else {
        (quote! { Self::Output::as_column_type() }, None)
    };

    if let Some(output_type_expr) = output_type_expr {
        output_type = quote! { #output_type_expr };
    }

    if let Some(introduces_nulls) = introduces_nulls {
        introduces_nulls_fn = Some(quote! {
            fn introduces_nulls(&self) -> bool {
                #introduces_nulls
            }
        });
    }

    let arena = if arena {
        quote! { , temp_storage }
    } else {
        quote! {}
    };

    let could_error_fn = could_error.map(|could_error| {
        quote! {
            fn could_error(&self) -> bool {
                #could_error
            }
        }
    });

    let is_infix_op_fn = is_infix_op.map(|is_infix_op| {
        quote! {
            fn is_infix_op(&self) -> bool {
                #is_infix_op
            }
        }
    });

    let propagates_nulls_fn = propagates_nulls.map(|propagates_nulls| {
        quote! {
            fn propagates_nulls(&self) -> bool {
                #propagates_nulls
            }
        }
    });

    let result = quote! {
        #[derive(proptest_derive::Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Hash, mz_lowertest::MzReflect)]
        pub struct #struct_name;

        impl<'a> crate::func::binary::EagerBinaryFunc<'a> for #struct_name {
            type Input1 = #input1_ty;
            type Input2 = #input2_ty;
            type Output = #output_ty;

            fn call(&self, a: Self::Input1, b: Self::Input2, temp_storage: &'a mz_repr::RowArena) -> Self::Output {
                #fn_name(a, b #arena)
            }

            fn output_type(&self, input_type_a: mz_repr::SqlColumnType, input_type_b: mz_repr::SqlColumnType) -> mz_repr::SqlColumnType {
                use mz_repr::AsColumnType;
                let output = #output_type;
                let propagates_nulls = crate::func::binary::EagerBinaryFunc::propagates_nulls(self);
                let nullable = output.nullable;
                // The output is nullable if it is nullable by itself or the input is nullable
                // and this function propagates nulls
                output.nullable(nullable || (propagates_nulls && (input_type_a.nullable || input_type_b.nullable)))
            }

            #could_error_fn
            #introduces_nulls_fn
            #is_infix_op_fn
            #is_monotone_fn
            #negate_fn
            #propagates_nulls_fn
        }

        impl std::fmt::Display for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(#name)
            }
        }

        #func_doc

        #func

    };

    Ok(result)
}

/// Convert a Rust type to its SQL documentation representation.
fn type_to_sqldoc(ty: &syn::Type) -> String {
    /// Handle types that have a vector/slice representation in Rust but
    /// are represented by a specific type in SQL, such as `bytea` for `Vec<u8>`.
    fn bytea_handling(ty: String) -> String {
        match ty.as_ref() {
            "uint1" => "bytea".to_string(),
            _ => format!("{ty}[]"),
        }
    }
    /// Remap Rust type names to SQL type names for documentation.
    fn remap_ty(ty: &str) -> &str {
        match ty {
            "AclItem" => "aclitem",
            "Array" => "anyarray",
            "Char" => "character",
            "Date" => "date",
            "DateTime" => "timestamptz",
            "Datum" => "any",
            "DatumList" => "listany",
            "DatumMap" => "mapany",
            "Decimal" => "numeric",
            "Interval" => "interval",
            "Jsonb" | "JsonbRef" => "jsonb",
            "MzAclItem" => "mz_acl_item",
            "NaiveDateTime" => "timestamp",
            "NaiveTime" => "time",
            "Numeric" => "numeric",
            "Oid" => "oid",
            "PgLegacyChar" => "character",
            "PgLegacyName" => "name",
            "Range" => "rangeany",
            "RegClass" => "regclass",
            "RegProc" => "regprocedure",
            "RegType" => "regtype",
            "Timestamp" => "mz_timestamp",
            "Uuid" => "uuid",
            "VarChar" => "varchar",
            "bool" => "boolean",
            "f32" => "float4",
            "f64" => "float8",
            "i16" => "int2",
            "i32" => "int4",
            "i64" => "int8",
            "i8" => "int2",
            "str" | "String" => "text",
            "u16" => "uint2",
            "u32" => "uint4",
            "u64" => "uint8",
            "u8" => "uint1",
            _ => panic!("Unknown type for sqldoc: {}", ty),
        }
    }

    match ty {
        syn::Type::Path(type_path) => {
            let seg = type_path.path.segments.last().expect("Path with segments");

            if seg.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                        return format!("{}?", type_to_sqldoc(inner_ty));
                    }
                }
                panic!("Option function should return AngleBracketed types");
            } else if seg.ident == "ExcludeNull"
                || seg.ident == "Result"
                || seg.ident == "CheckedTimestamp"
                || seg.ident == "OrderedDecimal"
            {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                        return type_to_sqldoc(inner_ty);
                    }
                }
                panic!("Wrapper types should return AngleBracketed types");
            } else if seg.ident == "ArrayRustType" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                        return format!("{}[]", type_to_sqldoc(inner_ty));
                    }
                }
                panic!("ArrayRustType function should return AngleBracketed types");
            } else if seg.ident == "Cow" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    // Find the first generic argument (Cow starts with a lifetime)
                    for arg in &args.args {
                        if let syn::GenericArgument::Type(inner_ty) = arg {
                            return type_to_sqldoc(inner_ty);
                        }
                    }
                }
                panic!("Cow function should return AngleBracketed types");
            } else if seg.ident == "Vec" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                        return bytea_handling(type_to_sqldoc(inner_ty));
                    }
                }
                panic!("Vec function should return AngleBracketed types");
            } else {
                remap_ty(&seg.ident.to_string()).to_string()
            }
        }
        syn::Type::Reference(type_ref) => type_to_sqldoc(&type_ref.elem),
        syn::Type::Group(group_ref) => type_to_sqldoc(&group_ref.elem),
        syn::Type::Slice(slice_ref) => bytea_handling(type_to_sqldoc(&slice_ref.elem)),
        _ => panic!("Unsupported type: {ty:?}"),
    }
}

/// Generate the function documentation implementation. Adds a `func_doc` associated
/// function to the struct, which returns a `FuncDoc` instance.
fn generate_function_doc(
    name: String,
    func: &syn::ItemFn,
    struct_name: &Ident,
    arg_types: &[&syn::Type],
    arg_names: &[&str],
    return_type: syn::Type,
    modifiers: &Modifiers,
) -> TokenStream {
    let sqlname = if let Some(sqlname) = &modifiers.sqlname {
        format!("{}", quote! { #sqlname })
    } else {
        name.clone()
    }
    .replace('"', "");

    let url = modifiers.url.as_ref().map(|expr| {
        quote! { url: Some(#expr), }
    });
    let version_added = modifiers.version_added.as_ref().map(|expr| {
        quote! { version_added: Some(#expr), }
    });
    let unmaterializable = modifiers.unmaterializable.as_ref().map(|expr| {
        quote! { unmaterializable: #expr, }
    });
    let known_time_zone_limitation_cast =
        modifiers
            .known_time_zone_limitation_cast
            .as_ref()
            .map(|expr| {
                quote! { known_time_zone_limitation_cast: #expr, }
            });
    let side_effecting = modifiers.side_effecting.as_ref().map(|expr| {
        quote! { side_effecting: #expr, }
    });
    let alias = modifiers.alias.as_ref().map(|expr| {
        quote! { alias: Some(stringify!(#expr)), }
    });

    let category = modifiers
        .category
        .as_deref()
        .unwrap_or("Uncategorized")
        .to_string();

    let description = documentation_string(func);

    let return_type = type_to_sqldoc(&return_type);

    let signature = if let Some(signature) = modifiers.signature.as_ref() {
        signature.clone()
    } else if modifiers.is_infix_op.is_some() {
        let args: Vec<String> = arg_types.iter().copied().map(type_to_sqldoc).collect();
        format!("{} {sqlname} {} -> {return_type}", args[0], args[1],)
    } else {
        let args: Vec<String> = arg_names
            .iter()
            .zip_eq(arg_types.iter().copied().map(type_to_sqldoc))
            .map(|(name, ty)| format!("{name} {ty}"))
            .collect();
        format!("{}({}) -> {return_type}", sqlname, args.join(", "),)
    };
    let unique_ident = &func.sig.ident;
    quote! {
        impl #struct_name {
            pub const fn func_doc() -> crate::func::FuncDoc {
                crate::func::FuncDoc {
                    unique_name: stringify!(#unique_ident),
                    category: #category,
                    signature: #signature,
                    description: #description,
                    #url
                    #version_added
                    #unmaterializable
                    #known_time_zone_limitation_cast
                    #side_effecting
                    #alias
                    ..crate::func::FuncDoc::default()
                }
            }
        }
    }
}
