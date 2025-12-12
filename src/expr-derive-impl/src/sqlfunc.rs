// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use darling::FromMeta;
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use serde::Serialize;
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

    let (tokens, fn_doc) = match determine_parameters_arena(&func) {
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

    if let Some(doc) = fn_doc {
        write_doc(&doc);
    }

    let test = include_test.then(|| generate_test(attr, item, &func.sig.ident));

    Ok(quote! {
        #tokens
        #test
    })
}

fn write_doc(doc: &FnDoc) {
    use std::env;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::PathBuf;

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // 2. Define the path for the temporary collection file
    // The macro will append data to this file.
    let temp_collection_path = out_dir.join("macro_docs_temp");
    // Create directory if it doesn't exist
    std::fs::create_dir_all(&temp_collection_path).expect("Unable to create output directory");
    let temp_collection_path = temp_collection_path.join(format!("{}.json", doc.unique_name));

    // 3. Open the file in append mode
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&temp_collection_path)
        .expect("Unable to open or create macro docs temp file");

    // 4. Write the documentation to the file as json uing serde_json
    let json_string = serde_json::to_string(doc).expect("Failed to serialize doc to YAML");
    file.write_all(json_string.as_bytes())
        .expect("Unable to write macro doc to temp file");
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
            Ok(quote! { #pat }.to_string())
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

fn documentation_string(func: &syn::ItemFn) -> String {
    let mut doc_lines = Vec::new();

    for attr in &func.attrs {
        if attr.path().is_ident("doc") {
            // 2. Ensure it is a NameValue (e.g., #[doc = "..."])
            //    We ignore #[doc(hidden)] or #[doc(alias = ...)] which are Meta::List
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
fn unary_func(
    func: &syn::ItemFn,
    modifiers: Modifiers,
) -> darling::Result<(TokenStream, Option<FnDoc>)> {
    let fn_name = &func.sig.ident;
    let struct_name = camel_case(&func.sig.ident);
    let input_ty = arg_type(func, 0)?;
    let output_ty = output_type(func)?;

    let doc = compose_fn_doc(
        fn_name.to_string(),
        &func,
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

        #func
    };
    Ok((result, Some(doc)))
}

/// Produce a `EagerBinaryFunc` implementation.
fn binary_func(
    func: &syn::ItemFn,
    modifiers: Modifiers,
    arena: bool,
) -> darling::Result<(TokenStream, Option<FnDoc>)> {
    let fn_name = &func.sig.ident;
    let struct_name = camel_case(&func.sig.ident);
    let input1_ty = arg_type(func, 0)?;
    let input2_ty = arg_type(func, 1)?;
    let output_ty = output_type(func)?;

    let doc = compose_fn_doc(
        fn_name.to_string(),
        &func,
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

        #func

    };

    Ok((result, Some(doc)))
}

#[derive(Serialize)]
struct FnDoc {
    unique_name: String,
    category: String,
    signature: String,
    description: String,
}

fn type_to_sqldoc(ty: &syn::Type) -> String {
    fn remap_ty(ty: &str) -> &str {
        match ty {
            "AclItem" => "aclitem",
            "Array" => "anyarray",
            "Char" => "character",
            "Date" => "date",
            "DateTime" => "timestamptz",
            "Datum" => "any",
            "DatumList" => "anylist",
            "DatumMap" => "anymap",
            "Decimal" => "numeric",
            "Interval" => "interval",
            "Jsonb" | "JsonbRef" => "jsonb",
            "MzAclItem" => "mz_acl_item",
            "NaiveDateTime" => "timestamp",
            "Numeric" => "numeric",
            "Oid" => "oid",
            "PgLegacyChar" => "character",
            "PgLegacyName" => "name",
            "Range" => "anyrange",
            "RegClass" => "regclass",
            "RegProc" => "regprocedure",
            "RegType" => "regtype",
            "String" => "text",
            "str" => "text",
            "Timestamp" => "mz_timestamp",
            "NaiveTime" => "time",
            "Uuid" => "uuid",
            "VarChar" => "varchar",
            "Vec<u8>" => "bytea",
            "[u8]" => "bytea",
            "bool" => "boolean",
            "f32" => "float4",
            "f64" => "float8",
            "i16" => "int2",
            "i32" => "int4",
            "i64" => "int8",
            "i8" => "int2",
            "u16" => "uint2",
            "u32" => "uint4",
            "u64" => "uint8",
            "u8" => "uint2",
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
            } else if seg.ident == "Vec" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                        return format!("[{}]", type_to_sqldoc(inner_ty));
                    }
                }
                panic!("Vec function should return AngleBracketed types");
            } else {
                remap_ty(&seg.ident.to_string()).to_string()
            }
        }
        syn::Type::Reference(type_ref) => type_to_sqldoc(&type_ref.elem),
        syn::Type::Group(group_ref) => type_to_sqldoc(&group_ref.elem),
        syn::Type::Slice(slice_ref) => format!("[{}]", type_to_sqldoc(&slice_ref.elem)),
        _ => panic!("Unsupported type: {ty:?}"),
    }
}

fn compose_fn_doc(
    name: String,
    func: &syn::ItemFn,
    arg_types: &[&syn::Type],
    arg_names: &[&str],
    return_type: syn::Type,
    modifiers: &Modifiers,
) -> FnDoc {
    let sqlname = if let Some(sqlname) = &modifiers.sqlname {
        format!("{}", quote! { #sqlname })
    } else {
        name.clone()
    }
    .replace('"', "");

    let category = modifiers
        .category
        .as_deref()
        .unwrap_or("Uncategorized")
        .to_string();

    let doc = documentation_string(func);

    let return_type = type_to_sqldoc(&return_type);

    let signature = if modifiers.is_infix_op.is_some() {
        let args: Vec<String> = arg_types.iter().copied().map(type_to_sqldoc).collect();
        format!("{} {sqlname} {} -> {return_type}", args[0], args[1],)
    } else {
        let args: Vec<String> = arg_names
            .iter()
            .zip(arg_types.iter().copied().map(type_to_sqldoc))
            .map(|(name, ty)| format!("{name}: {ty}"))
            .collect();
        format!("{}({}) -> {return_type}", sqlname, args.join(", "),)
    };
    FnDoc {
        unique_name: name.to_string(),
        category,
        signature,
        description: doc.to_string(),
    }
}
