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
use syn::spanned::Spanned;
use syn::{Expr, Lifetime, Lit};

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
    /// The output type of the function as an expression. Applies to binary and variadic functions.
    output_type_expr: Option<Expr>,
    /// Optional expression evaluating to a boolean indicating whether the function could error.
    /// Applies to all functions.
    could_error: Option<Expr>,
    /// Whether the function propagates nulls. Applies to binary and variadic functions.
    propagates_nulls: Option<Expr>,
    /// Whether the function introduces nulls. Applies to all functions.
    introduces_nulls: Option<Expr>,
    /// Whether the function is associative. Applies to variadic functions.
    is_associative: Option<Expr>,
    /// Whether to generate a snapshot test for the function. Defaults to false.
    test: Option<bool>,
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
    let mut attr_args = darling::ast::NestedMeta::parse_meta_list(attr.clone())?;

    // Check if the first attribute arg is a bare Path (struct name for variadic).
    let struct_ty = match attr_args.first() {
        Some(darling::ast::NestedMeta::Meta(syn::Meta::Path(_))) => {
            let darling::ast::NestedMeta::Meta(syn::Meta::Path(path)) = attr_args.remove(0) else {
                unreachable!()
            };
            Some(path)
        }
        _ => None,
    };

    let modifiers = Modifiers::from_list(&attr_args).unwrap();
    let generate_tests = modifiers.test.unwrap_or(false);
    let func = syn::parse2::<syn::ItemFn>(item.clone())?;

    let tokens = match determine_arity(&func) {
        Arity::Nullary => Err(darling::Error::custom("Nullary functions not supported")),
        Arity::Unary { arena: false } => unary_func(&func, modifiers),
        Arity::Unary { arena: true } => Err(darling::Error::custom(
            "Unary functions do not yet support RowArena.",
        )),
        Arity::Binary { arena } => binary_func(&func, modifiers, arena),
        Arity::Variadic { arena, has_self } => {
            variadic_func(&func, modifiers, struct_ty, arena, has_self)
        }
    }?;

    let test = (generate_tests && include_test).then(|| generate_test(attr, item, &func.sig.ident));

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

/// Checks if the last parameter of the function is a `&RowArena`.
fn last_is_arena(func: &syn::ItemFn) -> bool {
    func.sig.inputs.last().map_or(false, |last| {
        if let syn::FnArg::Typed(pat) = last {
            if let syn::Type::Reference(reference) = &*pat.ty {
                if let syn::Type::Path(path) = &*reference.elem {
                    return path.path.is_ident("RowArena");
                }
            }
        }
        false
    })
}

/// Arity classification for a function annotated with `#[sqlfunc]`.
enum Arity {
    Nullary,
    Unary { arena: bool },
    Binary { arena: bool },
    Variadic { arena: bool, has_self: bool },
}

/// Checks whether a parameter's type is `Variadic<...>` or `OptionalArg<...>`,
/// which indicates the function should be treated as variadic regardless of
/// parameter count.
fn is_variadic_arg(arg: &syn::FnArg) -> bool {
    if let syn::FnArg::Typed(pat) = arg {
        if let syn::Type::Path(path) = &*pat.ty {
            if let Some(segment) = path.path.segments.last() {
                let ident = segment.ident.to_string();
                return ident == "Variadic" || ident == "OptionalArg";
            }
        }
    }
    false
}

/// Determines the arity of a function annotated with `#[sqlfunc]`.
///
/// Accounts for `&self` receivers, trailing `&RowArena` parameters, and
/// parameter types like `Variadic<T>` or `OptionalArg<T>` that indicate
/// variadic dispatch.
fn determine_arity(func: &syn::ItemFn) -> Arity {
    let arena = last_is_arena(func);
    let has_self = matches!(func.sig.inputs.first(), Some(syn::FnArg::Receiver(_)));

    let mut effective_count = func.sig.inputs.len();
    if arena {
        effective_count -= 1;
    }
    if has_self {
        effective_count -= 1;
    }

    // Check if any effective parameter uses a variadic-typed wrapper.
    let start = if has_self { 1 } else { 0 };
    let end = if arena {
        func.sig.inputs.len() - 1
    } else {
        func.sig.inputs.len()
    };
    let has_variadic_param = func
        .sig
        .inputs
        .iter()
        .skip(start)
        .take(end - start)
        .any(is_variadic_arg);

    if has_variadic_param || effective_count >= 3 {
        Arity::Variadic { arena, has_self }
    } else {
        match effective_count {
            0 => Arity::Nullary,
            1 => Arity::Unary { arena },
            2 => Arity::Binary { arena },
            _ => unreachable!(),
        }
    }
}

/// Convert an identifier to a camel-cased identifier.
/// Checks if a parameter type accepts NULL.
///
/// `Option<T>` always accepts NULL. `OptionalArg<T>` delegates to `T`.
/// `Datum` accepts NULL (it passes through raw values including null).
/// Everything else (references, concrete types) rejects NULL.
fn is_nullable_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(last_segment) = type_path.path.segments.last() {
            let ident = &last_segment.ident;
            if ident == "Option" || ident == "Datum" {
                return true;
            }
            if ident == "OptionalArg" {
                // OptionalArg<T> delegates nullability to T.
                if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                        return is_nullable_type(inner_ty);
                    }
                }
                return false;
            }
        }
    }
    false
}

/// Checks if a type is `Variadic<T>`.
fn is_variadic_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(last_segment) = type_path.path.segments.last() {
            return last_segment.ident == "Variadic";
        }
    }
    false
}

/// For a `Variadic<T>` type, checks if `T` accepts NULL.
fn variadic_element_is_nullable(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(last_segment) = type_path.path.segments.last() {
            if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                    return is_nullable_type(inner_ty);
                }
            }
        }
    }
    false
}

/// Generates per-position nullability checks for non-nullable parameters.
///
/// For each parameter that rejects NULL (not `Option`, not `OptionalArg<Option<..>>`),
/// generates a check that the corresponding input position is nullable. For `Variadic<T>`
/// with non-nullable `T`, generates a check over all remaining input positions.
fn non_nullable_position_checks(param_types: &[syn::Type]) -> Vec<TokenStream> {
    let mut checks = Vec::new();
    for (i, ty) in param_types.iter().enumerate() {
        if is_variadic_type(ty) {
            if !variadic_element_is_nullable(ty) {
                checks.push(quote! { || input_types.iter().skip(#i).any(|t| t.nullable) });
            }
        } else if !is_nullable_type(ty) {
            checks.push(quote! { || input_types.get(#i).map_or(false, |t| t.nullable) });
        }
    }
    checks
}

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

/// Recursively patches lifetimes in a type, adding `'a` to references without a lifetime
/// and recursing into generic arguments and tuples.
fn patch_lifetimes(ty: &syn::Type) -> syn::Type {
    match ty {
        syn::Type::Reference(r) => {
            let elem = Box::new(patch_lifetimes(&r.elem));
            if r.lifetime.is_none() {
                syn::Type::Reference(syn::TypeReference {
                    lifetime: Some(Lifetime::new("'a", r.span())),
                    elem,
                    ..r.clone()
                })
            } else {
                syn::Type::Reference(syn::TypeReference { elem, ..r.clone() })
            }
        }
        syn::Type::Tuple(t) => {
            let elems = t.elems.iter().map(patch_lifetimes).collect();
            syn::Type::Tuple(syn::TypeTuple { elems, ..t.clone() })
        }
        syn::Type::Path(p) => {
            let mut p = p.clone();
            for segment in &mut p.path.segments {
                if let syn::PathArguments::AngleBracketed(args) = &mut segment.arguments {
                    for arg in &mut args.args {
                        if let syn::GenericArgument::Type(ty) = arg {
                            *ty = patch_lifetimes(ty);
                        }
                    }
                }
            }
            syn::Type::Path(p)
        }
        _ => ty.clone(),
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

/// Produce a `EagerUnaryFunc` implementation.
fn unary_func(func: &syn::ItemFn, modifiers: Modifiers) -> darling::Result<TokenStream> {
    let fn_name = &func.sig.ident;
    let struct_name = camel_case(&func.sig.ident);
    let input_ty = arg_type(func, 0)?;
    let output_ty = output_type(func)?;
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
        is_associative,
        test: _,
    } = modifiers;

    if is_infix_op.is_some() {
        return Err(darling::Error::unknown_field(
            "is_infix_op not supported for unary functions",
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
    if is_associative.is_some() {
        return Err(darling::Error::unknown_field(
            "is_associative not supported for unary functions",
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

    let (mut output_type, mut introduces_nulls_fn) = if let Some(output_type) = output_type {
        let introduces_nulls_fn = quote! {
            fn introduces_nulls(&self) -> bool {
                <#output_type as ::mz_repr::OutputDatumType<'_, ()>>::nullable()
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

    let could_error_fn = could_error.map(|could_error| {
        quote! {
            fn could_error(&self) -> bool {
                #could_error
            }
        }
    });

    let result = quote! {
        #[derive(
            proptest_derive::Arbitrary, Ord, PartialOrd, Clone,
            Debug, Eq, PartialEq, serde::Serialize,
            serde::Deserialize, Hash, mz_lowertest::MzReflect,
        )]
        pub struct #struct_name;

        impl crate::func::EagerUnaryFunc for #struct_name {
            type Input<'a> = #input_ty;
            type Output<'a> = #output_ty;

            fn call<'a>(&self, a: Self::Input<'a>) -> Self::Output<'a> {
                #fn_name(a)
            }

            fn output_sql_type(
                &self,
                input_type: mz_repr::SqlColumnType
            ) -> mz_repr::SqlColumnType {
                use mz_repr::AsColumnType;
                let output = #output_type;
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
        is_associative,
        test: _,
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
    if is_associative.is_some() {
        return Err(darling::Error::unknown_field(
            "is_associative not supported for binary functions",
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
                <#output_type as ::mz_repr::OutputDatumType<'_, ()>>::nullable()
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

    // Per-position checks: for each non-nullable parameter, check if
    // the corresponding input column is nullable.
    let binary_non_nullable_checks =
        non_nullable_position_checks(&[input1_ty.clone(), input2_ty.clone()]);

    let result = quote! {
        #[derive(
            proptest_derive::Arbitrary, Ord, PartialOrd, Clone,
            Debug, Eq, PartialEq, serde::Serialize,
            serde::Deserialize, Hash, mz_lowertest::MzReflect,
        )]
        pub struct #struct_name;

        impl crate::func::binary::EagerBinaryFunc for #struct_name {
            type Input<'a> = (#input1_ty, #input2_ty);
            type Output<'a> = #output_ty;

            fn call<'a>(
                &self,
                (a, b): Self::Input<'a>,
                temp_storage: &'a mz_repr::RowArena
            ) -> Self::Output<'a> {
                #fn_name(a, b #arena)
            }

            fn output_sql_type(
                &self,
                input_types: &[mz_repr::SqlColumnType],
            ) -> mz_repr::SqlColumnType {
                use mz_repr::AsColumnType;
                let output = #output_type;
                let propagates_nulls =
                    crate::func::binary::EagerBinaryFunc::propagates_nulls(self);
                let nullable = output.nullable;
                // The output is nullable if:
                // 1. The function introduces nulls (output.nullable), or
                // 2. A non-nullable parameter's input is nullable (will reject
                //    NULL at runtime via try_from_iter), or
                // 3. propagates_nulls is true and any input is nullable
                //    (optimizer short-circuits all-NULL inputs)
                let non_nullable_input_is_nullable =
                    false #(#binary_non_nullable_checks)*;
                let inputs_nullable = input_types.iter().any(|it| it.nullable);
                let is_null = nullable
                    || non_nullable_input_is_nullable
                    || (propagates_nulls && inputs_nullable);
                output.nullable(is_null)
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
    Ok(result)
}

/// Produce an `EagerVariadicFunc` implementation.
///
/// Two modes based on whether the function has a `&self` receiver:
/// * `&self` present: struct defined externally, generates method impl + trait impl + Display
/// * No `&self`: generates unit struct + trait impl + Display + preserves original function
fn variadic_func(
    func: &syn::ItemFn,
    modifiers: Modifiers,
    struct_ty: Option<syn::Path>,
    arena: bool,
    has_self: bool,
) -> darling::Result<TokenStream> {
    let fn_name = &func.sig.ident;
    let output_ty = output_type(func)?;
    let struct_name = struct_ty
        .as_ref()
        .and_then(|ty| ty.segments.last())
        .map_or_else(|| camel_case(fn_name), |seg| seg.ident.clone());

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
        is_associative,
        test: _,
    } = modifiers;

    // Reject modifiers that don't apply to variadic functions.
    if preserves_uniqueness.is_some() {
        return Err(darling::Error::unknown_field(
            "preserves_uniqueness not supported for variadic functions",
        ));
    }
    if inverse.is_some() {
        return Err(darling::Error::unknown_field(
            "inverse not supported for variadic functions",
        ));
    }
    if negate.is_some() {
        return Err(darling::Error::unknown_field(
            "negate not supported for variadic functions",
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

    // Collect input parameters (skip &self, skip &RowArena).
    let start = if has_self { 1 } else { 0 };
    let end = if arena {
        func.sig.inputs.len() - 1
    } else {
        func.sig.inputs.len()
    };
    let input_params: Vec<&syn::FnArg> = func
        .sig
        .inputs
        .iter()
        .skip(start)
        .take(end - start)
        .collect();

    if input_params.is_empty() {
        return Err(darling::Error::custom(
            "variadic function must have at least one input parameter",
        ));
    }

    // Extract parameter names and types.
    let mut param_names = Vec::new();
    let mut param_types = Vec::new();
    for param in &input_params {
        match param {
            syn::FnArg::Typed(pat) => {
                if let syn::Pat::Ident(ident) = &*pat.pat {
                    param_names.push(ident.ident.clone());
                } else {
                    return Err(
                        darling::Error::custom("unsupported parameter pattern").with_span(&pat.pat)
                    );
                }
                param_types.push(patch_lifetimes(&pat.ty));
            }
            _ => {
                return Err(darling::Error::custom("unexpected self parameter"));
            }
        }
    }

    // Build input type: single param = bare type, multiple = tuple.
    let input_type: syn::Type = if param_types.len() == 1 {
        param_types[0].clone()
    } else {
        syn::parse_quote! { (#(#param_types),*) }
    };

    // Build destructure pattern for call.
    let destructure = if param_names.len() == 1 {
        let name = &param_names[0];
        quote! { #name }
    } else {
        quote! { (#(#param_names),*) }
    };

    let arena_arg = if arena {
        quote! { , temp_storage }
    } else {
        quote! {}
    };

    let call_expr = if has_self {
        quote! { self.#fn_name(#(#param_names),* #arena_arg) }
    } else {
        quote! { #fn_name(#(#param_names),* #arena_arg) }
    };

    // Build modifier functions.
    let name = sqlname
        .as_ref()
        .map_or_else(|| quote! { stringify!(#fn_name) }, |name| quote! { #name });

    let (mut output_type_code, mut introduces_nulls_fn) = if let Some(output_type) = output_type {
        let introduces_nulls_fn = quote! {
            fn introduces_nulls(&self) -> bool {
                <#output_type as ::mz_repr::OutputDatumType<'_, ()>>::nullable()
            }
        };
        let output_type_code = quote! { <#output_type>::as_column_type() };
        (output_type_code, Some(introduces_nulls_fn))
    } else {
        (quote! { Self::Output::as_column_type() }, None)
    };

    if let Some(output_type_expr) = output_type_expr {
        output_type_code = quote! { #output_type_expr };
    }

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

    let is_monotone_fn = is_monotone.map(|is_monotone| {
        quote! {
            fn is_monotone(&self) -> bool {
                #is_monotone
            }
        }
    });

    let is_associative_fn = is_associative.map(|is_associative| {
        quote! {
            fn is_associative(&self) -> bool {
                #is_associative
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

    // Per-position checks: for each non-nullable parameter, check if
    // the corresponding input column is nullable.
    let non_nullable_checks = non_nullable_position_checks(&param_types);

    let trait_impl = quote! {
        impl crate::func::variadic::EagerVariadicFunc for #struct_name {
            type Input<'a> = #input_type;
            type Output<'a> = #output_ty;

            fn call<'a>(
                &self,
                #destructure: Self::Input<'a>,
                temp_storage: &'a mz_repr::RowArena,
            ) -> Self::Output<'a> {
                #call_expr
            }

            fn output_type(
                &self,
                input_types: &[mz_repr::SqlColumnType],
            ) -> mz_repr::SqlColumnType {
                use mz_repr::AsColumnType;
                let output = #output_type_code;
                let propagates_nulls =
                    crate::func::variadic::EagerVariadicFunc::propagates_nulls(self);
                let nullable = output.nullable;
                // The output is nullable if:
                // 1. The function introduces nulls (output.nullable), or
                // 2. A non-nullable parameter's input is nullable (will reject
                //    NULL at runtime via try_from_iter), or
                // 3. propagates_nulls is true and any input is nullable
                //    (optimizer short-circuits all-NULL inputs)
                let non_nullable_input_is_nullable =
                    false #(#non_nullable_checks)*;
                let inputs_nullable = input_types.iter().any(|it| it.nullable);
                output.nullable(
                    nullable
                    || non_nullable_input_is_nullable
                    || (propagates_nulls && inputs_nullable)
                )
            }

            #could_error_fn
            #introduces_nulls_fn
            #is_infix_op_fn
            #is_monotone_fn
            #is_associative_fn
            #propagates_nulls_fn
        }
    };

    let display_impl = quote! {
        impl std::fmt::Display for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(#name)
            }
        }
    };

    let result = if has_self {
        // External struct: generate method impl + trait impl + Display.
        quote! {
            impl #struct_name {
                #func
            }
            #trait_impl
            #display_impl
        }
    } else {
        // Unit struct: generate struct + trait impl + Display + original function.
        quote! {
            #[derive(
                proptest_derive::Arbitrary, Ord, PartialOrd, Clone,
                Debug, Eq, PartialEq, serde::Serialize,
                serde::Deserialize, Hash, mz_lowertest::MzReflect,
            )]
            pub struct #struct_name;

            #trait_impl
            #display_impl

            #func
        }
    };

    Ok(result)
}
