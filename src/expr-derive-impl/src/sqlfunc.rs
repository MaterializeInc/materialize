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
    /// Optional documentation string. Overrides `///` doc comments if present.
    doc: Option<String>,
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

/// Extract the documentation string from a list of attributes.
fn documentation_string(attrs: &[syn::Attribute]) -> String {
    let mut doc_lines = Vec::new();

    for attr in attrs {
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

/// Replace all lifetimes in a type with `'static` so the type can be used in contexts
/// without lifetime parameters, such as `SqlDocName::sql_doc_name()` turbofish calls.
fn staticify_lifetimes(ty: &syn::Type) -> syn::Type {
    match ty {
        syn::Type::Reference(r) => {
            let elem = staticify_lifetimes(&r.elem);
            syn::Type::Reference(syn::TypeReference {
                lifetime: Some(Lifetime::new("'static", r.and_token.span())),
                elem: Box::new(elem),
                ..r.clone()
            })
        }
        syn::Type::Path(p) => {
            let mut p = p.clone();
            for seg in &mut p.path.segments {
                if let syn::PathArguments::AngleBracketed(args) = &mut seg.arguments {
                    for arg in &mut args.args {
                        match arg {
                            syn::GenericArgument::Lifetime(lt) => {
                                *lt = Lifetime::new("'static", lt.apostrophe);
                            }
                            syn::GenericArgument::Type(inner_ty) => {
                                *inner_ty = staticify_lifetimes(inner_ty);
                            }
                            _ => {}
                        }
                    }
                }
            }
            syn::Type::Path(p)
        }
        syn::Type::Group(g) => syn::Type::Group(syn::TypeGroup {
            elem: Box::new(staticify_lifetimes(&g.elem)),
            ..g.clone()
        }),
        syn::Type::Slice(s) => syn::Type::Slice(syn::TypeSlice {
            elem: Box::new(staticify_lifetimes(&s.elem)),
            ..s.clone()
        }),
        syn::Type::Tuple(t) => {
            let elems = t.elems.iter().map(staticify_lifetimes).collect();
            syn::Type::Tuple(syn::TypeTuple { elems, ..t.clone() })
        }
        _ => ty.clone(),
    }
}

/// Generate the `func_doc()` associated function implementation for a struct.
fn generate_func_doc_impl(
    struct_name: &Ident,
    unique_name: &str,
    category: &str,
    signature_expr: TokenStream,
    description: &str,
    url: Option<&String>,
    version_added: Option<&String>,
    unmaterializable: Option<TokenStream>,
    known_time_zone_limitation_cast: Option<TokenStream>,
    side_effecting: Option<TokenStream>,
    alias: Option<TokenStream>,
) -> TokenStream {
    let url_field = url.map(|u| quote! { url: Some(#u), });
    let va_field = version_added.map(|v| quote! { version_added: Some(#v), });
    let unmat_field = unmaterializable.map(|expr| quote! { unmaterializable: #expr, });
    let tz_field = known_time_zone_limitation_cast
        .map(|expr| quote! { known_time_zone_limitation_cast: #expr, });
    let se_field = side_effecting.map(|expr| quote! { side_effecting: #expr, });
    let alias_field = alias.map(|expr| quote! { alias: Some(#expr), });

    quote! {
        impl #struct_name {
            pub fn func_doc() -> crate::func::FuncDoc {
                crate::func::FuncDoc {
                    unique_name: #unique_name,
                    category: #category,
                    signature: #signature_expr,
                    description: #description,
                    #url_field
                    #va_field
                    #unmat_field
                    #tz_field
                    #se_field
                    #alias_field
                    ..crate::func::FuncDoc::default()
                }
            }
        }
    }
}

/// Produce a `EagerUnaryFunc` implementation.
fn unary_func(func: &syn::ItemFn, modifiers: Modifiers) -> darling::Result<TokenStream> {
    let fn_name = &func.sig.ident;
    let struct_name = camel_case(&func.sig.ident);
    let input_ty = arg_type(func, 0)?;
    let output_ty = output_type(func)?;
    let arg0_name = arg_name(func, 0)?;

    // Build doc-related tokens before destructuring modifiers.
    let doc_attrs: Vec<_> = func
        .attrs
        .iter()
        .filter(|a| a.path().is_ident("doc"))
        .collect();
    let description = modifiers
        .doc
        .clone()
        .unwrap_or_else(|| documentation_string(&func.attrs));
    let sqlname_str = modifiers
        .sqlname
        .as_ref()
        .map(|s| format!("{}", quote! { #s }).replace('"', ""))
        .unwrap_or_else(|| fn_name.to_string());
    let display_name = modifiers
        .sqlname
        .as_ref()
        .map_or_else(|| quote! { stringify!(#fn_name) }, |name| quote! { #name });
    let unique_name = fn_name.to_string();
    let category = modifiers
        .category
        .as_deref()
        .unwrap_or("Uncategorized")
        .to_string();

    let input_static = staticify_lifetimes(&input_ty);
    let output_static = staticify_lifetimes(output_ty);

    let signature_expr = if let Some(ref sig) = modifiers.signature {
        quote! { #sig.to_string() }
    } else {
        quote! {
            format!("{}({} {}) -> {}",
                #sqlname_str,
                #arg0_name,
                <#input_static as ::mz_repr::SqlDocName>::sql_doc_name(),
                <#output_static as ::mz_repr::SqlDocName>::sql_doc_name(),
            )
        }
    };

    let func_doc_impl = generate_func_doc_impl(
        &struct_name,
        &unique_name,
        &category,
        signature_expr,
        &description,
        modifiers.url.as_ref(),
        modifiers.version_added.as_ref(),
        modifiers.unmaterializable.as_ref().map(|e| quote! { #e }),
        modifiers
            .known_time_zone_limitation_cast
            .as_ref()
            .map(|e| quote! { #e }),
        modifiers.side_effecting.as_ref().map(|e| quote! { #e }),
        modifiers.alias.as_ref().map(|e| {
            let s = quote!(#e).to_string();
            quote! { #s }
        }),
    );

    let Modifiers {
        is_monotone,
        sqlname: _,
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
        category: _,
        signature: _,
        url: _,
        version_added: _,
        unmaterializable: _,
        known_time_zone_limitation_cast: _,
        side_effecting: _,
        alias: _,
        doc: _,
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
        #(#doc_attrs)*
        #[derive(
            proptest_derive::Arbitrary, Ord, PartialOrd, Clone,
            Debug, Eq, PartialEq, serde::Serialize,
            serde::Deserialize, Hash, mz_lowertest::MzReflect,
        )]
        pub struct #struct_name;

        #func_doc_impl

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
                f.write_str(#display_name)
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
    let arg0_name = arg_name(func, 0)?;
    let arg1_name = arg_name(func, 1)?;

    // Build doc-related tokens before destructuring modifiers.
    let doc_attrs: Vec<_> = func
        .attrs
        .iter()
        .filter(|a| a.path().is_ident("doc"))
        .collect();
    let description = modifiers
        .doc
        .clone()
        .unwrap_or_else(|| documentation_string(&func.attrs));
    let sqlname_str = modifiers
        .sqlname
        .as_ref()
        .map(|s| format!("{}", quote! { #s }).replace('"', ""))
        .unwrap_or_else(|| fn_name.to_string());
    let display_name = modifiers
        .sqlname
        .as_ref()
        .map_or_else(|| quote! { stringify!(#fn_name) }, |name| quote! { #name });
    let unique_name = fn_name.to_string();
    let category = modifiers
        .category
        .as_deref()
        .unwrap_or("Uncategorized")
        .to_string();

    let input1_static = staticify_lifetimes(&input1_ty);
    let input2_static = staticify_lifetimes(&input2_ty);
    let output_static = staticify_lifetimes(output_ty);

    let is_infix = modifiers.is_infix_op.is_some();
    let signature_expr = if let Some(ref sig) = modifiers.signature {
        quote! { #sig.to_string() }
    } else if is_infix {
        quote! {
            format!("{} {} {} -> {}",
                <#input1_static as ::mz_repr::SqlDocName>::sql_doc_name(),
                #sqlname_str,
                <#input2_static as ::mz_repr::SqlDocName>::sql_doc_name(),
                <#output_static as ::mz_repr::SqlDocName>::sql_doc_name(),
            )
        }
    } else {
        quote! {
            format!("{}({} {}, {} {}) -> {}",
                #sqlname_str,
                #arg0_name,
                <#input1_static as ::mz_repr::SqlDocName>::sql_doc_name(),
                #arg1_name,
                <#input2_static as ::mz_repr::SqlDocName>::sql_doc_name(),
                <#output_static as ::mz_repr::SqlDocName>::sql_doc_name(),
            )
        }
    };

    let func_doc_impl = generate_func_doc_impl(
        &struct_name,
        &unique_name,
        &category,
        signature_expr,
        &description,
        modifiers.url.as_ref(),
        modifiers.version_added.as_ref(),
        modifiers.unmaterializable.as_ref().map(|e| quote! { #e }),
        modifiers
            .known_time_zone_limitation_cast
            .as_ref()
            .map(|e| quote! { #e }),
        modifiers.side_effecting.as_ref().map(|e| quote! { #e }),
        modifiers.alias.as_ref().map(|e| {
            let s = quote!(#e).to_string();
            quote! { #s }
        }),
    );

    let Modifiers {
        is_monotone,
        sqlname: _,
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
        category: _,
        signature: _,
        url: _,
        version_added: _,
        unmaterializable: _,
        known_time_zone_limitation_cast: _,
        side_effecting: _,
        alias: _,
        doc: _,
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
        #(#doc_attrs)*
        #[derive(
            proptest_derive::Arbitrary, Ord, PartialOrd, Clone,
            Debug, Eq, PartialEq, serde::Serialize,
            serde::Deserialize, Hash, mz_lowertest::MzReflect,
        )]
        pub struct #struct_name;

        #func_doc_impl

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
                f.write_str(#display_name)
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

    // Build doc-related tokens before destructuring modifiers.
    let doc_attrs: Vec<_> = func
        .attrs
        .iter()
        .filter(|a| a.path().is_ident("doc"))
        .collect();
    let description = modifiers
        .doc
        .clone()
        .unwrap_or_else(|| documentation_string(&func.attrs));
    let sqlname_str = modifiers
        .sqlname
        .as_ref()
        .map(|s| format!("{}", quote! { #s }).replace('"', ""))
        .unwrap_or_else(|| fn_name.to_string());
    let display_name = modifiers
        .sqlname
        .as_ref()
        .map_or_else(|| quote! { stringify!(#fn_name) }, |name| quote! { #name });
    let unique_name = fn_name.to_string();
    let category = modifiers
        .category
        .as_deref()
        .unwrap_or("Uncategorized")
        .to_string();
    let explicit_signature = modifiers.signature.clone();
    let doc_url = modifiers.url.clone();
    let doc_version_added = modifiers.version_added.clone();
    let doc_unmaterializable = modifiers.unmaterializable.map(|e| quote! { #e });
    let doc_known_tz = modifiers
        .known_time_zone_limitation_cast
        .map(|e| quote! { #e });
    let doc_side_effecting = modifiers.side_effecting.map(|e| quote! { #e });
    let doc_alias = modifiers.alias.as_ref().map(|e| {
        let s = quote!(#e).to_string();
        quote! { #s }
    });

    let Modifiers {
        is_monotone,
        sqlname: _,
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
        category: _,
        signature: _,
        url: _,
        version_added: _,
        unmaterializable: _,
        known_time_zone_limitation_cast: _,
        side_effecting: _,
        alias: _,
        doc: _,
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

    // Build doc signature from parameter types.
    let output_static = staticify_lifetimes(output_ty);
    let param_statics: Vec<_> = param_types.iter().map(staticify_lifetimes).collect();
    let param_name_strs: Vec<String> = param_names.iter().map(|n| n.to_string()).collect();
    let signature_expr = if let Some(ref sig) = explicit_signature {
        quote! { #sig.to_string() }
    } else {
        #[allow(clippy::disallowed_methods)] // compile-time zip, lengths always match
        let param_parts: Vec<_> = param_name_strs
            .iter()
            .zip(param_statics.iter())
            .map(|(name, ty)| {
                quote! {
                    <#ty as ::mz_repr::SqlDocName>::fmt_doc_param(#name)
                }
            })
            .collect();
        quote! {
            {
                let params: Vec<(String, bool)> = vec![#(#param_parts),*];
                let mut sig = String::new();
                for (formatted, is_optional) in &params {
                    if *is_optional {
                        sig.push_str(&format!(" [, {}]", formatted));
                    } else {
                        if !sig.is_empty() { sig.push_str(", "); }
                        sig.push_str(formatted);
                    }
                }
                format!("{}({}) -> {}",
                    #sqlname_str,
                    sig,
                    <#output_static as ::mz_repr::SqlDocName>::sql_doc_name(),
                )
            }
        }
    };

    let func_doc_impl = generate_func_doc_impl(
        &struct_name,
        &unique_name,
        &category,
        signature_expr,
        &description,
        doc_url.as_ref(),
        doc_version_added.as_ref(),
        doc_unmaterializable,
        doc_known_tz,
        doc_side_effecting,
        doc_alias,
    );

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
                f.write_str(#display_name)
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
            #func_doc_impl
        }
    } else {
        // Unit struct: generate struct + trait impl + Display + original function.
        quote! {
            #(#doc_attrs)*
            #[derive(
                proptest_derive::Arbitrary, Ord, PartialOrd, Clone,
                Debug, Eq, PartialEq, serde::Serialize,
                serde::Deserialize, Hash, mz_lowertest::MzReflect,
            )]
            pub struct #struct_name;

            #trait_impl
            #display_impl
            #func_doc_impl

            #func
        }
    };

    Ok(result)
}

/// Convert a CamelCase identifier to snake_case.
fn to_snake_case(name: &str) -> String {
    let mut result = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch);
        }
    }
    result
}

/// Modifiers parsed from `#[sqldoc(...)]` attributes on structs.
#[derive(Debug, Default, darling::FromMeta)]
pub(crate) struct DocModifiers {
    /// A unique name for the function, used internally. Defaults to the snake_case of the struct name.
    unique_name: Option<String>,
    /// The category of the function (e.g. "Cast", "String", "Timestamp").
    category: Option<String>,
    /// The signature of the function for documentation.
    signature: Option<String>,
    /// Optional URL for documentation.
    url: Option<String>,
    /// Optional version when the function was added.
    version_added: Option<String>,
    /// Whether the function is unmaterializable.
    unmaterializable: Option<bool>,
    /// Whether the function has known time zone cast limitations.
    known_time_zone_limitation_cast: Option<bool>,
    /// Whether the function has side effects.
    side_effecting: Option<bool>,
    /// Optional alias name for the function.
    alias: Option<String>,
}

/// Implementation for the `#[sqldoc]` attribute macro. Generates a `func_doc()` associated
/// function on a struct, extracting documentation from doc comments and explicit attributes.
pub fn sqldoc(attr: TokenStream, item: TokenStream) -> darling::Result<TokenStream> {
    let item_struct = syn::parse2::<syn::ItemStruct>(item)?;
    let attr_args = darling::ast::NestedMeta::parse_meta_list(attr)?;
    let modifiers = DocModifiers::from_list(&attr_args)?;

    let struct_name = &item_struct.ident;
    let description = documentation_string(&item_struct.attrs);

    let unique_name = modifiers
        .unique_name
        .unwrap_or_else(|| to_snake_case(&struct_name.to_string()));
    let category = modifiers
        .category
        .as_deref()
        .unwrap_or("Uncategorized")
        .to_string();
    let signature = modifiers.signature.as_deref().unwrap_or("").to_string();

    let func_doc_impl = generate_func_doc_impl(
        struct_name,
        &unique_name,
        &category,
        quote! { #signature.to_string() },
        &description,
        modifiers.url.as_ref(),
        modifiers.version_added.as_ref(),
        modifiers.unmaterializable.map(|b| quote! { #b }),
        modifiers
            .known_time_zone_limitation_cast
            .map(|b| quote! { #b }),
        modifiers.side_effecting.map(|b| quote! { #b }),
        modifiers.alias.as_ref().map(|a| quote! { #a }),
    );

    Ok(quote! {
        #item_struct
        #func_doc_impl
    })
}
