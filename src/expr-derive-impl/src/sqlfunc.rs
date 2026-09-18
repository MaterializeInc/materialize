// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//! The `#[sqlfunc]` attribute macro's entry point.
//!
//! Parses the attribute, classifies the annotated function's arity, and hands the
//! matching [`crate::shape::Shape`] to [`crate::generate::generate`].

use darling::FromMeta;
use proc_macro2::{Ident, TokenStream};
use quote::quote;

use crate::modifiers::Modifiers;
use crate::shape::Shape;
use crate::signature::last_is_arena;

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
    let generate_tests = modifiers.generates_test();
    let func = syn::parse2::<syn::ItemFn>(item.clone())?;
    let tokens = match determine_arity(&func) {
        Arity::Nullary => Err(darling::Error::custom("Nullary functions not supported")),
        Arity::Unary { arena: false } => unary_func(&func, modifiers, &attr),
        Arity::Unary { arena: true } => Err(darling::Error::custom(
            "Unary functions do not yet support RowArena.",
        )),
        Arity::Binary => binary_func(&func, modifiers),
        Arity::Variadic { has_self } => variadic_func(&func, modifiers, struct_ty, has_self),
    }?;

    let test = (generate_tests && include_test).then(|| generate_test(attr, item, &func.sig.ident));

    Ok(quote! {
        #tokens
        #test
    })
}

/// Renders a token stream as compact, canonical source text.
///
/// The rendering walks the token trees rather than calling
/// `TokenStream::to_string`, whose spacing differs between the compiler and
/// the proc-macro2 fallback and has changed across rustc releases. Idents
/// and literals are rendered verbatim, so whitespace inside a string literal
/// is preserved and distinguishes two bodies. Spacing between tokens follows
/// fixed rules that read like formatted code for signatures and attribute
/// arguments, and is merely deterministic for bodies.
fn render_tokens(tokens: &TokenStream) -> String {
    let mut out = String::new();
    render_into(tokens.clone(), &mut out);
    out
}

fn render_into(tokens: TokenStream, out: &mut String) {
    /// What the previous token was, as far as spacing cares.
    enum Last {
        Start,
        Ident,
        Punct { ch: char, joint: bool },
        Other,
    }
    let mut last = Last::Start;
    for tree in tokens {
        let glue = match &last {
            Last::Start => true,
            Last::Punct { joint: true, .. } => true,
            Last::Punct { ch, .. } if "&.!#<".contains(*ch) => true,
            _ => out.ends_with("::"),
        } || match &tree {
            TokenTree::Punct(p) if ",;:.?>".contains(p.as_char()) => true,
            // Generic parameter lists and macro invocations attach to the
            // preceding ident.
            TokenTree::Punct(p) if "<!".contains(p.as_char()) => matches!(last, Last::Ident),
            // Call and index groups attach to the callee, including a closing
            // generic angle bracket, but not to an arrow's `>`.
            TokenTree::Group(g) => {
                matches!(g.delimiter(), Delimiter::Parenthesis | Delimiter::Bracket)
                    && (matches!(last, Last::Ident)
                        || (matches!(last, Last::Punct { ch: '>', .. })
                            && !out.ends_with("->")
                            && !out.ends_with("=>")))
            }
            _ => false,
        };
        if !glue {
            out.push(' ');
        }
        last = match tree {
            TokenTree::Ident(ident) => {
                out.push_str(&ident.to_string());
                Last::Ident
            }
            TokenTree::Literal(lit) => {
                out.push_str(&lit.to_string());
                Last::Other
            }
            TokenTree::Punct(p) => {
                out.push(p.as_char());
                Last::Punct {
                    ch: p.as_char(),
                    joint: p.spacing() == Spacing::Joint,
                }
            }
            TokenTree::Group(g) => {
                let (open, close) = match g.delimiter() {
                    Delimiter::Parenthesis => ("(", ")"),
                    Delimiter::Bracket => ("[", "]"),
                    Delimiter::Brace => ("{", "}"),
                    Delimiter::None => ("", ""),
                };
                out.push_str(open);
                render_into(g.stream(), out);
                out.push_str(close);
                Last::Other
            }
        };
    }
}

/// FNV-1a, 64 bit. A fingerprint, not a secure hash: it only needs to be
/// stable across builds and sensitive to any change in its input.
fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

/// Emits the source-derived members of the generated `FuncName` impl: the
/// `SQLFUNC` const (declaration text, types-only signature, body
/// fingerprint) and `sqlfunc_input_types`, which yields the column types the
/// function naturally consumes when every parameter type has one. Both exist
/// only under mz-expr's `func-registry` feature, like the trait members they
/// implement.
///
/// Text comes from the token trees via [`render_tokens`], so formatting and
/// comments do not affect it. `input_tys_raw` are the parameter types as
/// written, for the signature. `input_tys` are the erased types the trait
/// impl uses, for the probes.
fn sqlfunc_source(
    attr: &TokenStream,
    func: &syn::ItemFn,
    input_tys_raw: &[syn::Type],
    output_ty_raw: &syn::Type,
    input_tys: &[syn::Type],
) -> TokenStream {
    let attr = render_tokens(attr);
    let attr = if attr.is_empty() {
        String::new()
    } else {
        format!("({attr})")
    };
    let decl = format!(
        "#[sqlfunc{attr}] {}",
        render_tokens(&func.sig.to_token_stream())
    );
    let render_type = |ty: &syn::Type| render_tokens(&ty.to_token_stream());
    let signature = format!(
        "fn({}) -> {}",
        input_tys_raw
            .iter()
            .map(render_type)
            .collect::<Vec<_>>()
            .join(", "),
        render_type(output_ty_raw)
    );
    let body_fingerprint = fnv1a64(render_tokens(&func.block.to_token_stream()).as_bytes());
    let probes: Vec<TokenStream> = input_tys.iter().flat_map(probe_column_types).collect();
    quote! {
        #[cfg(feature = "func-registry")]
        const SQLFUNC: Option<crate::func::SqlFuncSource> = Some(crate::func::SqlFuncSource {
            decl: #decl,
            signature: #signature,
            body_fingerprint: #body_fingerprint,
        });

        #[cfg(feature = "func-registry")]
        fn sqlfunc_input_types() -> Option<Vec<mz_repr::SqlColumnType>> {
            use crate::func::registry::{ProbeColumnType as _, ProbeColumnTypeFallback as _};
            [#(#probes),*].into_iter().collect()
        }
    }
}

/// One probe expression per datum a parameter consumes. `Variadic<T>` stands
/// for two `T` arguments and `OptionalArg<T>` for one present `T`.
fn probe_column_types(ty: &syn::Type) -> Vec<TokenStream> {
    if let Some((wrapper, inner)) = single_generic_arg(ty) {
        match wrapper.as_str() {
            "Variadic" => return vec![probe_column_type(inner), probe_column_type(inner)],
            "OptionalArg" => return vec![probe_column_type(inner)],
            _ => {}
        }
    }
    vec![probe_column_type(ty)]
}

/// An expression of type `Option<SqlColumnType>`: the column type of `ty` if
/// it implements `AsColumnType`, else `None`. Resolved by autoref
/// specialization on `ColumnTypeProbe`, so it needs no trait bound the macro
/// cannot check.
fn probe_column_type(ty: &syn::Type) -> TokenStream {
    let ty = staticize_lifetimes(ty);
    quote! {
        (&crate::func::registry::ColumnTypeProbe::<#ty>(::std::marker::PhantomData)).column_type()
    }
}

/// The last path segment's name and its single type argument, for types
/// shaped like `Wrapper<T>`.
fn single_generic_arg(ty: &syn::Type) -> Option<(String, &syn::Type)> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let mut types = args.args.iter().filter_map(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });
    let inner = types.next()?;
    if types.next().is_some() {
        return None;
    }
    Some((segment.ident.to_string(), inner))
}

/// Replaces every lifetime in `ty` with `'static`, so a type written against
/// the trait impl's `'a` can be named in a function body.
fn staticize_lifetimes(ty: &syn::Type) -> syn::Type {
    let mut ty = ty.clone();
    fn walk(ty: &mut syn::Type) {
        match ty {
            syn::Type::Reference(r) => {
                r.lifetime = Some(Lifetime::new("'static", r.span()));
                walk(&mut r.elem);
            }
            syn::Type::Tuple(t) => t.elems.iter_mut().for_each(walk),
            syn::Type::Path(p) => {
                for segment in &mut p.path.segments {
                    if let syn::PathArguments::AngleBracketed(args) = &mut segment.arguments {
                        for arg in &mut args.args {
                            match arg {
                                syn::GenericArgument::Lifetime(lt) => {
                                    *lt = Lifetime::new("'static", lt.span());
                                }
                                syn::GenericArgument::Type(ty) => walk(ty),
                                _ => {}
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    walk(&mut ty);
    ty
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

/// Arity classification for a function annotated with `#[sqlfunc]`.
enum Arity {
    Nullary,
    Unary {
        /// Whether a trailing `&RowArena` parameter is present, which
        /// `EagerUnaryFunc::call` has no place for.
        arena: bool,
    },
    Binary,
    Variadic {
        has_self: bool,
    },
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
        Arity::Variadic { has_self }
    } else {
        match effective_count {
            0 => Arity::Nullary,
            1 => Arity::Unary { arena },
            2 => Arity::Binary,
            _ => unreachable!(),
        }
    }
}

/// Produce an `EagerUnaryFunc` implementation.
fn unary_func(func: &syn::ItemFn, modifiers: Modifiers) -> darling::Result<TokenStream> {
    crate::generate::generate(Shape::Unary, func, modifiers, None, false)
}

/// Produce an `EagerBinaryFunc` implementation.
fn binary_func(func: &syn::ItemFn, modifiers: Modifiers) -> darling::Result<TokenStream> {
    crate::generate::generate(Shape::Binary, func, modifiers, None, false)
}

/// Produce an `EagerVariadicFunc` implementation.
fn variadic_func(
    func: &syn::ItemFn,
    modifiers: Modifiers,
    struct_ty: Option<syn::Path>,
    has_self: bool,
    attr: &TokenStream,
) -> darling::Result<TokenStream> {
    crate::generate::generate(Shape::Variadic, func, modifiers, struct_ty, has_self)
}

#[cfg(test)]
mod render_tests {
    use super::render_tokens;

    #[mz_ore::test]
    fn signature_reads_like_formatted_code() {
        let sig: proc_macro2::TokenStream = syn::parse_quote! {
            fn f<'a>(mut a: &'a str, b: Option<i32>) -> Result<Cow<'a, str>, E>
        };
        assert_eq!(
            render_tokens(&sig),
            "fn f<'a>(mut a: &'a str, b: Option<i32>) -> Result<Cow<'a, str>, E>"
        );
        let attr: proc_macro2::TokenStream = syn::parse_quote! {
            is_monotone = "(true, true)", sqlname = "+", could_error = false
        };
        assert_eq!(
            render_tokens(&attr),
            "is_monotone = \"(true, true)\", sqlname = \"+\", could_error = false"
        );
    }

    #[mz_ore::test]
    fn literal_contents_are_verbatim() {
        let a: proc_macro2::TokenStream = syn::parse_quote! { format!("({x})") };
        let b: proc_macro2::TokenStream = syn::parse_quote! { format!("( {x})") };
        assert_ne!(render_tokens(&a), render_tokens(&b));
        assert_eq!(render_tokens(&a), "format!(\"({x})\")");
    }

    #[mz_ore::test]
    fn formatting_is_invisible() {
        let a: proc_macro2::TokenStream = "fn f ( a : i32 ) -> i32 { a + 1 }".parse().unwrap();
        let b: proc_macro2::TokenStream = "fn f(a:i32)->i32{a+1}".parse().unwrap();
        assert_eq!(render_tokens(&a), render_tokens(&b));
    }
}
