// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//! Source text the scalar function registry records for each `#[sqlfunc]`.
//!
//! The registry compares a function's declaration, its types-only signature, and a
//! fingerprint of its body across builds, so every string here depends only on the
//! tokens the call site wrote, not on how they were spaced or commented.

use proc_macro2::{Delimiter, Spacing, TokenStream, TokenTree};
use quote::{ToTokens, quote};
use syn::Lifetime;
use syn::spanned::Spanned;

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
pub(crate) fn sqlfunc_source(
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
