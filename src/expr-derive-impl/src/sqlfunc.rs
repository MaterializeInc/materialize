// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use darling::FromMeta;
use proc_macro2::{Delimiter, Ident, Spacing, TokenStream, TokenTree};
use quote::{ToTokens, quote};
use syn::spanned::Spanned;
use syn::{Expr, Lifetime, Lit};

use crate::shape::{Modifier, Shape};

/// Modifiers passed as key-value pairs to the `#[sqlfunc]` macro.
#[derive(Debug, Default, darling::FromMeta)]
pub(crate) struct Modifiers {
    /// An optional expression that evaluates to a boolean indicating whether the function is
    /// monotone with respect to its arguments. Defined for unary and binary functions.
    pub(crate) is_monotone: Option<Expr>,
    /// Optional expression evaluating to a boolean: whether `is_monotone`'s
    /// endpoint-sampling guarantee still holds when an operand may be infinite.
    /// Set `false` for multiplication and division. Applies to binary functions.
    pub(crate) is_infinity_monotone: Option<Expr>,
    /// The SQL name for the function. Applies to all functions.
    sqlname: Option<SqlName>,
    /// Whether the function preserves uniqueness. Applies to unary functions.
    pub(crate) preserves_uniqueness: Option<Expr>,
    /// The inverse of the function, if it exists. Applies to unary functions.
    pub(crate) inverse: Option<Expr>,
    /// The negated function, if it exists. Applies to binary functions.
    pub(crate) negate: Option<Expr>,
    /// Whether the function is an infix operator. Applies to binary functions, and needs to
    /// be specified.
    pub(crate) is_infix_op: Option<Expr>,
    /// The output type of the function, if it cannot be inferred. Applies to all functions.
    output_type: Option<syn::Path>,
    /// The output type of the function as an expression. Applies to binary and variadic functions.
    output_type_expr: Option<Expr>,
    /// Optional expression evaluating to a boolean indicating whether the function could error.
    /// Applies to all functions.
    pub(crate) could_error: Option<Expr>,
    /// Whether the function propagates nulls. Applies to binary and variadic functions.
    pub(crate) propagates_nulls: Option<Expr>,
    /// Whether the function introduces nulls. Applies to all functions.
    pub(crate) introduces_nulls: Option<Expr>,
    /// Whether the function is associative. Applies to variadic functions.
    pub(crate) is_associative: Option<Expr>,
    /// Whether the function is a noop cast. Applies to unary functions.
    pub(crate) is_eliminable_cast: Option<Expr>,
    /// Whether to generate a snapshot test for the function. Defaults to false.
    test: Option<bool>,
}

impl Modifiers {
    /// The method-producing modifiers that are present, in table order.
    ///
    /// Modifiers that do not produce a trait method are excluded, because
    /// `crate::generate` consumes those by name.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (Modifier, &Expr)> + '_ {
        [
            (Modifier::CouldError, self.could_error.as_ref()),
            (Modifier::IntroducesNulls, self.introduces_nulls.as_ref()),
            (Modifier::IsMonotone, self.is_monotone.as_ref()),
            (Modifier::IsInfixOp, self.is_infix_op.as_ref()),
            (Modifier::PropagatesNulls, self.propagates_nulls.as_ref()),
            (Modifier::Inverse, self.inverse.as_ref()),
            (
                Modifier::PreservesUniqueness,
                self.preserves_uniqueness.as_ref(),
            ),
            (Modifier::IsEliminableCast, self.is_eliminable_cast.as_ref()),
            (Modifier::Negate, self.negate.as_ref()),
            (
                Modifier::IsInfinityMonotone,
                self.is_infinity_monotone.as_ref(),
            ),
            (Modifier::IsAssociative, self.is_associative.as_ref()),
        ]
        .into_iter()
        .filter_map(|(modifier, expr)| expr.map(|expr| (modifier, expr)))
    }
}

/// Errors if `mods` carries a method-producing modifier `shape` does not accept.
fn reject_inapplicable(shape: Shape, mods: &Modifiers) -> darling::Result<()> {
    for (modifier, _) in mods.iter() {
        let accepted = shape
            .modifiers()
            .iter()
            .any(|(candidate, _)| *candidate == modifier);
        if !accepted {
            return Err(darling::Error::custom(format!(
                "`{}` is not supported for {} functions",
                modifier.name(),
                shape.label(),
            )));
        }
    }
    Ok(())
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
        Arity::Unary { arena: false } => unary_func(&func, modifiers, &attr),
        Arity::Unary { arena: true } => Err(darling::Error::custom(
            "Unary functions do not yet support RowArena.",
        )),
        Arity::Binary { arena } => binary_func(&func, modifiers, arena, &attr),
        Arity::Variadic { arena, has_self } => {
            variadic_func(&func, modifiers, struct_ty, arena, has_self, &attr)
        }
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

/// Extracts generic type parameters from a function signature.
/// Returns an empty Vec if there are no type parameters.
fn find_generic_type_params(func: &syn::ItemFn) -> Vec<Ident> {
    func.sig
        .generics
        .params
        .iter()
        .filter_map(|p| {
            if let syn::GenericParam::Type(tp) = p {
                Some(tp.ident.clone())
            } else {
                None
            }
        })
        .collect()
}

/// How a generic type parameter `T` appears in a type.
#[derive(Debug, Clone)]
enum GenericUsage {
    /// `T` does not appear in this type.
    Absent,
    /// `T` appears bare (possibly wrapped in `Option` or `Result`).
    Bare,
    /// `T` appears inside a container type (e.g. `DatumList<'a, T>`, `Array<'a, T>`).
    /// The stored `syn::TypePath` is the container with `T` erased to `Datum<'a>`.
    InContainer(syn::TypePath),
}

impl PartialEq for GenericUsage {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (GenericUsage::Absent, GenericUsage::Absent) => true,
            (GenericUsage::Bare, GenericUsage::Bare) => true,
            (GenericUsage::InContainer(a), GenericUsage::InContainer(b)) => {
                container_idents_match(a, b)
            }
            _ => false,
        }
    }
}

impl Eq for GenericUsage {}

/// Compare two container type paths by their ident segments (ignoring lifetimes
/// and generic arguments). Two containers are "same" if their path idents match.
///
/// This is safe because after erasure all container types have the same generic
/// arity (lifetimes + `Datum<'a>`), so ident equality implies structural equality.
fn container_idents_match(a: &syn::TypePath, b: &syn::TypePath) -> bool {
    let a_idents: Vec<_> = a.path.segments.iter().map(|s| &s.ident).collect();
    let b_idents: Vec<_> = b.path.segments.iter().map(|s| &s.ident).collect();
    a_idents == b_idents
}

/// Classifies how a generic type parameter appears in a type.
///
/// Strips `Option<...>`, `Result<..., E>`, and `ExcludeNull<...>` wrappers before
/// inspecting the inner type. Any generic type wrapping `T` that isn't `Option`,
/// `Result`, or `ExcludeNull` is treated as a container. If the container doesn't
/// implement `SqlContainerType`, the generated code won't compile (a clear error).
fn classify_generic_usage(ty: &syn::Type, generic_name: &Ident) -> GenericUsage {
    match ty {
        syn::Type::Path(type_path) => {
            if type_path.path.is_ident(generic_name) {
                return GenericUsage::Bare;
            }
            if let Some(last) = type_path.path.segments.last() {
                let ident_str = last.ident.to_string();
                // Unwrap Option, Result, ExcludeNull wrappers
                if ident_str == "Option" || ident_str == "Result" || ident_str == "ExcludeNull" {
                    if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                        if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                            return classify_generic_usage(inner, generic_name);
                        }
                    }
                }
                // Check if any angle-bracketed arg contains the generic param.
                // If so, treat this type as a container.
                if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                    let has_generic_arg = args.args.iter().any(|arg| {
                        if let syn::GenericArgument::Type(inner) = arg {
                            type_contains_ident(inner, generic_name)
                        } else {
                            false
                        }
                    });
                    if has_generic_arg {
                        // Build the erased container type path (T → Datum<'a>).
                        let erased = erase_generic_param(ty, generic_name);
                        if let syn::Type::Path(erased_path) = erased {
                            return GenericUsage::InContainer(erased_path);
                        }
                    }
                    // Recurse into args for nested containers
                    // (e.g., Option<DatumList<'a, T>> was already handled by
                    // the Option unwrapping above, but handle other nestings)
                    for arg in &args.args {
                        if let syn::GenericArgument::Type(inner) = arg {
                            let inner_usage = classify_generic_usage(inner, generic_name);
                            if inner_usage != GenericUsage::Absent {
                                return inner_usage;
                            }
                        }
                    }
                }
            }
            GenericUsage::Absent
        }
        syn::Type::Reference(r) => classify_generic_usage(&r.elem, generic_name),
        syn::Type::Tuple(t) => {
            // Prefer container usages over bare. For example, `(T, DatumList<'_, T>)`
            // should classify as `InDatumList`, not `Bare`.
            let mut best = GenericUsage::Absent;
            for elem in &t.elems {
                let usage = classify_generic_usage(elem, generic_name);
                match (&best, &usage) {
                    (GenericUsage::Absent, _) => best = usage,
                    (GenericUsage::Bare, u) if *u != GenericUsage::Absent => best = usage.clone(),
                    _ => {
                        if usage != GenericUsage::Absent && usage != best {
                            // Conflicting container usages — cannot resolve.
                            return GenericUsage::Bare;
                        }
                    }
                }
            }
            best
        }
        _ => GenericUsage::Absent,
    }
}

/// Returns whether a type syntactically contains an identifier.
fn type_contains_ident(ty: &syn::Type, ident: &Ident) -> bool {
    match ty {
        syn::Type::Path(type_path) => {
            if type_path.path.is_ident(ident) {
                return true;
            }
            if let Some(last) = type_path.path.segments.last() {
                if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                    return args.args.iter().any(|arg| {
                        if let syn::GenericArgument::Type(inner) = arg {
                            type_contains_ident(inner, ident)
                        } else {
                            false
                        }
                    });
                }
            }
            false
        }
        syn::Type::Reference(r) => type_contains_ident(&r.elem, ident),
        syn::Type::Tuple(t) => t.elems.iter().any(|e| type_contains_ident(e, ident)),
        _ => false,
    }
}

/// Returns whether the outermost wrapper of a type is `Option`.
fn is_option_wrapped(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(last) = type_path.path.segments.last() {
            return last.ident == "Option";
        }
    }
    false
}

/// Derives an `output_type_expr` TokenStream from the structural relationship
/// between input types and the output type, based on where generic parameters appear.
///
/// Finds the first generic parameter that appears in the output type, then looks for
/// the first input parameter containing that generic in a container type to determine
/// the unwrap strategy.
///
/// `is_unary` controls whether the generated expression uses `input_type`
/// (singular, for unary functions) or `input_types[i]` (indexed, for binary/variadic).
///
/// Returns `None` if no generic parameter appears in the output type.
fn derive_output_type_for_generics(
    input_types: &[syn::Type],
    output_ty: &syn::Type,
    generic_names: &[Ident],
    is_unary: bool,
) -> darling::Result<Option<TokenStream>> {
    // Find the first generic param that appears in the output.
    let generic_name = match generic_names
        .iter()
        .find(|gn| classify_generic_usage(output_ty, gn) != GenericUsage::Absent)
    {
        Some(gn) => gn,
        None => return Ok(None),
    };
    derive_output_type_for_generic(input_types, output_ty, generic_name, is_unary)
}

/// Derives an `output_type_expr` for a single generic parameter.
///
/// Uses `SqlContainerType` trait calls instead of matching on specific container
/// type names. The generated code calls `<Container as SqlContainerType>::unwrap_element_type()`
/// and `wrap_element_type()`, letting Rust's type system resolve the correct behavior.
fn derive_output_type_for_generic(
    input_types: &[syn::Type],
    output_ty: &syn::Type,
    generic_name: &Ident,
    is_unary: bool,
) -> darling::Result<Option<TokenStream>> {
    let output_usage = classify_generic_usage(output_ty, generic_name);
    if output_usage == GenericUsage::Absent {
        return Ok(None);
    }

    let nullable = is_option_wrapped(output_ty);

    // Find the first input parameter that has T in a container.
    // Prefer container inputs over bare inputs.
    let mut container_input: Option<(usize, GenericUsage)> = None;
    for (i, ty) in input_types.iter().enumerate() {
        let usage = classify_generic_usage(ty, generic_name);
        match &usage {
            GenericUsage::InContainer(_) => {
                container_input = Some((i, usage));
                break;
            }
            GenericUsage::Bare => {
                // Bare T in input — not a container, keep looking for a container.
                if container_input.is_none() {
                    container_input = Some((i, usage));
                }
            }
            GenericUsage::Absent => {}
        }
    }

    let (pos, source_usage) = container_input.ok_or_else(|| {
        darling::Error::custom(
            "generic parameter T appears in the output type but not in any input type",
        )
    })?;

    // Generate the base expression to access the input type.
    let input_access = if is_unary {
        quote! { input_type }
    } else {
        let pos_lit = syn::Index::from(pos);
        quote! { input_types[#pos_lit] }
    };

    // For multi-input functions, generate soft assertions that all inputs
    // carrying T agree on the SQL element type. This catches bugs in the
    // planner's overload resolution or cast insertion.
    let consistency_checks = if !is_unary {
        let mut checks = Vec::new();
        for (i, ty) in input_types.iter().enumerate() {
            if i == pos {
                continue;
            }
            let usage = classify_generic_usage(ty, generic_name);
            if usage == GenericUsage::Absent {
                continue;
            }
            let primary_elem = element_type_expr(&input_access, &source_usage);
            let i_lit = syn::Index::from(i);
            let other_access = quote! { input_types[#i_lit] };
            let other_elem = element_type_expr(&other_access, &usage);
            let generic_str = generic_name.to_string();
            checks.push(quote! {
                mz_ore::soft_assert_or_log!(
                    #primary_elem.base_eq(#other_elem),
                    "auto-derived sqlfunc output type inference found inconsistent \
                     SQL types for generic {} across inputs: {:?} vs {:?}; \
                     this indicates a bug in polymorphic coercion, builtin \
                     declaration, or sqlfunc inference",
                    #generic_str,
                    #primary_elem,
                    #other_elem,
                );
            });
        }
        quote! { #(#checks)* }
    } else {
        quote! {}
    };

    // Now generate the output_type_expr based on the combination of
    // source container and output usage.
    let expr = match (&output_usage, &source_usage) {
        // Output is bare T, source is a container → unwrap element type via trait.
        (GenericUsage::Bare, GenericUsage::InContainer(in_container)) => {
            let in_c = elide_lifetimes(in_container);
            quote! {
                {
                    #consistency_checks
                    <#in_c as mz_repr::SqlContainerType>::unwrap_element_type(
                        &#input_access.scalar_type
                    ).clone().nullable(#nullable)
                }
            }
        }
        // Output is bare T, source is bare T → forward input type directly.
        (GenericUsage::Bare, GenericUsage::Bare) => {
            quote! {
                {
                    #consistency_checks
                    #input_access.scalar_type.clone().nullable(#nullable)
                }
            }
        }
        // Output is a container, source is a container (same or different) →
        // unwrap from input container, wrap into output container via traits.
        (GenericUsage::InContainer(out_container), GenericUsage::InContainer(in_container)) => {
            let out_c = elide_lifetimes(out_container);
            let in_c = elide_lifetimes(in_container);
            quote! {
                {
                    #consistency_checks
                    <#out_c as mz_repr::SqlContainerType>::wrap_element_type(
                        <#in_c as mz_repr::SqlContainerType>::unwrap_element_type(
                            &#input_access.scalar_type
                        ).clone()
                    ).nullable(#nullable)
                }
            }
        }
        // Other cases — user must provide explicit output_type_expr.
        _ => {
            return Err(darling::Error::custom(format!(
                "cannot auto-derive output_type_expr: output uses T as {:?} but \
                 the first T-containing input uses T as {:?}",
                output_usage, source_usage
            )));
        }
    };

    Ok(Some(expr))
}

/// Generates a token stream that extracts the T-level SQL type from an input
/// access expression, based on how T is used in that input.
fn element_type_expr(input_access: &TokenStream, usage: &GenericUsage) -> TokenStream {
    match usage {
        GenericUsage::Bare => {
            quote! { &#input_access.scalar_type }
        }
        GenericUsage::InContainer(container) => {
            let c = elide_lifetimes(container);
            quote! {
                <#c as mz_repr::SqlContainerType>::unwrap_element_type(
                    &#input_access.scalar_type
                )
            }
        }
        GenericUsage::Absent => unreachable!("element_type_expr called with Absent usage"),
    }
}

/// Replaces all lifetime parameters in a `syn::TypePath` with `'_`.
///
/// Used for container type paths in turbofish position (e.g.
/// `<DatumList<'_, Datum<'_>> as SqlContainerType>::...`).
/// The `output_sql_type` method's `&self` provides an implicit lifetime
/// that the compiler can infer through `'_`.
fn elide_lifetimes(tp: &syn::TypePath) -> syn::TypePath {
    let mut tp = tp.clone();
    for segment in &mut tp.path.segments {
        if let syn::PathArguments::AngleBracketed(args) = &mut segment.arguments {
            for arg in &mut args.args {
                match arg {
                    syn::GenericArgument::Lifetime(lt) => {
                        *lt = Lifetime::new("'_", lt.span());
                    }
                    syn::GenericArgument::Type(ty) => {
                        elide_lifetimes_in_type(ty);
                    }
                    _ => {}
                }
            }
        }
    }
    tp
}

/// Recursively replaces all lifetime parameters in a `syn::Type` with `'_`.
fn elide_lifetimes_in_type(ty: &mut syn::Type) {
    match ty {
        syn::Type::Path(tp) => {
            *tp = elide_lifetimes(tp);
        }
        syn::Type::Reference(r) => {
            if let Some(lt) = &mut r.lifetime {
                *lt = Lifetime::new("'_", lt.span());
            }
            elide_lifetimes_in_type(&mut r.elem);
        }
        syn::Type::Tuple(t) => {
            for elem in &mut t.elems {
                elide_lifetimes_in_type(elem);
            }
        }
        _ => {}
    }
}

/// Replaces occurrences of a generic type parameter with `Datum<'a>` in a type.
///
/// Used to convert types from the user's generic function signature into concrete
/// types for the generated trait impl's associated types, where `T` is not in scope.
fn erase_generic_param(ty: &syn::Type, generic_name: &Ident) -> syn::Type {
    match ty {
        syn::Type::Path(type_path) => {
            if type_path.path.is_ident(generic_name) {
                return syn::parse_quote!(Datum<'a>);
            }
            let mut type_path = type_path.clone();
            for segment in &mut type_path.path.segments {
                if let syn::PathArguments::AngleBracketed(args) = &mut segment.arguments {
                    for arg in &mut args.args {
                        if let syn::GenericArgument::Type(inner) = arg {
                            *inner = erase_generic_param(inner, generic_name);
                        }
                    }
                }
            }
            syn::Type::Path(type_path)
        }
        syn::Type::Reference(r) => {
            let elem = Box::new(erase_generic_param(&r.elem, generic_name));
            syn::Type::Reference(syn::TypeReference { elem, ..r.clone() })
        }
        syn::Type::Tuple(t) => {
            let elems = t
                .elems
                .iter()
                .map(|e| erase_generic_param(e, generic_name))
                .collect();
            syn::Type::Tuple(syn::TypeTuple { elems, ..t.clone() })
        }
        _ => ty.clone(),
    }
}

/// Erases all generic type parameters from a type, replacing each with `Datum<'a>`.
fn erase_all_generic_params(ty: &syn::Type, generic_names: &[Ident]) -> syn::Type {
    let mut ty = ty.clone();
    for gn in generic_names {
        ty = erase_generic_param(&ty, gn);
    }
    ty
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
        syn::FnArg::Receiver(_) => Err(syn::Error::new(
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
fn unary_func(
    func: &syn::ItemFn,
    modifiers: Modifiers,
    attr: &TokenStream,
) -> darling::Result<TokenStream> {
    let fn_name = &func.sig.ident;
    let struct_name = camel_case(&func.sig.ident);
    let input_ty_raw = arg_type(func, 0)?;
    let output_ty_raw = output_type(func)?;
    let generic_params = find_generic_type_params(func);
    // Erase generic type params → Datum<'a> for use in the trait impl's associated types.
    let input_ty = erase_all_generic_params(&input_ty_raw, &generic_params);
    let output_ty = erase_all_generic_params(output_ty_raw, &generic_params);

    reject_inapplicable(Shape::Unary, &modifiers)?;

    let Modifiers {
        is_monotone,
        sqlname,
        preserves_uniqueness,
        inverse,
        output_type,
        mut output_type_expr,
        could_error,
        mut introduces_nulls,
        is_eliminable_cast,
        ..
    } = modifiers;

    // If generic type parameters are present and no explicit output_type_expr,
    // auto-derive one from the structural relationship between input and output types.
    // Use raw (pre-erasure) types so we can see the generic parameters.
    if !generic_params.is_empty() {
        if output_type_expr.is_none() && output_type.is_none() {
            if let Some(derived) = derive_output_type_for_generics(
                std::slice::from_ref(&input_ty_raw),
                output_ty_raw,
                &generic_params,
                true,
            )? {
                output_type_expr = Some(syn::parse2(derived)?);
                if introduces_nulls.is_none() {
                    let nullable = is_option_wrapped(output_ty_raw);
                    introduces_nulls = Some(syn::parse_quote!(#nullable));
                }
            }
        }
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

    let is_eliminable_cast_fn = is_eliminable_cast.map(|is_eliminable_cast| {
        quote! {
            fn is_eliminable_cast(&self) -> bool {
                #is_eliminable_cast
            }
        }
    });

    let source = sqlfunc_source(
        attr,
        func,
        std::slice::from_ref(&input_ty_raw),
        output_ty_raw,
        std::slice::from_ref(&input_ty),
    );

    let result = quote! {
        #[derive(
            Ord, PartialOrd, Clone,
            Debug, Eq, PartialEq, serde::Serialize,
            serde::Deserialize, Hash,
        )]
        #[cfg_attr(any(test, feature = "proptest"), derive(proptest_derive::Arbitrary))]
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
            #is_eliminable_cast_fn
        }

        impl std::fmt::Display for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(#name)
            }
        }

        impl crate::func::FuncName for #struct_name {
            const NAME: &'static str = stringify!(#fn_name);
            #source
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
    attr: &TokenStream,
) -> darling::Result<TokenStream> {
    let fn_name = &func.sig.ident;
    let struct_name = camel_case(&func.sig.ident);
    let input1_ty_raw = arg_type(func, 0)?;
    let input2_ty_raw = arg_type(func, 1)?;
    let output_ty_raw = output_type(func)?;
    let generic_params = find_generic_type_params(func);
    // Erase generic type params → Datum<'a> for use in the trait impl's associated types.
    let input1_ty = erase_all_generic_params(&input1_ty_raw, &generic_params);
    let input2_ty = erase_all_generic_params(&input2_ty_raw, &generic_params);
    let output_ty = erase_all_generic_params(output_ty_raw, &generic_params);

    reject_inapplicable(Shape::Binary, &modifiers)?;

    let Modifiers {
        is_monotone,
        sqlname,
        is_infix_op,
        output_type,
        mut output_type_expr,
        negate,
        could_error,
        propagates_nulls,
        mut introduces_nulls,
        is_infinity_monotone,
        ..
    } = modifiers;

    // Auto-derive output_type_expr from generic parameters, if applicable.
    // Use raw (pre-erasure) types so we can see the generic parameters.
    if !generic_params.is_empty() {
        if output_type_expr.is_none() && output_type.is_none() {
            if let Some(derived) = derive_output_type_for_generics(
                &[input1_ty_raw.clone(), input2_ty_raw.clone()],
                output_ty_raw,
                &generic_params,
                false,
            )? {
                output_type_expr = Some(syn::parse2(derived)?);
                if introduces_nulls.is_none() {
                    let nullable = is_option_wrapped(output_ty_raw);
                    introduces_nulls = Some(syn::parse_quote!(#nullable));
                }
            }
        }
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

    let is_infinity_monotone_fn = is_infinity_monotone.map(|is_infinity_monotone| {
        quote! {
            fn is_infinity_monotone(&self) -> bool {
                #is_infinity_monotone
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

    let source = sqlfunc_source(
        attr,
        func,
        &[input1_ty_raw.clone(), input2_ty_raw.clone()],
        output_ty_raw,
        &[input1_ty.clone(), input2_ty.clone()],
    );

    let result = quote! {
        #[derive(
            Ord, PartialOrd, Clone,
            Debug, Eq, PartialEq, serde::Serialize,
            serde::Deserialize, Hash,
        )]
        #[cfg_attr(any(test, feature = "proptest"), derive(proptest_derive::Arbitrary))]
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
            #is_infinity_monotone_fn
            #negate_fn
            #propagates_nulls_fn
        }

        impl std::fmt::Display for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(#name)
            }
        }

        impl crate::func::FuncName for #struct_name {
            const NAME: &'static str = stringify!(#fn_name);
            #source
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
    attr: &TokenStream,
) -> darling::Result<TokenStream> {
    let fn_name = &func.sig.ident;
    let output_ty_raw = output_type(func)?;
    let generic_params = find_generic_type_params(func);
    let output_ty = erase_all_generic_params(output_ty_raw, &generic_params);
    let struct_name = struct_ty
        .as_ref()
        .and_then(|ty| ty.segments.last())
        .map_or_else(|| camel_case(fn_name), |seg| seg.ident.clone());

    reject_inapplicable(Shape::Variadic, &modifiers)?;

    let Modifiers {
        is_monotone,
        sqlname,
        is_infix_op,
        output_type,
        mut output_type_expr,
        could_error,
        propagates_nulls,
        mut introduces_nulls,
        is_associative,
        ..
    } = modifiers;

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
            syn::FnArg::Receiver(_) => {
                return Err(darling::Error::custom("unexpected self parameter"));
            }
        }
    }

    // Auto-derive output_type_expr from generic parameters, if applicable.
    // Use raw (pre-erasure) types so we can see the generic parameters.
    if !generic_params.is_empty() {
        if output_type_expr.is_none() && output_type.is_none() {
            if let Some(derived) = derive_output_type_for_generics(
                &param_types,
                output_ty_raw,
                &generic_params,
                false,
            )? {
                output_type_expr = Some(syn::parse2(derived)?);
                if introduces_nulls.is_none() {
                    let nullable = is_option_wrapped(output_ty_raw);
                    introduces_nulls = Some(syn::parse_quote!(#nullable));
                }
            }
        }
    }

    let param_types_raw = param_types.clone();
    // Erase generic type params → Datum<'a> in param types for the trait impl's associated types.
    for ty in &mut param_types {
        *ty = erase_all_generic_params(ty, &generic_params);
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

    let source = sqlfunc_source(attr, func, &param_types_raw, output_ty_raw, &param_types);
    let funcname_impl = quote! {
        impl crate::func::FuncName for #struct_name {
            const NAME: &'static str = stringify!(#fn_name);
            #source
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
            #funcname_impl
        }
    } else {
        // Unit struct: generate struct + trait impl + Display + original function.
        quote! {
            #[derive(
                Ord, PartialOrd, Clone,
                Debug, Eq, PartialEq, serde::Serialize,
                serde::Deserialize, Hash,
            )]
            #[cfg_attr(any(test, feature = "proptest"), derive(proptest_derive::Arbitrary))]
            pub struct #struct_name;

            #trait_impl
            #display_impl
            #funcname_impl

            #func
        }
    };

    Ok(result)
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
