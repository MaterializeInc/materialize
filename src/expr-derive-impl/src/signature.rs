// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//! Analysis of a `#[sqlfunc]`-annotated function's signature.
//!
//! These helpers read `syn` types and answer what a generated trait impl needs to
//! know about the function it wraps: its associated types once generic parameters
//! are erased, which input positions reject NULL, and the output type a polymorphic
//! signature implies. Nothing here reads the attribute's modifiers or the arity
//! tables, so it stays a leaf of the crate.

use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::Lifetime;
use syn::spanned::Spanned;

/// Checks if the last parameter of the function is a `&RowArena`.
pub(crate) fn last_is_arena(func: &syn::ItemFn) -> bool {
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
pub(crate) fn non_nullable_position_checks(param_types: &[syn::Type]) -> Vec<TokenStream> {
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

/// Convert an identifier to a camel-cased identifier.
pub(crate) fn camel_case(ident: &Ident) -> Ident {
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
pub(crate) fn find_generic_type_params(func: &syn::ItemFn) -> Vec<Ident> {
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
pub(crate) fn is_option_wrapped(ty: &syn::Type) -> bool {
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
pub(crate) fn derive_output_type_for_generics(
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
pub(crate) fn erase_all_generic_params(ty: &syn::Type, generic_names: &[Ident]) -> syn::Type {
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
pub(crate) fn arg_type(arg: &syn::ItemFn, nth: usize) -> Result<syn::Type, syn::Error> {
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
pub(crate) fn patch_lifetimes(ty: &syn::Type) -> syn::Type {
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
pub(crate) fn output_type(arg: &syn::ItemFn) -> Result<&syn::Type, syn::Error> {
    match &arg.sig.output {
        syn::ReturnType::Type(_, ty) => Ok(&*ty),
        syn::ReturnType::Default => Err(syn::Error::new(
            arg.sig.output.span(),
            "Function needs to return a value",
        )),
    }
}
