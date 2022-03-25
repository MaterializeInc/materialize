// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Intermediate representation (IR) for codegen.

use std::collections::{BTreeMap, BTreeSet};
use std::iter;

use anyhow::{bail, Result};
use itertools::Itertools;
use quote::ToTokens;

/// The intermediate representation.
pub struct Ir {
    /// The items in the IR.
    pub items: BTreeMap<String, Item>,
    /// The generic parameters that appear throughout the IR.
    ///
    /// Walkabout assumes that generic parameters are named consistently
    /// throughout the types in the IR. This field maps each generic parameter
    /// to the union of all trait bounds required of that parameter.
    pub generics: BTreeMap<String, BTreeSet<String>>,
}

/// An item in the IR.
#[derive(Debug)]
pub enum Item {
    /// A struct item.
    Struct(Struct),
    /// An enum item.
    Enum(Enum),
    /// An abstract item, introduced via a generic parameter.
    Abstract,
}

impl Item {
    pub fn fields<'a>(&'a self) -> Box<dyn Iterator<Item = &Field> + 'a> {
        match self {
            Item::Struct(s) => Box::new(s.fields.iter()),
            Item::Enum(e) => Box::new(e.variants.iter().flat_map(|v| &v.fields)),
            Item::Abstract => Box::new(iter::empty()),
        }
    }

    pub fn generics(&self) -> &[ItemGeneric] {
        match self {
            Item::Struct(s) => &s.generics,
            Item::Enum(e) => &e.generics,
            Item::Abstract => &[],
        }
    }
}

/// A struct in the IR.
#[derive(Debug)]
pub struct Struct {
    /// The fields of the struct.
    pub fields: Vec<Field>,
    /// The generic parameters on the struct.
    pub generics: Vec<ItemGeneric>,
}

/// An enum in the IRs.
#[derive(Debug)]
pub struct Enum {
    /// The variants of the enum.
    pub variants: Vec<Variant>,
    /// The generic parameters on the enum.
    pub generics: Vec<ItemGeneric>,
}

/// A variant of an [`Enum`].
#[derive(Debug)]
pub struct Variant {
    /// The name of the variant.
    pub name: String,
    /// The fields of the variant.
    pub fields: Vec<Field>,
}

/// A field of a [`Variant`] or [`Struct`].
#[derive(Debug)]
pub struct Field {
    /// The optional name of the field.
    ///
    /// If omitted, the field is referred to by its index in its container.
    pub name: Option<String>,
    /// The type of the field.
    pub ty: Type,
}

/// A generic parameter of an [`Item`].
#[derive(Debug)]
pub struct ItemGeneric {
    /// The name of the generic parameter.
    pub name: String,
    /// The trait bounds on the generic parameter.
    pub bounds: Vec<String>,
}

/// The type of a [`Field`].
#[derive(Debug)]
pub enum Type {
    /// A primitive Rust type.
    ///
    /// Primitive types do not need to be visited.
    Primitive,
    /// Abstract type.
    ///
    /// Abstract types are visited, but their default visit function does
    /// nothing.
    Abstract(String),
    /// An [`Option`] type..
    ///
    /// The value inside the option will need to be visited if the option is
    /// `Some`.
    Option(Box<Type>),
    /// A [`Vec`] type.
    ///
    /// Each value in the vector will need to be visited.
    Vec(Box<Type>),
    /// A [`Box`] type.
    ///
    /// The value inside the box will need to be visited.
    Box(Box<Type>),
    /// A type local to the AST.
    ///
    /// The value will need to be visited by calling the appropriate `Visit`
    /// or `VisitMut` trait method on the value.
    Local(String),
}

/// Analyzes the provided items and produces an IR.
///
/// This is a very, very lightweight semantic analysis phase for Rust code. Our
/// main goal is to determine the type of each field of a struct or enum
/// variant, so we know how to visit it. See [`Type`] for details.
pub(crate) fn analyze(syn_items: &[syn::DeriveInput]) -> Result<Ir> {
    let mut items = BTreeMap::new();
    for syn_item in syn_items {
        let name = syn_item.ident.to_string();
        let generics = analyze_generics(&syn_item.generics)?;
        let item = match &syn_item.data {
            syn::Data::Struct(s) => Item::Struct(Struct {
                fields: analyze_fields(&s.fields)?,
                generics,
            }),
            syn::Data::Enum(e) => {
                let mut variants = vec![];
                for v in &e.variants {
                    variants.push(Variant {
                        name: v.ident.to_string(),
                        fields: analyze_fields(&v.fields)?,
                    });
                }
                Item::Enum(Enum { variants, generics })
            }
            syn::Data::Union(_) => bail!("Unable to analyze union: {}", syn_item.ident),
        };
        for field in item.fields() {
            let mut field_ty = &field.ty;
            while let Type::Box(ty) | Type::Vec(ty) | Type::Option(ty) = field_ty {
                field_ty = ty;
            }
            if let Type::Abstract(name) = field_ty {
                items.insert(name.clone(), Item::Abstract);
            }
        }
        items.insert(name, item);
    }

    let mut generics = BTreeMap::<_, BTreeSet<String>>::new();
    for item in items.values() {
        for ig in item.generics() {
            generics
                .entry(ig.name.clone())
                .or_default()
                .extend(ig.bounds.clone());
        }
    }

    for item in items.values() {
        validate_fields(&items, item.fields())?
    }

    Ok(Ir { items, generics })
}

fn validate_fields<'a, I>(items: &BTreeMap<String, Item>, fields: I) -> Result<()>
where
    I: IntoIterator<Item = &'a Field>,
{
    for f in fields {
        match &f.ty {
            Type::Local(s) if !items.contains_key(s) => {
                bail!(
                    "Unable to analyze non built-in type that is not defined in input: {}",
                    s
                );
            }
            _ => (),
        }
    }
    Ok(())
}

fn analyze_fields(fields: &syn::Fields) -> Result<Vec<Field>> {
    fields
        .iter()
        .map(|f| {
            Ok(Field {
                name: f.ident.as_ref().map(|id| id.to_string()),
                ty: analyze_type(&f.ty)?,
            })
        })
        .collect()
}

fn analyze_generics(generics: &syn::Generics) -> Result<Vec<ItemGeneric>> {
    let mut out = vec![];
    for g in generics.params.iter() {
        match g {
            syn::GenericParam::Type(syn::TypeParam { ident, bounds, .. }) => {
                let name = ident.to_string();
                let bounds = analyze_generic_bounds(bounds)?;
                // Generic parameter names that end in '2' conflict with the
                // folder's name generation.
                if name.ends_with('2') {
                    bail!("Generic parameters whose name ends in '2' conflict with folder's naming scheme: {}", name);
                }
                out.push(ItemGeneric { name, bounds });
            }
            _ => {
                bail!(
                    "Unable to analyze non-type generic parameter: {}",
                    g.to_token_stream()
                )
            }
        }
    }
    Ok(out)
}

fn analyze_generic_bounds<'a, I>(bounds: I) -> Result<Vec<String>>
where
    I: IntoIterator<Item = &'a syn::TypeParamBound>,
{
    let mut out = vec![];
    for b in bounds {
        match b {
            syn::TypeParamBound::Trait(t) if t.path.segments.len() != 1 => {
                bail!(
                    "Unable to analyze trait bound with more than one path segment: {}",
                    b.to_token_stream()
                )
            }
            syn::TypeParamBound::Trait(t) => out.push(t.path.segments[0].ident.to_string()),
            _ => bail!("Unable to analyze non-trait bound: {}", b.to_token_stream()),
        }
    }
    Ok(out)
}

fn analyze_type(ty: &syn::Type) -> Result<Type> {
    match ty {
        syn::Type::Path(syn::TypePath { qself: None, path }) => match path.segments.len() {
            2 => {
                let name = path.segments.iter().map(|s| s.ident.to_string()).join("::");
                Ok(Type::Abstract(name))
            }
            1 => {
                let segment = path.segments.last().unwrap();
                let segment_name = segment.ident.to_string();

                let container = |construct_ty: fn(Box<Type>) -> Type| match &segment.arguments {
                    syn::PathArguments::AngleBracketed(args) if args.args.len() == 1 => {
                        match args.args.last().unwrap() {
                            syn::GenericArgument::Type(ty) => {
                                let inner = Box::new(analyze_type(ty)?);
                                Ok(construct_ty(inner))
                            }
                            _ => bail!("Container type argument is not a basic (i.e., non-lifetime, non-constraint) type argument: {}", ty.into_token_stream()),
                        }
                    }
                    syn::PathArguments::AngleBracketed(_) => bail!(
                        "Container type does not have exactly one type argument: {}",
                        ty.into_token_stream()
                    ),
                    syn::PathArguments::Parenthesized(_) => bail!(
                        "Container type has unexpected parenthesized type arguments: {}",
                        ty.into_token_stream()
                    ),
                    syn::PathArguments::None => bail!(
                        "Container type is missing type argument: {}",
                        ty.into_token_stream()
                    ),
                };

                match &*segment_name {
                    "bool" | "usize" | "u64" | "i64" | "char" | "String" | "PathBuf" => {
                        match segment.arguments {
                            syn::PathArguments::None => Ok(Type::Primitive),
                            _ => bail!(
                                "Primitive type had unexpected arguments: {}",
                                ty.into_token_stream()
                            ),
                        }
                    }
                    "Vec" => container(Type::Vec),
                    "Option" => container(Type::Option),
                    "Box" => container(Type::Box),
                    _ => Ok(Type::Local(segment_name)),
                }
            }
            _ => {
                bail!(
                    "Unable to analyze type path with more than two components: '{}'",
                    path.into_token_stream()
                )
            }
        },
        _ => bail!(
            "Unable to analyze non-struct, non-enum type: {}",
            ty.into_token_stream()
        ),
    }
}
