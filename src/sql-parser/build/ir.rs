// Copyright Materialize, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Intermediate representation (IR).

use std::collections::BTreeMap;

use anyhow::{bail, Result};

/// The intermediate representation.
pub type Ir = BTreeMap<String, Item>;

/// An item in the IR.
#[derive(Debug)]
pub enum Item {
    /// A struct item.
    Struct(Struct),
    /// An enum item.
    Enum(Enum),
}

/// A struct in the IR.
#[derive(Debug)]
pub struct Struct {
    /// The fields of the struct.
    pub fields: Vec<Field>,
}

/// An enum in the IRs.
#[derive(Debug)]
pub struct Enum {
    /// The variants of the enum.
    pub variants: Vec<Variant>,
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

/// The type of a [`Field`].
#[derive(Debug)]
pub enum Type {
    /// A primitive Rust type.
    ///
    /// Primitive types do not need to be visited.
    Primitive,
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
///
/// Types whose names are listed in `ignored_types` are not included in the IR.
pub fn analyze(items: &[syn::DeriveInput], ignored_types: &[&str]) -> Result<Ir> {
    let ir = items
        .iter()
        .filter(|item| !ignored_types.contains(&&*item.ident.to_string()))
        .map(|item| {
            let name = item.ident.to_string();
            let item = match &item.data {
                syn::Data::Struct(s) => Item::Struct(Struct {
                    fields: analyze_fields(&s.fields)?,
                }),
                syn::Data::Enum(e) => {
                    let mut variants = vec![];
                    for v in &e.variants {
                        variants.push(Variant {
                            name: v.ident.to_string(),
                            fields: analyze_fields(&v.fields)?,
                        });
                    }
                    Item::Enum(Enum { variants })
                }
                syn::Data::Union(_) => bail!("Unable to analyze union: {}", item.ident),
            };
            Ok((name, item))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;

    for item in ir.values() {
        match item {
            Item::Struct(s) => validate_fields(&ir, &s.fields)?,
            Item::Enum(e) => {
                for v in &e.variants {
                    validate_fields(&ir, &v.fields)?;
                }
            }
        }
    }

    Ok(ir)
}

fn validate_fields(items: &BTreeMap<String, Item>, fields: &[Field]) -> Result<()> {
    for f in fields {
        match &f.ty {
            Type::Local(s) if !items.contains_key(s) => {
                bail!("AST references unknown type {}", s);
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

fn analyze_type(ty: &syn::Type) -> Result<Type> {
    match ty {
        syn::Type::Path(syn::TypePath { qself: None, path }) => {
            if path.segments.len() != 1 {
                bail!("Unable to analyze qualified path: {:?}", ty);
            }
            let segment = path.segments.last().unwrap();
            let segment_name = segment.ident.to_string();

            let simple = |ty| match segment.arguments {
                syn::PathArguments::None => Ok(ty),
                _ => bail!("Unable to analyze type: {:?}", ty),
            };

            let container = |construct_ty: fn(Box<Type>) -> Type| match &segment.arguments {
                syn::PathArguments::AngleBracketed(args) if args.args.len() == 1 => {
                    match args.args.last().unwrap() {
                        syn::GenericArgument::Type(ty) => {
                            let inner = Box::new(analyze_type(ty)?);
                            Ok(construct_ty(inner))
                        }
                        _ => bail!("Unable to analyze type: {:?}", ty),
                    }
                }
                _ => bail!("Unable to analyze type: {:?}", ty),
            };

            match &*segment_name {
                "bool" | "usize" | "u64" | "char" | "String" | "PathBuf" => simple(Type::Primitive),
                // HACK(benesch): DateTimeField is part of the AST but comes
                // from another crate whose source code is not easily
                // accessible here. We probably want our own local definition
                // of this type, but for now, just hardcode it as a primitive.
                "DateTimeField" => simple(Type::Primitive),
                "Vec" => container(Type::Vec),
                "Option" => container(Type::Option),
                "Box" => container(Type::Box),
                _ => Ok(Type::Local(segment_name)),
            }
        }
        _ => bail!("Unable to analyze type: {:?}", ty),
    }
}
