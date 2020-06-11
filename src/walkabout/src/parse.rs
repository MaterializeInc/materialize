// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A basic parser for Rust code.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use syn::{Data, DataEnum, DataStruct, DataUnion, DeriveInput, Item};

/// Parses the module at `path` and any contained submodules.
///
/// Returns [`DeriveInput`]s representing all struct and enum items in the
/// module. This is exactly what a custom derive procedural macro would see,
/// except that we can present information for all types simultaneously.
///

pub fn parse_mod<P>(path: P) -> Result<Vec<DeriveInput>>
where
    P: AsRef<Path>,
{
    let mut out = vec![];
    collect_items(path, &mut out)?;
    Ok(out)
}

fn collect_items<P>(path: P, out: &mut Vec<DeriveInput>) -> Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let dir = path.parent().expect("missing parent directory");
    let stem = path
        .file_stem()
        .expect("missing file stem")
        .to_str()
        .expect("file stem is not valid UTF-8");

    let src =
        fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    let file =
        syn::parse_file(&src).with_context(|| format!("Failed to parse {}", path.display()))?;

    for item in file.items {
        match item {
            Item::Mod(item) if item.content.is_none() => {
                let path = match stem {
                    "mod" | "lib" => dir.join(&format!("{}.rs", item.ident)),
                    _ => dir.join(&format!("{}/{}.rs", stem, item.ident)),
                };
                collect_items(path, out)?;
            }
            Item::Struct(item) => {
                out.push(DeriveInput {
                    ident: item.ident,
                    vis: item.vis,
                    attrs: item.attrs,
                    generics: item.generics,
                    data: Data::Struct(DataStruct {
                        fields: item.fields,
                        struct_token: item.struct_token,
                        semi_token: item.semi_token,
                    }),
                });
            }
            Item::Enum(item) => {
                out.push(DeriveInput {
                    ident: item.ident,
                    vis: item.vis,
                    attrs: item.attrs,
                    generics: item.generics,
                    data: Data::Enum(DataEnum {
                        enum_token: item.enum_token,
                        brace_token: item.brace_token,
                        variants: item.variants,
                    }),
                });
            }
            Item::Union(item) => {
                out.push(DeriveInput {
                    ident: item.ident,
                    vis: item.vis,
                    attrs: item.attrs,
                    generics: item.generics,
                    data: Data::Union(DataUnion {
                        union_token: item.union_token,
                        fields: item.fields,
                    }),
                });
            }
            _ => (),
        }
    }
    Ok(())
}
