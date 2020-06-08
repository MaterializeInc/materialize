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

//! A basic parser for Rust code.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use syn::{Data, DataEnum, DataStruct, DeriveInput, Item};

/// Parses the module at `path` and any contained submodules.
///
/// Returns [`DeriveInput`]s representing all struct and enum items in the
/// module. This is exactly what a custom derive procedural macro would see,
/// except that we can present information for all types simultaneously.
///
/// Note that parsing a Rust module is complicated. While most of the heavy
/// lifting is performed by [`syn`], syn does not understand the various options
/// for laying out a crate—and there are many attributes and edition settings
/// that control how modules can be laid out on the file system. This function
/// does not attempt to be fully general and only handles the layout used by
/// this crate.
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
            _ => (),
        }
    }
    Ok(())
}
