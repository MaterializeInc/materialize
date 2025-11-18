// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indexmap::{IndexMap, IndexSet};
use mz_cloud_resources::crd::materialize::v1alpha1::MaterializeSpec;
use schemars::schema_for;
use serde::Serialize;

#[derive(Serialize)]
struct DocsField {
    name: String,
    r#type: String,
    description: String,
    default: Option<serde_json::Value>,
    required: bool,
}

impl DocsField {
    // Extract fields for a type (not flattened - fields are relative to the type)
    fn process_fields(
        type_name: &str,
        props: &serde_json::Value,
        root_schema: &serde_json::Value,
        types_map: &mut IndexMap<String, Vec<DocsField>>,
        processed_types: &mut IndexSet<String>,
    ) {
        // Skip if we've already processed this type
        if processed_types.contains(type_name) {
            return;
        }
        processed_types.insert(type_name.to_owned());

        let required_fields = extract_required_fields(props);

        if let Some(properties) = props.get("properties").and_then(|p| p.as_object()) {
            let mut fields = Vec::with_capacity(properties.len());
            for (name, field_props) in properties {
                let mut description = field_props
                    .get("description")
                    .and_then(|d| d.as_str())
                    .unwrap_or("")
                    .to_owned();
                let default = field_props.get("default").cloned();
                let required = required_fields.contains(name);

                // Get the field type (without nested fields)
                let r#type = DocsField::extract_field_type(
                    field_props,
                    root_schema,
                    types_map,
                    processed_types,
                );

                // If this is an enum, append the variants to the description
                if let Some(enum_text) = get_enum_description(field_props, root_schema, &default) {
                    description.push_str(&enum_text);
                }

                fields.push(DocsField {
                    name: name.to_owned(),
                    r#type,
                    description,
                    default,
                    required,
                });
            }
            types_map.insert(type_name.to_owned(), fields);
        }
    }

    // Get field type for type extraction (collects types into map, doesn't flatten)
    fn extract_field_type(
        props: &serde_json::Value,
        root_schema: &serde_json::Value,
        types_map: &mut IndexMap<String, Vec<DocsField>>,
        processed_types: &mut IndexSet<String>,
    ) -> String {
        // Check for $ref first
        let ref_str_opt = props.get("$ref").and_then(|r| r.as_str()).or_else(|| {
            // We assume there is only one non-null type in anyOf
            props
                .get("oneOf")
                .or_else(|| props.get("anyOf"))
                .and_then(|o| o.as_array())
                .and_then(|arr| {
                    arr.iter()
                        .find_map(|v| v.get("$ref").and_then(|r| r.as_str()))
                })
        });

        if let Some(ref_str) = ref_str_opt {
            let type_name = ref_str
                .split('/')
                .next_back()
                .expect("ref string should always end in a type name");

            let resolved = resolve_ref(root_schema, ref_str).expect("refs must resolve");

            // Recursively document the resolved type
            DocsField::process_fields(type_name, resolved, root_schema, types_map, processed_types);

            // If it's an enum just return Enum here, as that's more clear
            // that only some values are valid.
            if is_enum_type(resolved) {
                return "Enum".to_string();
            }
            return type_name.to_string();
        }

        // No $ref, check the type field
        match get_json_schema_type(props).as_str() {
            "array" => {
                let items = props.get("items").expect("arrays should always have items");
                let item_type =
                    DocsField::extract_field_type(items, root_schema, types_map, processed_types);
                format!("Array<{}>", item_type)
            }
            "boolean" => "Bool".to_owned(),
            "integer" => "Integer".to_owned(),
            "object" => {
                let additional_props = props
                    .get("additionalProperties")
                    .expect("objects should have additionalProperties");
                let value_type = DocsField::extract_field_type(
                    additional_props,
                    root_schema,
                    types_map,
                    processed_types,
                );
                format!("Map<String, {}>", value_type)
            }
            "string" => {
                if props.get("format").and_then(|f| f.as_str()) == Some("uuid") {
                    "Uuid".to_owned()
                } else {
                    "String".to_owned()
                }
            }
            unknown => panic!("found unexpected type: {unknown}"),
        }
    }
}

// Get type string as reported by the JSON schema
fn get_json_schema_type(schema_json: &serde_json::Value) -> String {
    let obj = schema_json
        .as_object()
        .expect("schema_json should be object");

    // Check for oneOf/anyOf (enum/union types)
    // We assume that all non-null variants are the same type.
    if let Some(variants) = obj
        .get("oneOf")
        .or_else(|| obj.get("anyOf"))
        .and_then(|o| o.as_array())
    {
        for variant in variants {
            let variant_type = get_json_schema_type(variant);
            if variant_type != "null" {
                return variant_type;
            }
        }
        panic!("found oneOf or anyOf without any non-null variants: {variants:?}");
    }

    // Check the type field
    let type_val = obj.get("type").expect("must have type");
    match type_val {
        serde_json::Value::String(type_str) => type_str.to_owned(),
        serde_json::Value::Array(type_arr) => {
            // Handle union types like ["string", "null"]
            assert_eq!(type_arr.len(), 2);
            for t in type_arr {
                let t_str = t.as_str().expect("should be string");
                if t_str != "null" {
                    return t_str.to_string();
                }
            }
            panic!("no not-null type found in object: {obj:?}");
        }
        other => panic!("unexpected value type {other:?} in object: {obj:?}"),
    }
}

fn resolve_ref<'a>(
    root_schema: &'a serde_json::Value,
    ref_str: &str,
) -> Option<&'a serde_json::Value> {
    let type_name = ref_str.strip_prefix("#/$defs/")?;
    root_schema
        .as_object()?
        .get("$defs")?
        .as_object()?
        .get(type_name)
}

// Extract required fields from a schema
fn extract_required_fields(props: &serde_json::Value) -> IndexSet<String> {
    props
        .get("required")
        .and_then(|r| r.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

// Check if a resolved schema is an enum
fn is_enum_type(resolved: &serde_json::Value) -> bool {
    resolved.get("oneOf").is_some()
}

// Get enum description text for a field
fn get_enum_description(
    field_props: &serde_json::Value,
    root_schema: &serde_json::Value,
    default: &Option<serde_json::Value>,
) -> Option<String> {
    let resolved = resolve_ref(root_schema, field_props.get("$ref")?.as_str()?)?;
    if is_enum_type(resolved) {
        return Some(format_enum_variants_from_json(resolved, default));
    }
    None
}

// Format enum variants as text to append to description
fn format_enum_variants_from_json(
    schema_json: &serde_json::Value,
    default: &Option<serde_json::Value>,
) -> String {
    let mut lines = Vec::new();
    lines.push("\n\nValid values:".to_string());

    // Extract default value string if present
    let default_str = default
        .as_ref()
        .and_then(|d| d.as_str())
        .map(|s| s.to_string());

    // Enums use oneOf array containing variant descriptions
    let variants: IndexMap<String, Option<String>> = schema_json
        .get("oneOf")
        .expect("schemars uses oneOf with const values for enums")
        .as_array()
        .expect("oneOf is always an array")
        .into_iter()
        .map(|variant| {
            let name = variant
                .get("const")
                .expect("we only handle const enums currently")
                .as_str()
                .expect("enum const values should always be strings")
                .to_owned();
            let description = variant
                .get("description")
                .and_then(|d| d.as_str())
                .map(|d| d.to_owned());
            (name, description)
        })
        .collect();

    // Format variants
    for (variant_name, variant_description) in &variants {
        let default_marker = if default_str
            .as_ref()
            .map(|d| d == variant_name)
            .unwrap_or(false)
        {
            " (default)"
        } else {
            ""
        };
        if let Some(desc) = variant_description {
            lines.push(format!(
                "- `{}`{}:<br>{}",
                variant_name,
                default_marker,
                // Prepend two spaces, so that we nest under the bullet point
                // when processed as markdown,
                desc.split("\n")
                    .map(|line| format!("  {}", line))
                    .collect::<Vec<_>>()
                    .join("\n")
            ));
        } else {
            lines.push(format!("- `{}`{}", variant_name, default_marker));
        }
    }
    lines.join("\n")
}

fn main() {
    let root_schema = schema_for!(MaterializeSpec);
    // Convert all to JSON for easier merging
    let schema_json = root_schema.to_value();

    // Extract types from the schema JSON
    let mut types_map: IndexMap<String, Vec<DocsField>> = IndexMap::new();
    let mut processed_types = IndexSet::new();

    // Get the root type name (MaterializeSpec)
    let root_type_name = schema_json.get("title").unwrap().as_str().unwrap();

    DocsField::process_fields(
        root_type_name,
        &schema_json,
        &schema_json,
        &mut types_map,
        &mut processed_types,
    );

    let types_vec: Vec<(String, Vec<DocsField>)> = types_map.into_iter().rev().collect();
    println!("{}", serde_json::to_string_pretty(&types_vec).unwrap());
}
