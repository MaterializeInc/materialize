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
    fn extract_type_fields(
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
        processed_types.insert(type_name.to_string());

        // Extract required fields from the schema
        let required_fields = extract_required_fields(props);

        // Extract properties
        if let Some(properties) = props.get("properties").and_then(|p| p.as_object()) {
            let mut fields = Vec::new();
            for (name, field_props) in properties {
                let description = field_props
                    .get("description")
                    .and_then(|d| d.as_str())
                    .unwrap_or("")
                    .to_string();
                let default = field_props.get("default").cloned();
                let is_required = required_fields.contains(name);

                // Get the field type (without nested fields)
                let r#type = DocsField::field_type_for_type_extraction(
                    field_props,
                    root_schema,
                    types_map,
                    processed_types,
                );

                // If this is an enum, append the variants to the description
                let mut final_description = description;
                let enum_text = get_enum_description_text(field_props, root_schema, &default);
                if !enum_text.is_empty() {
                    final_description.push_str(&enum_text);
                }

                fields.push(DocsField {
                    name: name.clone(),
                    r#type,
                    description: final_description,
                    default,
                    required: is_required,
                });
            }
            types_map.insert(type_name.to_string(), fields);
        }
    }

    // Get field type for type extraction (collects types into map, doesn't flatten)
    fn field_type_for_type_extraction(
        props: &serde_json::Value,
        root_schema: &serde_json::Value,
        types_map: &mut IndexMap<String, Vec<DocsField>>,
        processed_types: &mut IndexSet<String>,
    ) -> String {
        // Check for $ref first
        let ref_str_opt = props.get("$ref").and_then(|r| r.as_str()).or_else(|| {
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
            let type_name = ref_str.split('/').last().unwrap_or("Unknown");

            if let Some(resolved) = resolve_ref(root_schema, ref_str) {
                // Check if it's a struct with properties
                if is_struct_type(resolved) {
                    DocsField::extract_type_fields(
                        type_name,
                        resolved,
                        root_schema,
                        types_map,
                        processed_types,
                    );
                    return type_name.to_string();
                }
                // Check if it's an enum
                if is_enum_type(resolved) {
                    return "Enum".to_string();
                }
                // Check if it's a Quantity
                if is_quantity_type(resolved) {
                    return "io.k8s.apimachinery.pkg.api.resource.Quantity".to_string();
                }
                let resolved_type = get_type_from_schema_json(resolved);
                if resolved_type == "string" {
                    if is_quantity_type(resolved) {
                        return "io.k8s.apimachinery.pkg.api.resource.Quantity".to_string();
                    }
                    return "String".to_string();
                }
                if resolved_type != "Unknown" && resolved_type != type_name {
                    return resolved_type;
                }
                return type_name.to_string();
            } else {
                // Try to resolve by name
                let full_type_name = ref_str
                    .strip_prefix("#/definitions/")
                    .or_else(|| ref_str.strip_prefix("#/$defs/"))
                    .unwrap_or(type_name);

                let resolved = resolve_ref_by_name(root_schema, full_type_name)
                    .or_else(|| resolve_ref_by_name(root_schema, type_name));

                if let Some(resolved) = resolved {
                    if is_struct_type(resolved) {
                        DocsField::extract_type_fields(
                            type_name,
                            resolved,
                            root_schema,
                            types_map,
                            processed_types,
                        );
                        return type_name.to_string();
                    }
                }
                return type_name.to_string();
            }
        }

        // No $ref, check the type field
        let type_str = get_type_from_schema_json(props);
        let base_types = [
            "array", "boolean", "integer", "object", "string", "number", "null",
        ];
        if !base_types.contains(&type_str.as_str()) && type_str != "Unknown" {
            return type_str;
        }

        match type_str.as_str() {
            "array" => {
                if let Some(items) = props.get("items") {
                    let item_type = DocsField::field_type_for_type_extraction(
                        items,
                        root_schema,
                        types_map,
                        processed_types,
                    );
                    format!("Array<{}>", item_type)
                } else {
                    "Array".to_string()
                }
            }
            "boolean" => "Bool".to_string(),
            "integer" => "Integer".to_string(),
            "object" => {
                if let Some(additional_props) = props.get("additionalProperties") {
                    if additional_props.is_object() {
                        let value_type = DocsField::field_type_for_type_extraction(
                            additional_props,
                            root_schema,
                            types_map,
                            processed_types,
                        );
                        let final_value_type = if value_type == "string" {
                            if is_quantity_type(additional_props) {
                                "io.k8s.apimachinery.pkg.api.resource.Quantity"
                            } else {
                                "String"
                            }
                        } else {
                            &value_type
                        };
                        format!("Map<String, {}>", final_value_type)
                    } else {
                        // Inline object
                        "Object".to_string()
                    }
                } else {
                    // Inline object
                    "Object".to_string()
                }
            }
            "string" => {
                if props.get("enum").is_some() {
                    "Enum".to_string()
                } else if props.get("format").and_then(|f| f.as_str()) == Some("uuid") {
                    "Uuid".to_string()
                } else {
                    // Check if it's a Quantity
                    if is_quantity_type(props) {
                        return "io.k8s.apimachinery.pkg.api.resource.Quantity".to_string();
                    }
                    "String".to_string()
                }
            }
            _ => "Unknown".to_string(),
        }
    }
}

// Get type string from a JSON schema
fn get_type_from_schema_json(schema_json: &serde_json::Value) -> String {
    if let Some(obj) = schema_json.as_object() {
        // First check for $ref (reference to another type)
        if let Some(ref_val) = obj.get("$ref") {
            if let Some(ref_str) = ref_val.as_str() {
                // Extract type name from reference path
                // For external types, this might be the only info we have
                if let Some(type_name) = ref_str.split('/').last() {
                    return type_name.to_string();
                }
            }
        }

        // Check for enum field (indicates a String enum)
        if obj.contains_key("enum") {
            return "string".to_string();
        }

        // Check for oneOf/anyOf (union types)
        if obj.contains_key("oneOf") || obj.contains_key("anyOf") {
            if let Some(one_of) = obj.get("oneOf").or_else(|| obj.get("anyOf")) {
                if let Some(variants) = one_of.as_array() {
                    for variant in variants {
                        let variant_type = get_type_from_schema_json(variant);
                        if variant_type != "Unknown" && variant_type != "null" {
                            return variant_type;
                        }
                    }
                }
            }
            return "Unknown".to_string();
        }

        // Check for type field
        if let Some(type_val) = obj.get("type") {
            if let Some(type_str) = type_val.as_str() {
                return type_str.to_string();
            } else if let Some(type_arr) = type_val.as_array() {
                // Handle union types like ["string", "null"]
                for t in type_arr {
                    if let Some(t_str) = t.as_str() {
                        if t_str != "null" {
                            return t_str.to_string();
                        }
                    }
                }
            }
        }

        // If no explicit type but has properties, it's an object
        if obj.contains_key("properties") {
            return "object".to_string();
        }

        // If no explicit type but has additionalProperties, it might be a map
        if obj.contains_key("additionalProperties") {
            return "object".to_string();
        }
    }
    "Unknown".to_string()
}

// Resolve a $ref reference from the root schema
fn resolve_ref<'a>(
    root_schema: &'a serde_json::Value,
    ref_str: &str,
) -> Option<&'a serde_json::Value> {
    // References are typically like "#/$defs/TypeName" or "#/definitions/TypeName"
    // For external types, they might be like "#/definitions/io.k8s.api.core.v1.ResourceRequirements"
    if !ref_str.starts_with("#/") {
        return None;
    }

    let path = &ref_str[2..]; // Skip "#/"
    let parts: Vec<&str> = path.split('/').collect();

    if parts.len() < 2 {
        return None;
    }

    let defs_key = parts[0];
    // For multi-segment paths, join all parts after the first with dots (for external types)
    // e.g., "#/definitions/io/k8s/api/core/v1/ResourceRequirements" -> "io.k8s.api.core.v1.ResourceRequirements"
    let type_name = if parts.len() > 2 {
        parts[1..].join(".")
    } else {
        parts[1].to_string()
    };

    // Check both $defs and definitions
    // The root schema from schema_for! has a "definitions" field at the top level
    if defs_key == "$defs" || defs_key == "definitions" {
        // Try root level first (for schema_for! structure)
        if let Some(defs) = root_schema.as_object()?.get(defs_key) {
            if let Some(def_obj) = defs.as_object() {
                // Try exact match with the constructed type name
                if let Some(resolved) = def_obj.get(&type_name) {
                    return Some(resolved);
                }
                // Also try with the original path segments joined differently
                // Some schemas might use dots in the key: "io.k8s.api.core.v1.ResourceRequirements"
                let alt_type_name = parts[1..].join(".");
                if let Some(resolved) = def_obj.get(&alt_type_name) {
                    return Some(resolved);
                }
            }
        }
        // Also try under schema.definitions (in case it's nested)
        if let Some(schema) = root_schema.get("schema") {
            if let Some(defs) = schema.as_object()?.get(defs_key) {
                if let Some(def_obj) = defs.as_object() {
                    if let Some(resolved) = def_obj.get(&type_name) {
                        return Some(resolved);
                    }
                    let alt_type_name = parts[1..].join(".");
                    if let Some(resolved) = def_obj.get(&alt_type_name) {
                        return Some(resolved);
                    }
                }
            }
        }
    }
    None
}

// Helper to search for a type in a definitions object
fn search_definitions<'a>(
    def_obj: &'a serde_json::Map<String, serde_json::Value>,
    type_name: &str,
) -> Option<&'a serde_json::Value> {
    // Try exact match first
    if let Some(resolved) = def_obj.get(type_name) {
        return Some(resolved);
    }
    // Try with full path - external types might be stored with their full path
    for (key, value) in def_obj.iter() {
        // Skip exact match (already checked above)
        if key == type_name {
            continue;
        }
        // Try matching with dot separator
        if key.ends_with(&format!(".{}", type_name)) {
            return Some(value);
        }
        // Try matching where type_name is the last component (after last dot)
        if let Some(last_dot) = key.rfind('.') {
            if &key[last_dot + 1..] == type_name {
                return Some(value);
            }
        }
        // Also try if the key contains the type name (for cases with different separators)
        if key.contains(type_name)
            && (key.ends_with(type_name) || key.ends_with(&format!(".{}", type_name)))
        {
            return Some(value);
        }
    }
    None
}

// Resolve a type by name (tries both short name and full path)
fn resolve_ref_by_name<'a>(
    root_schema: &'a serde_json::Value,
    type_name: &str,
) -> Option<&'a serde_json::Value> {
    // Try to find the type in definitions, checking both the short name and full path
    let defs_keys = ["definitions", "$defs"];

    for defs_key in defs_keys {
        // Try root level first
        if let Some(defs) = root_schema.as_object()?.get(defs_key) {
            if let Some(def_obj) = defs.as_object() {
                if let Some(resolved) = search_definitions(def_obj, type_name) {
                    return Some(resolved);
                }
            }
        }
        // Also try under schema.definitions
        if let Some(schema) = root_schema.get("schema") {
            if let Some(defs) = schema.as_object()?.get(defs_key) {
                if let Some(def_obj) = defs.as_object() {
                    if let Some(resolved) = search_definitions(def_obj, type_name) {
                        return Some(resolved);
                    }
                }
            }
        }
    }
    None
}

// Get enum values from a JSON schema if it's an enum
fn get_enum_values_from_json(schema_json: &serde_json::Value) -> Option<Vec<String>> {
    if let Some(obj) = schema_json.as_object() {
        if let Some(enum_vals) = obj.get("enum").and_then(|e| e.as_array()) {
            Some(
                enum_vals
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect(),
            )
        } else {
            None
        }
    } else {
        None
    }
}

// Check if a schema represents a Quantity type
fn is_quantity_type(props: &serde_json::Value) -> bool {
    // Check $ref for Quantity
    if let Some(ref_val) = props.get("$ref") {
        if let Some(ref_str) = ref_val.as_str() {
            if ref_str.contains("Quantity") {
                return true;
            }
        }
    }
    // Check description for Quantity mention
    if let Some(desc) = props.get("description").and_then(|d| d.as_str()) {
        if desc.contains("Quantity") && desc.contains("fixed-point") {
            return true;
        }
    }
    false
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

// Check if a resolved schema is a struct (has properties)
fn is_struct_type(resolved: &serde_json::Value) -> bool {
    resolved
        .get("properties")
        .and_then(|p| p.as_object())
        .is_some()
}

// Check if a resolved schema is an enum
fn is_enum_type(resolved: &serde_json::Value) -> bool {
    get_enum_values_from_json(resolved).is_some() || resolved.get("oneOf").is_some()
}

// Get enum description text for a field
fn get_enum_description_text(
    field_props: &serde_json::Value,
    root_schema: &serde_json::Value,
    default: &Option<serde_json::Value>,
) -> String {
    // Check if this field itself is an enum
    if get_enum_values_from_json(field_props).is_some() {
        return format_enum_variants_from_json(field_props, default);
    }
    // Check if it's a $ref to an enum
    if let Some(ref_val) = field_props.get("$ref") {
        if let Some(ref_str) = ref_val.as_str() {
            if let Some(resolved) = resolve_ref(root_schema, ref_str) {
                if is_enum_type(resolved) {
                    return format_enum_variants_from_json(resolved, default);
                }
            }
        }
    }
    String::new()
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

    DocsField::extract_type_fields(
        root_type_name,
        &schema_json,
        &schema_json,
        &mut types_map,
        &mut processed_types,
    );

    let types_vec: Vec<(String, Vec<DocsField>)> = types_map.into_iter().rev().collect();
    println!("{}", serde_json::to_string_pretty(&types_vec).unwrap());
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

    // Check if this enum uses oneOf structure with variant descriptions
    // schemars uses oneOf with const values for enums
    if let Some(one_of) = schema_json
        .get("oneOf")
        .or_else(|| schema_json.get("anyOf"))
    {
        if let Some(variants) = one_of.as_array() {
            // Build a map of enum values to their descriptions from oneOf
            let mut variant_descriptions: IndexMap<String, String> = IndexMap::new();
            let mut variant_values: Vec<String> = Vec::new();

            for variant in variants {
                // Each variant in oneOf should have a "const" field with the variant value
                if let Some(const_val) = variant.get("const") {
                    if let Some(val_str) = const_val.as_str() {
                        variant_values.push(val_str.to_string());
                        if let Some(description) =
                            variant.get("description").and_then(|d| d.as_str())
                        {
                            variant_descriptions
                                .insert(val_str.to_string(), description.to_string());
                        }
                    }
                }
            }

            // Format variants from oneOf
            for val_str in &variant_values {
                let default_marker = if default_str.as_ref().map(|d| d == val_str).unwrap_or(false)
                {
                    " (default)"
                } else {
                    ""
                };
                if let Some(desc) = variant_descriptions.get(val_str) {
                    lines.push(format!(
                        "- `{}`{}:\n{}",
                        val_str,
                        default_marker,
                        // Prepend two spaces, so that we nest under the bullet point
                        // when processed as markdown,
                        desc.split("\n")
                            .map(|line| format!("  {}", line))
                            .collect::<Vec<_>>()
                            .join("\n")
                    ));
                } else {
                    lines.push(format!("- `{}`{}", val_str, default_marker));
                }
            }
            return lines.join("\n");
        }
    }

    // Fall back to simple format using enum array (if present)
    if let Some(enum_vals) = schema_json.get("enum").and_then(|e| e.as_array()) {
        for value in enum_vals {
            if let Some(val_str) = value.as_str() {
                let default_marker = if default_str.as_ref().map(|d| d == val_str).unwrap_or(false)
                {
                    " (default)"
                } else {
                    ""
                };
                lines.push(format!("- `{}`{}", val_str, default_marker));
            }
        }
    }
    lines.join("\n")
}
