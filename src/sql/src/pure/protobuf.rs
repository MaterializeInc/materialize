// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compilation of Protobuf schemas fetched from a Confluent Schema Registry.

use std::collections::BTreeMap;
use std::path::Path;

use mz_repr::strconv;
use mz_sql_parser::ast::CsrSeedProtobufSchema;
use protox::Compiler;
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};

use crate::plan::PlanError;

/// Compiles the `.proto` file named `primary` into an encoded
/// `FileDescriptorSet` that includes all of its transitive imports.
///
/// `files` maps file names, as they appear in `import` statements, to their
/// source text. The primary file must define exactly one message.
pub(super) fn compile_proto_files(
    primary: &str,
    files: BTreeMap<String, String>,
) -> Result<CsrSeedProtobufSchema, PlanError> {
    // Registry-supplied files take precedence over the bundled well-known
    // types, because the Confluent toolchain publishes its own copies of
    // e.g. `google/protobuf/timestamp.proto` that its producers were built
    // against.
    let mut resolver = ChainFileResolver::new();
    resolver.add(InMemoryFileResolver(files));
    resolver.add(GoogleFileResolver::new());

    let mut compiler = Compiler::with_file_resolver(resolver);
    compiler.include_imports(true);
    compiler
        .open_file(primary)
        .map_err(|cause| PlanError::InvalidProtobufSchema { cause })?;

    // NOTE: The set is ordered topologically with dependencies first, so the
    // primary file is not necessarily at index 0.
    let fds = compiler.file_descriptor_set();
    let primary_fd = fds
        .file
        .iter()
        .find(|f| f.name() == primary)
        .expect("compiled file is in the file descriptor set");
    let message_name = match primary_fd.message_type.as_slice() {
        [message] => message.name().to_owned(),
        [] => bail_unsupported!(29603, "Protobuf schemas with no messages"),
        _ => bail_unsupported!(29603, "Protobuf schemas with multiple messages"),
    };

    let mut schema = String::new();
    strconv::format_bytes(&mut schema, &compiler.encode_file_descriptor_set());

    Ok(CsrSeedProtobufSchema {
        schema,
        message_name,
    })
}

/// Resolves files by exact name from an in-memory map.
#[derive(Debug)]
struct InMemoryFileResolver(BTreeMap<String, String>);

impl FileResolver for InMemoryFileResolver {
    // Subject names are used verbatim as file names, so bypass protox's
    // path normalization for names we know about.
    fn resolve_path(&self, path: &Path) -> Option<String> {
        let name = path.to_str()?;
        self.0.contains_key(name).then(|| name.to_owned())
    }

    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        match self.0.get(name) {
            Some(source) => File::from_source(name, source),
            None => Err(protox::Error::file_not_found(name)),
        }
    }
}

#[cfg(test)]
mod tests {
    use prost_reflect::DescriptorPool;

    use super::*;

    fn compile(primary: &str, files: &[(&str, &str)]) -> Result<CsrSeedProtobufSchema, PlanError> {
        let files = files
            .iter()
            .map(|(name, source)| (name.to_string(), source.to_string()))
            .collect();
        compile_proto_files(primary, files)
    }

    fn decode(schema: &CsrSeedProtobufSchema) -> DescriptorPool {
        let bytes = strconv::parse_bytes(&schema.schema).expect("valid bytes literal");
        DescriptorPool::decode(bytes.as_slice()).expect("valid file descriptor set")
    }

    fn invalid_schema_detail(err: PlanError) -> String {
        assert!(
            matches!(err, PlanError::InvalidProtobufSchema { .. }),
            "unexpected error: {err:?}"
        );
        err.detail().expect("detail")
    }

    #[mz_ore::test]
    fn imports_with_references() {
        let schema = compile(
            "topic-value",
            &[
                (
                    "topic-value",
                    r#"syntax = "proto3"; import "a.proto"; import "b.proto";
                       message Importer { A a = 1; B b = 2; }"#,
                ),
                ("a.proto", r#"syntax = "proto3"; message A { bool x = 1; }"#),
                (
                    "b.proto",
                    r#"syntax = "proto3"; import "a.proto"; message B { A a = 1; }"#,
                ),
            ],
        )
        .unwrap();
        assert_eq!(schema.message_name, "Importer");

        let pool = decode(&schema);
        let names: Vec<_> = pool.files().map(|f| f.name().to_owned()).collect();
        assert_eq!(names, ["a.proto", "b.proto", "topic-value"]);
        assert!(pool.get_message_by_name("Importer").is_some());
    }

    #[mz_ore::test]
    fn well_known_type_import() {
        let schema = compile(
            "topic-value",
            &[(
                "topic-value",
                r#"syntax = "proto3"; import "google/protobuf/timestamp.proto";
                   message M { google.protobuf.Timestamp ts = 1; }"#,
            )],
        )
        .unwrap();
        let ts = decode(&schema)
            .get_message_by_name("google.protobuf.Timestamp")
            .expect("bundled Timestamp");
        assert!(ts.get_field_by_name("seconds").is_some());
        assert!(ts.get_field_by_name("nanos").is_some());
    }

    #[mz_ore::test]
    fn registry_overrides_well_known_type() {
        let schema = compile(
            "topic-value",
            &[
                (
                    "topic-value",
                    r#"syntax = "proto3"; import "google/protobuf/timestamp.proto";
                       message M { google.protobuf.Timestamp ts = 1; }"#,
                ),
                (
                    "google/protobuf/timestamp.proto",
                    r#"syntax = "proto3"; package google.protobuf;
                       message Timestamp { int64 seconds = 1; int32 nanos = 2; int32 extra = 3; }"#,
                ),
            ],
        )
        .unwrap();
        let ts = decode(&schema)
            .get_message_by_name("google.protobuf.Timestamp")
            .unwrap();
        assert!(ts.get_field_by_name("extra").is_some());
    }

    #[mz_ore::test]
    fn message_count() {
        let err = compile("p", &[("p", r#"syntax = "proto3";"#)]).unwrap_err();
        assert!(
            err.to_string().contains("no messages"),
            "unexpected error: {err}"
        );

        let err = compile(
            "p",
            &[("p", r#"syntax = "proto3"; message A {} message B {}"#)],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("multiple messages"),
            "unexpected error: {err}"
        );

        // Messages in imported files do not count towards the primary file.
        let schema = compile(
            "p",
            &[
                (
                    "p",
                    r#"syntax = "proto3"; import "dep"; message A { B b = 1; }"#,
                ),
                ("dep", r#"syntax = "proto3"; message B {} message C {}"#),
            ],
        )
        .unwrap();
        assert_eq!(schema.message_name, "A");
    }

    #[mz_ore::test]
    fn invalid_schemas() {
        let detail = invalid_schema_detail(
            compile(
                "p",
                &[("p", "syntax = \"proto3\";\nmessage A { int32 x = 1 }")],
            )
            .unwrap_err(),
        );
        assert!(detail.starts_with("p:2:"), "unexpected detail: {detail}");

        let detail = invalid_schema_detail(
            compile(
                "p",
                &[("p", r#"syntax = "proto3"; import "missing.proto";"#)],
            )
            .unwrap_err(),
        );
        assert!(
            detail.contains("import 'missing.proto' not found"),
            "unexpected detail: {detail}"
        );

        let detail = invalid_schema_detail(
            compile(
                "p",
                &[("p", r#"syntax = "proto3"; message A { Unknown u = 1; }"#)],
            )
            .unwrap_err(),
        );
        assert!(detail.contains("Unknown"), "unexpected detail: {detail}");
    }
}
