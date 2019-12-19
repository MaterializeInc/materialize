extern crate protoc_rust;

use std::env;

use protoc_rust::Customize;

fn main() {
    let generate_protos = env::var("MATERIALIZE_GENERATE_PROTO")
        .map(|v| v == "1")
        .unwrap_or(false);

    if !generate_protos {
        return;
    }

    protoc_rust::run(protoc_rust::Args {
        out_dir: "protobuf/test/",
        input: &["testdata/test-proto-schemas.proto"],
        includes: &[],
        customize: Customize {
            ..Default::default()
        },
    })
    .expect("protoc");
}
