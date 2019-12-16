extern crate protoc_rust;

use protoc_rust::Customize;

fn main() {
    protoc_rust::run(protoc_rust::Args {
        out_dir: "",
        input: &["testdata/test-proto-schemas.proto"],
        includes: &[],
        customize: Customize {
            ..Default::default()
        },
    })
    .expect("protoc");
}
