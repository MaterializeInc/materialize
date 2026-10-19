// Copyright Materialize, Inc. and contributors. All rights reserved.
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

//! Writes the generated files. See the library's documentation.

fn main() {
    for (path, contents) in materialize_sdk_codegen::files() {
        if let Err(error) = std::fs::write(&path, contents) {
            eprintln!("cannot write {}: {error}", path.display());
            std::process::exit(1);
        }
        println!("wrote {}", path.display());
    }
}
