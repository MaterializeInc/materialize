// Copyright 2009 The Go Authors. All rights reserved.
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
//
// Portions of this file are derived from the path.Clean function in the Go
// project. The original source code was retrieved on January 19, 2022 from:
//
//     https://github.com/golang/go/blob/e7d5857a5a82551b8a70b6174ec73422442250ce/src/path/path.go
//
// The original source code is subject to the terms of the 3-clause BSD license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! Path utilities.

use std::path::{Component, Path, PathBuf};

/// Extension methods for [`Path`].
pub trait PathExt {
    /// Normalizes a path using purely lexical analysis.
    ///
    /// The following normalization rules are applied iteratively:
    ///
    ///   * Multiple contiguous path separators are replaced with a single
    ///     [`MAIN_SEPARATOR`].
    ///   * Current directory components (`.`) are removed.
    ///   * Parent directory components (`..`) that do not occur at the
    ///     beginning of the path are removed along with the preceding
    ///     component.
    ///    * Parent directory components at the start of a rooted path
    ///      (e.g., `/..`) are removed.
    ///    * Empty paths are replaced with ".".
    ///
    /// The returned path ends in a separator only if it represents the root
    /// directory.
    ///
    /// This method is a port of Go's [`path.Clean`] function.
    ///
    /// [`path.Clean`]: https://pkg.go.dev/path#Clean
    /// [`MAIN_SEPARATOR`]: std::path::MAIN_SEPARATOR
    fn clean(&self) -> PathBuf;
}

impl PathExt for Path {
    fn clean(&self) -> PathBuf {
        let mut buf = PathBuf::new();
        for component in self.components() {
            match component {
                // `.` elements are always redundant and can be dropped.
                Component::CurDir => (),

                // `..` elements require special handling.
                Component::ParentDir => match buf.components().last() {
                    // `..` at beginning or after another `..` needs to be
                    // retained.
                    None | Some(Component::ParentDir) => buf.push(Component::ParentDir),
                    // `..` after a root is a no-op.
                    Some(Component::RootDir) => (),
                    // `..` after a normal component can be normalized by
                    // dropping the prior component.
                    _ => {
                        buf.pop();
                    }
                },

                // All other component types can be pushed verbatim.
                Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                    buf.push(component);
                }
            }
        }
        if buf.as_os_str().is_empty() {
            buf.push(".");
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::PathExt;

    #[mz_test_macro::test]
    fn test_clean() {
        // These test cases are imported from the Go standard library.
        for (input, output) in &[
            // Already clean.
            ("", "."),
            ("abc", "abc"),
            ("abc/def", "abc/def"),
            ("a/b/c", "a/b/c"),
            (".", "."),
            ("..", ".."),
            ("../..", "../.."),
            ("../../abc", "../../abc"),
            ("/abc", "/abc"),
            ("/", "/"),
            // Remove trailing slash.
            ("abc/", "abc"),
            ("abc/def/", "abc/def"),
            ("a/b/c/", "a/b/c"),
            ("./", "."),
            ("../", ".."),
            ("../../", "../.."),
            ("/abc/", "/abc"),
            // Remove doubled slash.
            ("abc//def//ghi", "abc/def/ghi"),
            ("//abc", "/abc"),
            ("///abc", "/abc"),
            ("//abc//", "/abc"),
            ("abc//", "abc"),
            // Remove . elements.
            ("abc/./def", "abc/def"),
            ("/./abc/def", "/abc/def"),
            ("abc/.", "abc"),
            // Remove .. elements.
            ("abc/def/ghi/../jkl", "abc/def/jkl"),
            ("abc/def/../ghi/../jkl", "abc/jkl"),
            ("abc/def/..", "abc"),
            ("abc/def/../..", "."),
            ("/abc/def/../..", "/"),
            ("abc/def/../../..", ".."),
            ("/abc/def/../../..", "/"),
            ("abc/def/../../../ghi/jkl/../../../mno", "../../mno"),
            // Combinations.
            ("abc/./../def", "def"),
            ("abc//./../def", "def"),
            ("abc/../../././../def", "../../def"),
        ] {
            println!("clean({}) = {}", input, output);
            assert_eq!(Path::new(input).clean(), Path::new(output));
        }
    }
}
