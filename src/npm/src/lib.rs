// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A lightweight JavaScript package manager, like npm.
//!
//! There are several goals here:
//!
//!   * Embed the JavaScript assets into the production binary so that the
//!     binary does not depend on any external resources, like JavaScript CDNs.
//!     Access to these CDNs may be blocked by corporate firewalls, and old
//!     versions of environmentd may outlive the CDNs they refer to.
//!
//!   * Avoid checking in the code for JavaScript assets. Checking in blobs of
//!     JavaScript code bloats the repository and leads to merge conflicts. Plus
//!     it is hard to verify the origin of blobs in code review.
//!
//!   * Avoid depending on a full Node.js/npm/webpack stack. Materialize is
//!     primarily a Rust project, and thus most Materialize developers do not
//!     have a Node.js installation available nor do they have the expertise to
//!     debug the inevitable errors that arise.
//!
//! Our needs are simple enough that it is straightforward to download the
//! JavaScript and CSS files we need directly from the NPM registry, without
//! involving the actual npm tool.
//!
//! In our worldview, an `NpmPackage` consists of a name, version, an optional
//! CSS file, a non-minified "development" JavaScript file, and a minified
//! "production" JavaScript file. The CSS file, if present, is extracted into
//! `CSS_VENDOR`. The production and development JS files are extracted into
//! `JS_PROD_VENDOR` and `JS_DEV_VENDOR`, respectively. A SHA-256 digest of
//! these files is computed and used to determine when the files need to be
//! updated.
//!
//! To determine the file paths to use when adding a new package, visit
//!
//!#     <http://unpkg.com/PACKAGE@VERSION/>
//!
//! and browse the directory contents. (Note the trailing slash on the URL.) The
//! compiled JavaScript/CSS assets are usually in a folder named "dist" or
//! "UMD".
//!
//! To determine the correct digest, the easiest course of action is to provide
//! a bogus digest, then build environmentd. The error message will contain the
//! actual digest computed from the downloaded assets, which you can then copy
//! into the `NpmPackage` struct.
//!
//! To reference the vendored assets, use HTML tags like the following in your
//! templates:
//!
//!#     <link href="/css/vendor/package.CSS" rel="stylesheet">
//!#     <script src="/js/vendor/PACKAGE.js"></script>
//!
//! The "/js/vendor/PACKAGE.js" will automatically switch between the production
//! and development assets based on the presence of the `dev-web` feature.

use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use flate2::read::GzDecoder;
use hex_literal::hex;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

struct NpmPackage {
    name: &'static str,
    version: &'static str,
    digest: [u8; 32],
    css_file: Option<&'static str>,
    js_prod_file: &'static str,
    js_dev_file: &'static str,
    extra_file: Option<(&'static str, &'static str)>,
}

const NPM_PACKAGES: &[NpmPackage] = &[
    NpmPackage {
        name: "@hpcc-js/wasm",
        version: "0.3.14",
        digest: hex!("b1628f561790925e58d33dcc5552aa2d1e8316a14b8436999a3c9c86df7c514a"),
        css_file: None,
        js_prod_file: "dist/index.min.js",
        js_dev_file: "dist/index.js",
        extra_file: Some((
            "dist/graphvizlib.wasm",
            "js/vendor/@hpcc-js/graphvizlib.wasm",
        )),
    },
    NpmPackage {
        name: "babel-standalone",
        version: "6.26.0",
        digest: hex!("2a6dc2f1acc2893e53cb53192eee2dfc6a09d86ef7620dcb6323c4d624f99d92"),
        css_file: None,
        js_prod_file: "babel.min.js",
        js_dev_file: "babel.js",
        extra_file: None,
    },
    NpmPackage {
        name: "d3",
        version: "5.16.0",
        digest: hex!("85aa224591310c3cdd8a2ab8d3f8421bb7b0035926190389e790497c5b1d0f0b"),
        css_file: None,
        js_prod_file: "dist/d3.min.js",
        js_dev_file: "dist/d3.js",
        extra_file: None,
    },
    NpmPackage {
        name: "d3-flame-graph",
        version: "3.1.1",
        digest: hex!("603120d8f1badfde6155816585d8e4c494f9783ae8fd40a3974928df707b1889"),
        css_file: Some("dist/d3-flamegraph.css"),
        js_prod_file: "dist/d3-flamegraph.min.js",
        js_dev_file: "dist/d3-flamegraph.js",
        extra_file: None,
    },
    NpmPackage {
        name: "pako",
        version: "1.0.11",
        digest: hex!("1243d3fd9710c9b4e04d9528db02bfa55a4055bebc24743628fdddf59c83fa95"),
        css_file: None,
        js_prod_file: "dist/pako.min.js",
        js_dev_file: "dist/pako.js",
        extra_file: None,
    },
    NpmPackage {
        name: "react",
        version: "16.14.0",
        digest: hex!("2fd361cfad2e0f8df36b67a0fdd43bd8064822b077fc7d70a84388918c663089"),
        css_file: None,
        js_prod_file: "umd/react.production.min.js",
        js_dev_file: "umd/react.development.js",
        extra_file: None,
    },
    NpmPackage {
        name: "react-dom",
        version: "16.14.0",
        digest: hex!("27f6addacabaa5e5b9aa36ef443d4e79a947f3245c7d6c77f310f9c9fc944e25"),
        css_file: None,
        js_prod_file: "umd/react-dom.production.min.js",
        js_dev_file: "umd/react-dom.development.js",
        extra_file: None,
    },
];

const STATIC: &str = "src/http/static";
const CSS_VENDOR: &str = "src/http/static/css/vendor";
const JS_PROD_VENDOR: &str = "src/http/static/js/vendor";
const JS_DEV_VENDOR: &str = "src/http/static-dev/js/vendor";

impl NpmPackage {
    fn css_path(&self) -> PathBuf {
        Path::new(CSS_VENDOR).join(&format!("{}.css", self.name))
    }

    fn js_prod_path(&self) -> PathBuf {
        Path::new(JS_PROD_VENDOR).join(&format!("{}.js", self.name))
    }

    fn js_dev_path(&self) -> PathBuf {
        Path::new(JS_DEV_VENDOR).join(&format!("{}.js", self.name))
    }

    fn extra_path(&self) -> PathBuf {
        let dst = self.extra_file.map(|(_src, dst)| dst);
        Path::new(STATIC).join(dst.unwrap_or(""))
    }

    fn compute_digest(&self) -> Result<Vec<u8>, anyhow::Error> {
        let css_data = if self.css_file.is_some() {
            fs::read(self.css_path())?
        } else {
            vec![]
        };
        let js_prod_data = fs::read(self.js_prod_path())?;
        let js_dev_data = fs::read(self.js_dev_path())?;
        let extra_data = if self.extra_file.is_some() {
            fs::read(self.extra_path())?
        } else {
            vec![]
        };
        Ok(Sha256::new()
            .chain_update(&Sha256::digest(&css_data))
            .chain_update(&Sha256::digest(&js_prod_data))
            .chain_update(&Sha256::digest(&js_dev_data))
            .chain_update(&Sha256::digest(&extra_data))
            .finalize()
            .as_slice()
            .into())
    }
}

pub fn ensure() -> Result<(), anyhow::Error> {
    println!("ensuring all npm packages are up-to-date...");

    let client = reqwest::blocking::Client::new();
    for pkg in NPM_PACKAGES {
        if pkg.compute_digest().ok().as_deref() == Some(&pkg.digest) {
            println!("{} is up-to-date", pkg.name);
            continue;
        } else {
            println!("{} needs updating...", pkg.name);
        }

        let url = format!(
            "https://registry.npmjs.org/{}/-/{}-{}.tgz",
            pkg.name,
            pkg.name.split('/').last().unwrap(),
            pkg.version,
        );
        let res = client
            .get(&url)
            .send()
            .and_then(|res| res.error_for_status())
            .with_context(|| format!("downloading {}", pkg.name))?;
        let mut archive = tar::Archive::new(GzDecoder::new(res));
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.strip_prefix("package")?.to_owned();
            if let Some(css_file) = &pkg.css_file {
                if path == Path::new(css_file) {
                    unpack_entry(&mut entry, &pkg.css_path())?;
                }
            }
            if path == Path::new(pkg.js_prod_file) {
                unpack_entry(&mut entry, &pkg.js_prod_path())?;
            }
            if path == Path::new(pkg.js_dev_file) {
                unpack_entry(&mut entry, &pkg.js_dev_path())?;
            }
            if let Some((extra_src, _extra_dst)) = &pkg.extra_file {
                if path == Path::new(extra_src) {
                    unpack_entry(&mut entry, &pkg.extra_path())?;
                }
            }
        }

        let digest = pkg
            .compute_digest()
            .with_context(|| format!("computing digest for {}", pkg.name))?;
        if digest != pkg.digest {
            bail!(
                "npm package {} did not match expected digest
expected: {}
  actual: {}",
                pkg.name,
                hex::encode(pkg.digest),
                hex::encode(digest),
            );
        }
    }

    // Clean up any stray files. This is more important than it might seem,
    // since files in `CSS_VENDOR` and `JS_PROD_VENDOR` are blindly bundled into
    // the binary at build time by the `include_dir` macro.
    let mut known_paths = HashSet::new();
    for pkg in NPM_PACKAGES {
        if pkg.css_file.is_some() {
            known_paths.insert(pkg.css_path());
        }
        known_paths.insert(pkg.js_prod_path());
        known_paths.insert(pkg.js_dev_path());
        if pkg.extra_file.is_some() {
            known_paths.insert(pkg.extra_path());
        }
    }
    for dir in &[CSS_VENDOR, JS_PROD_VENDOR, JS_DEV_VENDOR] {
        for entry in WalkDir::new(dir) {
            let entry = entry?;
            if entry.file_type().is_file() && !known_paths.contains(entry.path()) {
                println!("removing stray vendor file {}", entry.path().display());
                fs::remove_file(entry.path())?;
            }
        }
    }

    Ok(())
}

fn unpack_entry<T>(entry: &mut tar::Entry<T>, target: &Path) -> Result<(), anyhow::Error>
where
    T: Read,
{
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating directory {}", parent.display()))?;
    }
    entry.unpack(target)?;
    Ok(())
}
