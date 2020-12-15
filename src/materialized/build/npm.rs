// Copyright Materialize, Inc. All rights reserved.
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
//!     versions of materialized may outlive the CDNs they refer to.
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
//!     http://unpkg.com/PACKAGE@VERSION/
//!
//! and browse the directory contents. (Note the trailing slash on the URL.) The
//! compiled JavaScript/CSS assets are usually in a folder named "dist" or
//! "UMD".
//!
//! To determine the correct digest, the easiest course of action is to provide
//! a bogus digest, then build materialized. The error message will contain the
//! actual digest computed from the downloaded assets, which you can then copy
//! into the `NpmPackage` struct.
//!
//! To reference the vendored assets, use HTML tags like the following in your
//! templates:
//!
//!     <link href="/css/vendor/package.CSS" rel="stylesheet">
//!     <script src="/js/vendor/PACKAGE.js"></script>
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
}

const NPM_PACKAGES: &[NpmPackage] = &[
    NpmPackage {
        name: "@hpcc-js/wasm",
        version: "0.3.14",
        digest: hex!("2defda7171f010f58ccfb72dad3b74978baaec7ae484025d5f405aee61826ff6"),
        css_file: None,
        js_prod_file: "dist/index.min.js",
        js_dev_file: "dist/index.js",
    },
    NpmPackage {
        name: "babel-standalone",
        version: "6.26.0",
        digest: hex!("8539e25167423cf3d1273147351406c01f4e796c836c7615995ba1faffd858ce"),
        css_file: None,
        js_prod_file: "babel.min.js",
        js_dev_file: "babel.js",
    },
    NpmPackage {
        name: "d3",
        version: "5.16.0",
        digest: hex!("3dcae05fa7d1a7c4cfbbc517d866779026191c3b81e67e2429af17f0d603f0a7"),
        css_file: None,
        js_prod_file: "dist/d3.min.js",
        js_dev_file: "dist/d3.js",
    },
    NpmPackage {
        name: "d3-flame-graph",
        version: "3.1.1",
        digest: hex!("e2cbdf1b15f675b00d3554c5f7bc7b15cc2c5ee4e51dc7bb69c312a62000ffe8"),
        css_file: Some("dist/d3-flamegraph.css"),
        js_prod_file: "dist/d3-flamegraph.min.js",
        js_dev_file: "dist/d3-flamegraph.js",
    },
    NpmPackage {
        name: "pako",
        version: "1.0.11",
        digest: hex!("99bb6f43fae78a98e70264bd66fd8eaf84a367f8e079961f38cd2a1363681ad1"),
        css_file: None,
        js_prod_file: "dist/pako.min.js",
        js_dev_file: "dist/pako.js",
    },
    NpmPackage {
        name: "react",
        version: "16.14.0",
        digest: hex!("8ba8a6953d116dba7da695ac3aa2c024773ce7b9d2bb87d68d25f80c8e37dde2"),
        css_file: None,
        js_prod_file: "umd/react.production.min.js",
        js_dev_file: "umd/react.development.js",
    },
    NpmPackage {
        name: "react-dom",
        version: "16.14.0",
        digest: hex!("6a95eebee42f1c33702a6dfbe4fc3a47df17a9b40688f85f77c75f942bd090cb"),
        css_file: None,
        js_prod_file: "umd/react-dom.production.min.js",
        js_dev_file: "umd/react-dom.development.js",
    },
];

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

    fn compute_digest(&self) -> Result<Vec<u8>, anyhow::Error> {
        let css_data = if self.css_file.is_some() {
            fs::read(self.css_path())?
        } else {
            vec![]
        };
        let js_prod_data = fs::read(self.js_prod_path())?;
        let js_dev_data = fs::read(self.js_dev_path())?;
        Ok(Sha256::new()
            .chain(&Sha256::digest(&css_data))
            .chain(&Sha256::digest(&js_prod_data))
            .chain(&Sha256::digest(&js_dev_data))
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
