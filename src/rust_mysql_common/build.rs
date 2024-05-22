// Copyright (c) 2020 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bindgen::builder;
use cmake::Config;
use subprocess::Exec;

use std::{
    env,
    fs::{copy, create_dir_all},
    path::{Path, PathBuf},
};

const MYSQL_DIR: &str = "mysql-8.0.35";

const LIBSTRINGS: &str = "libstrings.a";
const LIBWRAPPER: &str = "libwrapper.a";

/// Returns mysql source path.
fn download_mysql(out_dir: &Path) {
    const URL: &str = "https://downloads.mysql.com/archives/get/p/23/file/mysql-8.0.35.tar.gz";
    {
        Exec::cmd("curl").arg("-s").arg("-L").arg(URL)
            | Exec::cmd("tar").arg("-xz").arg("-C").arg(out_dir)
    }
    .join()
    .unwrap();
}

fn make_libstrings(mysql_src: &Path, libdir: &Path, out_dir: &Path) {
    println!("cargo:warning=Building libstrings.a");
    let dst = Config::new(mysql_src)
        .define("FORCE_INSOURCE_BUILD", "1")
        .define("DOWNLOAD_BOOST", "1")
        .define("WITH_BOOST", out_dir)
        .build_target("strings")
        .build();

    copy(
        dst.join("build")
            .join("archive_output_directory")
            .join(LIBSTRINGS),
        Path::new(libdir).join(LIBSTRINGS),
    )
    .unwrap();
}

fn make_libwrapper(mysql_src: &Path, libdir: &Path, out_dir: &Path) {
    println!("cargo:warning=Building libwrapper.a");
    cc::Build::new()
        .file("wrapper.cc")
        .cpp(true)
        .include(out_dir.join("build").join("include"))
        .include(mysql_src.join("include"))
        .flag("-std=c++14")
        .cargo_metadata(false)
        .compile("wrapper");
    copy(out_dir.join(LIBWRAPPER), Path::new(libdir).join(LIBWRAPPER)).unwrap();
}

#[allow(dead_code)]
fn gen_bindings(mysql_src: &Path, out_dir: &Path) {
    let builder = builder()
        .header("wrapper.hh")
        .clang_arg(format!(
            "-I{}",
            out_dir.join("build").join("include").display()
        ))
        .clang_arg(format!("-I{}", mysql_src.join("include").display()))
        .clang_arg("-std=c++14")
        .clang_arg("-stdlib=libc++")
        .allowlist_recursively(true)
        .allowlist_type("decimal_t")
        .allowlist_function("c_string2decimal")
        .allowlist_function("c_decimal2string")
        .allowlist_function("c_decimal2bin")
        .allowlist_function("c_bin2decimal")
        .allowlist_function("c_decimal_bin_size")
        .derive_debug(false)
        .use_core()
        .generate_comments(false);
    let bindings = builder
        .generate()
        .expect("unable to generate bindings for libstrings");
    bindings
        .write_to_file(
            Path::new("src")
                .join("binlog")
                .join("decimal")
                .join("test")
                .join("libstrings_bindings.rs"),
        )
        .expect("Couldn't write bindings!");
}

fn main() {
    if cfg!(all(feature = "test", unix)) {
        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
        let mysql_src = out_dir.join(MYSQL_DIR);
        let libdir = Path::new("lib")
            .join(Path::new(&env::var("CARGO_CFG_TARGET_OS").unwrap()))
            .join(env::var("CARGO_CFG_TARGET_ARCH").unwrap());

        create_dir_all(&libdir).unwrap();

        if !libdir.join(LIBSTRINGS).exists() || !libdir.join(LIBWRAPPER).exists() {
            if !mysql_src.exists() {
                println!("cargo:warning=Downloading MySql source distribution");
                download_mysql(&out_dir);
            }
            make_libstrings(&mysql_src, &libdir, &out_dir);
            make_libwrapper(&mysql_src, &libdir, &out_dir);
            // uncomment if mysql-server version bumped
            // gen_bindings(&mysql_src, &out_dir);
        }

        println!("cargo:rustc-link-search=native={}", libdir.display());
        println!("cargo:rustc-link-lib=wrapper");
        println!("cargo:rustc-link-lib=strings");
        if cfg!(target_os = "macos") {
            println!("cargo:rustc-link-lib=c++");
        }
    }
}
