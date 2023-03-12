fn main() {
    println!("cargo:rustc-link-search=native=c-code/");
    println!("cargo:rustc-link-lib=dylib=llama");
}
