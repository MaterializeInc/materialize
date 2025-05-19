fn main() {
    let mut wasm = mz_wasm::Wasm::new();
    wasm.load("misc/wasm/target/wasm32-wasip1/release/function_test.wasm")
        .unwrap();
    println!("{}", wasm.call::<i64, i64>("function_test", 1).unwrap());
    println!("{}", wasm.call::<i64, i64>("function_test", 2).unwrap());
    println!("{}", wasm.call::<i64, i64>("function_test", 3).unwrap());
    println!("{}", wasm.call::<i64, i64>("function_test", 4).unwrap());
    println!("{}", wasm.call::<i64, i64>("function_test", 5).unwrap());
}
