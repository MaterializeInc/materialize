use std::{collections::BTreeMap, path::Path};

use wasmtime::{Engine, Linker, Module, Store};

pub struct Wasm {
    engine: Engine,
    linker: Linker<()>,
    functions: BTreeMap<String, Module>,
}

impl Wasm {
    pub fn new() -> Wasm {
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);

        // functions to export to wasm modules
        linker.func_wrap("env", "square", square).unwrap();

        Self {
            engine,
            linker,
            functions: BTreeMap::new(),
        }
    }

    pub fn load(&mut self, file: impl AsRef<Path>) -> wasmtime::Result<()> {
        let name = file
            .as_ref()
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let module = Module::from_file(&self.engine, file)?;
        self.functions.insert(name, module);
        Ok(())
    }

    pub fn call<Params, Results>(&mut self, name: &str, params: Params) -> wasmtime::Result<Results>
    where
        Params: wasmtime::WasmParams,
        Results: wasmtime::WasmResults,
    {
        let mut store = Store::new(&self.engine, ());
        let module = self.functions.get(name).unwrap();
        let instance = self.linker.instantiate(&mut store, module)?;
        let f = instance.get_typed_func::<Params, Results>(&mut store, "run")?;
        f.call(&mut store, params)
    }
}

fn square(n: i64) -> i64 {
    n * n
}
