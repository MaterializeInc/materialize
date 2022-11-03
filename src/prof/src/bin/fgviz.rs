use askama::Template;

use mz_build_info::build_info;
use mz_prof::http::FlamegraphTemplate;

fn main() {
    let bi = build_info!();
    let rendered = FlamegraphTemplate {
        version: &bi.human_version(),
        title: "Flamegraph Visualizer",
        mzfg: "",
    }
    .render()
    .expect("template rendering cannot fail");
    print!("{}", rendered);
}
