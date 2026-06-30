// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! SP3a's color-explosion measurement sweep, ported to the SP3b mechanism. Not
//! a CI gate — the spike instrument. Each color is a flat color (parent =
//! black) in one `ColoredEGraph`, closed independently; metrics are aggregated
//! exactly as in SP3a so the §8 verdict reproduces.
//!
//! Run: `bin/cargo-test -p mz-transform --run-ignored ignored-only --no-capture \
//!   eqsat::colored::measure::measure_color_explosion`

#[cfg(test)]
mod tests {
    use crate::eqsat::colored::ColoredEGraph;
    use crate::eqsat::colored::toy::{gen_base, gen_colors, GenParams, Locality};

    /// Run one sweep cell: build a base + colors, close every color, aggregate.
    fn run_cell(axis: &str, value: usize, p: &GenParams) {
        let base = gen_base(p, 1);
        let colors = gen_colors(p, &base, 2);
        let node_count = base.node_count();

        let start = std::time::Instant::now();
        let mut tot_delta = 0usize;
        let mut tot_applied = 0usize;
        let mut tot_induced = 0usize;
        let mut max_iters = 0usize;
        for eqs in &colors {
            let mut ceg = ColoredEGraph::new(&base);
            let c = ceg.new_color(None);
            let m = ceg.close(c, eqs);
            tot_delta += m.delta_nodes;
            tot_applied += m.applied_equalities;
            tot_induced += m.induced_merges;
            max_iters = max_iters.max(m.iters);
        }
        let wall_ms = start.elapsed().as_millis();

        let naive = colors.len().saturating_mul(node_count).max(1);
        let sharing = tot_delta as f64 / naive as f64;
        let cascade = if tot_applied > 0 {
            tot_induced as f64 / tot_applied as f64
        } else {
            0.0
        };
        println!(
            "{axis},{value},{sharing:.4},{cascade:.3},{tot_delta},{node_count},{wall_ms},{max_iters}"
        );
    }

    #[mz_ore::test]
    #[ignore]
    fn measure_color_explosion() {
        let baseline = GenParams {
            base_size: 500,
            fan_out: 4,
            depth: 4,
            n_colors: 50,
            eqs_per_color: 4,
            locality: Locality::Mixed,
        };
        println!("# SP3b color-explosion sweep (ported from SP3a)");
        println!("# sharing_ratio = delta_nodes / (n_colors * node_count)");
        println!("# cascade_factor = induced_merges / applied_equalities");
        println!("axis,value,sharing_ratio,cascade_factor,delta_nodes,node_count,wall_ms,max_iters");

        for v in [100usize, 250, 500, 1000, 2500, 5000] {
            run_cell("base_size", v, &GenParams { base_size: v, ..baseline.clone() });
        }
        for v in [1usize, 2, 4, 8, 16, 32] {
            run_cell("fan_out", v, &GenParams { fan_out: v, ..baseline.clone() });
        }
        for v in [10usize, 50, 100, 250, 500, 1000] {
            run_cell("n_colors", v, &GenParams { n_colors: v, ..baseline.clone() });
        }
        for v in [1usize, 2, 4, 8, 16, 32] {
            run_cell("eqs_per_color", v, &GenParams { eqs_per_color: v, ..baseline.clone() });
        }
        for (label, loc) in [(0usize, Locality::LeafOnly), (1, Locality::Mixed), (2, Locality::SharedHot)] {
            run_cell("locality", label, &GenParams { locality: loc, ..baseline.clone() });
        }
    }
}
