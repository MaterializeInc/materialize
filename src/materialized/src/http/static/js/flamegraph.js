// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

function toHumanBytes(n) {
    const tokens = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB'];
    let iTok = 0;
    while (Math.abs(n) >= 1024 && iTok < tokens.length - 1) {
        n /= 1024;
        ++iTok;
    }
    return n.toFixed(2) + " " + tokens[iTok];
}

function renderFlamegraph(data, displayBytes) {
    let chart = flamegraph()
        .width(960)
        .setDetailsElement(document.getElementById("details"))
        .onClick(d => history.pushState({ id: d.id }, "", `#${d.id}`));

    if (displayBytes)
        chart =  chart.setLabelHandler(function (d) {
            return d.data.name + ' (' + d3.format('.3f')(100 * (d.x1 - d.x0), 3) + '%, ' + toHumanBytes(d.value) + ' )';
        });

    d3.select(window)
        .on("hashchange", () => {
            const id = parseInt(location.hash.substring(1), 10);
            if (!isNaN(id)) {
                const elem = chart.findById(id);
                if (elem)
                    chart.zoomTo(elem);
            }
        });

    d3.select("#chart")
        .datum(data)
        .call(chart);

    d3.select("#clear-button")
        .on("click", () => {
            d3.select("#search-form").node().reset();
            chart.clear();
        });

    d3.select("#reset-zoom-button")
        .on("click", () => chart.resetZoom());

    d3.select("#search-form").on("submit", () => d3.event.preventDefault());
    d3.select("#search-input").on("keyup", function () {
        chart.search(this.value);
    });
}
