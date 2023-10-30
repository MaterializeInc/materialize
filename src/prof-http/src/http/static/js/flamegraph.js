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

/// Given some stack traces along with their weights,
/// collate them into a tree structure by function name.
///
/// For example: given the following stacks and weights:
/// ([0x1234, 0xabcd], 100)
/// ([0x123a, 0xabff, 0x1234], 200)
/// ([0x1234, 0xffcc], 50)
/// and assuming that 0x1234 and 0x123a come from the function `f`,
/// 0xabcd and 0xabff come from `g`, and 0xffcc from `h`, this will produce:
///
/// "f" (350) -> "g" 200
///  |
///  v
/// "h" (50)
///
/// Stacks may carry an "annotation", which determines which top-level bin they fall into --
/// as of this writing, that only applies to separate threads, for CPU profiles.
function collateStacks(profile, symbols, anyAnnotation) {
    function trieStep(node, name) {
        for (const child of node.children) {
            if (child.name == name) {
                return child;
            }
        }
        node.children.push({name: name, value: 0, children: []});
        return node.children[node.children.length - 1];
    }
    let root = {name: "", value: 0, children: []};
    for (let stack of profile) {
        let curNode = root;
        curNode.value += stack.weight;
        if (anyAnnotation) {
            const anno = stack.annotation || "unknown";
            curNode = trieStep(curNode, anno);
            curNode.value += stack.weight;
        }
        for (let addr of stack.addresses) {
            const path = symbols[addr] || [addr];
            for (const name of path) {
                curNode = trieStep(curNode, name);
                curNode.value += stack.weight;
            }
        }
    }
    return root;
}


// .mzfg format, in ABNF:
//
// hex-digit  = DIGIT / "a" / "b" / "c" / "d" / "e" / "f"
//     / "A" / "B" / "C" / "D" / "E" / "F"
// address    = "0x" 1*16hex-digit
// non-lf     = %x00-09 / %x0B-FF ; anything but a newline
// non-lf-sc  = %x00-09 / %x0B-3A / %x3C-FF
//     ; anything but a newline or a semicolon
// non-lf-c   = %x00-09 / %x0B-39 / %x3B-FF
//     ; anything but a newline or a colon
//
// file        = header LF stacks [LF symbols]
// header      = *(header-line LF)
// header-line = *non-lf-c ": " *non-lf
//
// stacks = *(stack LF)
// stack  = *(address ";") SP 1*DIGIT ["." *DIGIT] [SP *non-lf]
//     ; The addresses of this stack, followed by
//     ; its weight, optionally followed by an annotation
//
// symbols     = (*symbol LF)
// symbol-name = *non-lf-sc
// symbol      = address SP 1*(symbol-name ";")
//     ; An address followed by its names.
//     ; An address can have more than one name, due to inlining.
//     ; For example, if 0x1234 is in function `f`, which is inlined in `g`,
//     ; the corresponding line will be 0x1234 g;f; .
function parseMzfg(input) {
    let lines = input.split('\n');
    if (lines.length != 0 && lines[lines.length - 1] == "") {
        // ignore trailing newline
        lines.pop();
    }
    let i = 0;

    // parse header
    let header = {};
    while (i < lines.length) {
        const line = lines[i];
        ++i;
        if (line.length == 0) {
            break;
        }

        const separatorIdx = line.indexOf(': ');
        if (separatorIdx == -1) {
            throw "Invalid header line: " + line;
        }
        const fieldName = line.slice(0, separatorIdx);
        const fieldValue = line.slice(separatorIdx + 2);
        header[fieldName] = fieldValue;
    }

    // validate header
    const version = header["mz_fg_version"];
    if (version != 1) {
        throw "Unrecognized version: " + version;
    }

    // parse stacks
    let stacks = [];
    let anyAnnotation = false;
    const stackRegex = /^(?<addresses>(0x[\da-fA-F]{1,16};)*) (?<weight>\d+(\.\d*)?)( (?<annotation>.*))?$/;
    while (i < lines.length) {
        const line = lines[i];
        ++i;
        if (line.length == 0) {
            break;
        }

        const parsedLine = stackRegex.exec(line);
        if (!parsedLine) {
            throw "Invalid stack trace line: " + line;
        }

        let addresses = parsedLine.groups['addresses'].split(';');
        // ignore the trailing semicolon
        addresses.pop();
        const weight = parseFloat(parsedLine.groups['weight']);
        let annotation = null;
        if (parsedLine.groups['annotation']) {
            annotation = parsedLine.groups['annotation'];
            anyAnnotation = true;
        }

        stacks.push({annotation: annotation, addresses: addresses, weight: weight});
    }

    // parse symbols
    const symbolRegex = /^(?<address>0x[\da-fA-F]{1,16}) (?<names>.+)$/;
    let names = {};

    while (i < lines.length) {
        const line = lines[i];
        ++i;

        const parsedLine = symbolRegex.exec(line);
        if (!parsedLine) {
            throw "Invalid symbol line: " + line;
        }
        // unescape the semicolons and backslashes in the line
        let escaping = false;
        let path = [];
        let name_buf = "";
        for (const ch of parsedLine.groups['names']) {
            if (escaping) {
                name_buf += ch;
                escaping = false;
            } else if (ch == '\\') {
                escaping = true;
            } else if (ch == ';') {
                // we split on un-escaped `;`, so we add the piece here
                // and reset the buffer
                path.push(name_buf);
                name_buf = "";
            } else {
                name_buf += ch;
            }
        }

        // There should be a trailing semicolon, but let's
        // be robust in case there isn't
        if (name_buf) {
            path.push(name_buf);
        }
        const addr = parsedLine.groups['address'];
        names[addr] = path;
    }

    return {names: names, stacks: stacks, anyAnnotation: anyAnnotation, header: header};
}

function renderExtra(extra) {
    let div = document.getElementById("extras");
    div.textContent = '';
    for (const extraKey in extra) {
        let domLine = document.createElement("p");
        const text = document.createTextNode(extraKey + ": " + extra[extraKey]);
        domLine.appendChild(text);
        div.appendChild(domLine);
    }
}

function renderPageFromMzfg(mzfg) {
    try {
        let parsedMzfg = parseMzfg(mzfg);
        let data = collateStacks(parsedMzfg.stacks, parsedMzfg.names, parsedMzfg.anyAnnotation);
        renderFlamegraph(data, parsedMzfg.header['display_bytes']);
        renderExtra(parsedMzfg.header);
    } catch(error) {
        alert("Error: " + error);
    }
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

function loadFile(ev) {
    const file = ev.target.files[0];
    file.text().then((mzfg) => {
        renderPageFromMzfg(mzfg);
    });
}

function download(filename, data) {
    let element = document.createElement('a');
    element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(data));
    element.setAttribute('download', filename);

    element.style.display = 'none';
    document.body.appendChild(element);

    element.click();

    document.body.removeChild(element);
}
