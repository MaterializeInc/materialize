---
title: "JSON Flattening"
description: "Create a view from a sample JSON data."
weight:
menu:
  main:
    parent: guides
---

XXX: This is not in the right spot.

Name of source: <input id="source" value="src">

Enter your sample JSON datum:

<textarea id="sample" rows="20" cols="40">

</textarea>

<div id="output"></div>

<script>
function escapeString(s) {
    return s.replace(`'`, `''`);
}

function escapeIdent(s) {
    return s.replace(`"`, `""`);
}

function flattenJSON(source, sample) {
    const selectItems = Object.entries(JSON.parse(sample))
        .map(([k, v]) => {
            let subscript = escapeString(k);
            let colName = escapeIdent(k);
            let cast = "";
            switch (typeof v) {
                case "boolean":
                    cast = "::bool";
                    break;
                case "number":
                    cast = "::numeric";
                    break;
                case "string":
                    cast = "->>0";
                    break;
            }
            return `        data['${subscript}']${cast} AS "${colName}"`;
        })
        .join(",\n");
    source = escapeIdent(source);
    return `CREATE MATERIALIZED VIEW processed AS
    SELECT
${selectItems}
    FROM (SELECT convert_from(bytes, 'utf8') AS data FROM "${source}");
`;
}

function render() {
    let output;
    try {
        const source = $("#source").val();
        const sample = $("#sample").val();
        const sql = flattenJSON(source, sample);
        output = `View definition: <pre>${sql}</pre&rt;`;
    } catch (e) {
        output = `Err: ${e}`;
    }
    $("#output").html(output);
}

$("#source").keyup(render);
$("#sample").keyup(render);
</script>
