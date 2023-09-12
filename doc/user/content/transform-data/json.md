---
title: "JSON to SQL"
description: "Create a view from sample JSON data."
menu:
  main:
    parent: 'transform'
    name: JSON
    weight: 20
---

Working with JSON formatted data in SQL can be tedious, because you need to constantly deconstruct
the JSON blob. The below tool accepts a JSON sample and will generate a SQL view with the
individual fields mapped to columns.


<div class="json_widget">
    <div class="json">
        <textarea title="JSON Sample" id="json_sample" placeholder="JSON Sample">
            { "payload": "materialize", "event": { "kind": 1, "success": true }, "ts": "2023-02-01T17:00:00.000Z" }
        </textarea>
        <div id="error_span" class="error">
            <p id="error_text"></p>
        </div>
    </div>
    <span class="input_container">
        <span class="input_container-text">
            <input title="View Name" id="view_name" placeholder="View Name" value="my_view">
            <input title="Relation Name" id="source_name" placeholder="Relation Name" value="my_source">
            <input title="JSON Column Name" id="column_name" placeholder="JSON Column Name" value="json_column">
        </span>
    <fieldset title="Kind of SQL Object" class="input_container-radio">
        <legend>Kind of SQL Object</legend>
        <span>
            <input type="radio" id="view" name="type_view" value="view"/>
            <label for="view">View</label>
        </span>
        <span>
            <input type="radio" id="materialized-view" name="type_view" value="materialized-view" checked/>
            <label for="materialized-view">Materialized View</label>
        </span>
    </fieldset>
    </span>
    <pre title="Generated SQL" class="sql_output chroma"><code id="output" class="sql_output-code language-sql" data-lang="sql"></code></pre>
</div>

<script>

/* Helper Methods

If this wasn't a simple script these would be in a `utils.js` or come from lodash.
*/

function escapeString(s) {
    return s.replace(`'`, `''`);
}

function escapeIdent(s) {
    return s.replace(`"`, `""`);
}

function clone(x) {
    return JSON.parse(JSON.stringify(x))
}

function debounce(callback, wait) {
    let timeout;
    return (...args) => {
        const context = this;
        clearTimeout(timeout);
        timeout = setTimeout(() => callback.apply(context, args), wait);
    }
}

/* JSON Parsing and SQL conversion */

const errorSpan = $("#error_span");
const errorText = $("#error_text");

const jsonInput = $("#json_sample");
const sqlOutput = $("#output");

/// Flattens a JSON objects into a list of fields, and their chain of parents.
function handleJson(source, sample, columnName) {
    if (!columnName) {
        columnName = "body"
    }

    let selectItems = [];
    const jsonObject = JSON.parse(sample);

    // Format the JSON for the user.
    const prettyJson = JSON.stringify(jsonObject, null, 2);
    jsonInput.val(prettyJson);

    expandObject(jsonObject, [columnName], selectItems);

    return selectItems;
}

/// Recursively iterates through the provided object, tracking the chain
/// of parent fields for later use in naming and desctructuring.
function expandObject(object, parents, columns) {
    for (const [name, value] of Object.entries(object)) {
        const subscript = escapeString(name);
        const columnName = escapeIdent(name);

        let cast = "";
        switch (typeof value) {
            case "boolean":
                cast = "::bool";
                break;
            case "number":
                cast = "::numeric";
                break;
            case "string":
                if (Date.parse(value)) {
                    cast = "::timestamp";
                }
                break;
            case "object":
                parents.push(name);
                expandObject(value, parents, columns)
                parents.pop()
                continue;
        }

        columns.push([name, cast, clone(parents)]);
    }
}

/// Given a list of fields/select items, forms a SQL query.
function formSql(selectItems, viewName, sourceName, objectType) {
    const FIELD_SEPARATOR = "\n    ";

    if (!viewName) {
        viewName = "my_view";
    }
    if (!sourceName) {
        sourceName = "my_source";
    }

    let type = "VIEW";
    if (objectType === "materialized-view") {
        type = "MATERIALIZED VIEW";
    }

    let selects = selectItems.map(([name, cast, parents]) => {
        // Note: The first "parent" is the JSON column.
        const formattedName = [...parents.slice(1), name].join("_");

        const parentPath = [parents[0], ...parents.slice(1).map((p) => `'${p}'`)].join("->");
        const formattedPath = parentPath.concat(`->>'${name}'`);

        let item = formattedPath;
        if (cast) {
            item = `(${item})${cast}`;
        }

        return `${item} AS ${formattedName}`;
    })
    .join(`,${FIELD_SEPARATOR}`);

    if (selectItems.length > 1) {
        selects = `${FIELD_SEPARATOR}${selects}`;
    }

    const sql = `CREATE ${type} ${viewName} AS SELECT ${selects}\nFROM ${sourceName};`

    return sql;
}

function errorClear() {
    errorSpan.attr('class', 'error error-hidden');
}

function errorSet(e) {
    errorText.text(e.message);
    errorSpan.attr('class', 'error error-visible');
}

function sqlSet(sql) {
    sqlOutput.text(sql.trim());
}

function sqlClear() {
    sqlOutput.text("");
}

function render() {
    errorClear();
    sqlClear();

    const viewName = $("#view_name").val();
    const sourceName = $("#source_name").val();
    const columnName = $("#column_name").val();
    const objectType = $("input[name='type_view']:checked").val();

    const jsonSample = jsonInput.val();

    try {
        const items = handleJson(sourceName, jsonSample, columnName);
        const sql = formSql(items, viewName, sourceName, objectType);
        sqlSet(sql);

        errorClear();
    } catch (e) {
        if (jsonSample) {
            console.log(e);
            errorSet(e);
        } else {
            errorClear();
        }
    }
}

render();

// Debounce at a quicker rate since these generally cannot generate errors.
$("#view_name").keyup(debounce(render, 200));
$("#source_name").keyup(debounce(render, 200));
$("#column_name").keyup(debounce(render, 200));
$("input[name='type_view']").change(render);

// Debounce relatively slowly on the JSON sample since it can generate parsing errors.
$("#json_sample").keyup(debounce(render, 600));

</script>
