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
    <span class="input_container">
        <span class="input_container-text">
            <input id="view_name" placeholder="View Name">
            <input id="source_name" placeholder="Relation Name">
            <input id="column_name" placeholder="JSON Column Name">
        </span>
    <fieldset class="input_container-radio">
        <legend>Kind of SQL Object</legend>
        <span>
            <input type="radio" id="view" name="type_view" value="view"/>
            <label for="view">View</label>
        </span>
        <span>
            <input type="radio" id="materialized-view" name="type_view" value="materialized-view"/>
            <label for="materialized-view">Materialized View</label>
        </span>
    </fieldset>
    </span>
    <div class="json">
        <textarea id="json_sample" placeholder="JSON Sample"></textarea>
        <div id="error_span" class="error">
            <p id="error_text"></p>
        </div>
    </div>
    <pre class="sql_output">
        <code id="output" class="sql_output-code"></code>
    </pre>
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

const error_span = $("#error_span");
const error_text = $("#error_text");

const json_input = $("#json_sample");
const sql_output = $("#output");

/// Flattens a JSON objects into a list of fields, and their chain of parents.
function handleJson(source, sample, column_name) {
    if (!column_name) {
        column_name = "body"
    }

    let selectItems = [];
    const json_object = JSON.parse(sample);

    // Format the JSON for the user.
    const pretty_json = JSON.stringify(json_object, null, 2);
    json_input.val(pretty_json);

    expandObject(json_object, [column_name], selectItems);

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
function formSql(selectItems, view_name, source_name, object_type) {
    const FIELD_SEPERATOR = "\n    ";

    if (!view_name) {
        view_name = "my_view";
    }
    if (!source_name) {
        source_name = "my_source";
    }

    let type = "VIEW";
    if (object_type === "materialized-view") {
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
    .join(`,${FIELD_SEPERATOR}`);

    if (selectItems.length > 1) {
        selects = `${FIELD_SEPERATOR}${selects}`;
    }

    const sql = `CREATE ${type} ${view_name} AS SELECT ${selects}\nFROM ${source_name};`

    return sql;
}

function errorClear() {
    error_span.attr('class', 'error error-hidden');
}

function errorSet(e) {
    error_text.text(e.message);
    error_span.attr('class', 'error error-visible');
}

function sqlClear() {
    sql_output.text("");
}

function render() {
    errorClear();
    sqlClear();

    const view_name = $("#view_name").val();
    const source_name = $("#source_name").val();
    const column_name = $("#column_name").val();
    const object_type = $("input[name='type_view']:checked").val();

    const json_sample = json_input.val();

    try {
        const items = handleJson(source_name, json_sample, column_name);
        const sql = formSql(items, view_name, source_name, object_type);
        sql_output.text(sql);

        errorClear();
    } catch (e) {
        if (json_sample) {
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


