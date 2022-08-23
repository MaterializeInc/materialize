// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

'use strict';

const hpccWasm = window['@hpcc-js/wasm'];

async function query(sql) {
  const response = await fetch('/api/sql', {
    method: 'POST',
    body: JSON.stringify({sql: sql}),
    headers: { 'Content-Type': 'application/json' },
  });
  if (!response.ok) {
    const text = await response.text();
    throw `request failed: ${response.status} ${response.statusText}: ${text}`;
  }
  const data = await response.json();
  return data;
}

const { useState, useEffect } = React;

function Dataflows() {
    const [stats, setStats] = useState(null);
    const [addrs, setAddrs] = useState(null);
    const [records, setRecords] = useState(null);
    const [opers, setOpers] = useState(null);
    const [chans, setChans] = useState(null);
    const [view, setView] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(false);
    const [page, setPage] = useState(null);

    useEffect(() => {

        if (!loading) {
            return;
        }

        const load = async () => {

            const {
                results: [addr_table, oper_table, chan_table, records_table],
            } = await query(`
                SELECT DISTINCT
                    id, address
                FROM
                    mz_catalog.mz_dataflow_addresses;

                SELECT DISTINCT
                    id, name
                FROM
                    mz_catalog.mz_dataflow_operators;

                SELECT
                    id, source_node, target_node, source_port, target_port, sum(sent) as sent
                FROM
                    mz_catalog.mz_dataflow_channels AS channels
                    LEFT JOIN mz_catalog.mz_message_counts AS counts
                        ON channels.id = counts.channel AND channels.worker = counts.source_worker
                GROUP BY id, source_node, target_node, source_port, target_port;

                SELECT
                    operator as id, sum(records)
                FROM
                    mz_catalog.mz_arrangement_sizes
                GROUP BY
                    id;
            `);

            // Map from id to address (array). {320: [11], 321: [11, 1]}.
            const addrs = {};
            addr_table.rows.forEach(([id, address]) => {
                if (!addrs[id]) {
                    addrs[id] = address;
                }
            });
            setAddrs(addrs);

            // Map from id to operator name. {320: 'name'}.
            const opers = Object.fromEntries(oper_table.rows);
            setOpers(opers);

            // {id: [source, target]}.
            const chans = Object.fromEntries(
                chan_table.rows.map(([id, source, target, source_port, target_port, sent]) => [id, [source, target, source_port, target_port, sent]])
            );
            setChans(chans);

            setRecords(Object.fromEntries(records_table.rows));

            try {
                const view = await getCreateView(stats.name);
                setView(view);
            } catch (error) {
                console.debug('could not get create view:', error);
                setView(null);
            }

            console.log("Loaded");
            setLoading(false);
        };

        load().catch((error) => {
            console.log("ERROR", error);
            setError(error);
            setLoading(false);
        });
    });

    useEffect(() => {

        if (loading || error || (page != null)) {
            return;
        }

        const render = async() => {

            console.log("Starting out");

            // Establish maps to and from ids, addresses, and names.
            const id_to_addr = Object.fromEntries(Object.entries(addrs).map(([id, addr]) => [id, addr]));
            const id_to_name = Object.fromEntries(Object.entries(opers).map(([id, name]) => [id, name]));
            const addr_to_id = Object.fromEntries(Object.entries(opers).map(([id, name]) => [addrStr(id_to_addr[id]), id]));
            const max_record_count = Math.max.apply(Math, Object.values(records));

            // Map scopes to children.
            const scope_children = new Map();
            const scope_channels = new Map();

            Object.entries(opers).forEach(([id, name]) => {
                let addr = id_to_addr[id];
                if (addr != null) {
                    // remove the last item (will re-insert later).
                    let last = addr.splice(addr.length-1, 1)[0];
                    let prefix_addr = addrStr(addr);
                    if (!scope_children.has(prefix_addr)) { scope_children.set(prefix_addr, []); }
                    if (!scope_channels.has(prefix_addr)) { scope_channels.set(prefix_addr, []); }
                    scope_children.get(prefix_addr).push(last);
                    addr.push(last);
                }
            });

            // Map scopes to edges.
            let channels = [...new Set(Object.entries(chans))];
            channels.forEach(([id, st]) => {
                if (id_to_addr[id] != null) {
                    let addr = addrStr(id_to_addr[id]);
                    if (!scope_children.has(addr)) { scope_channels.set(addr, []); }
                    if (!scope_channels.has(addr)) { scope_channels.set(addr, []); }
                    scope_channels.get(addr).push([st[0], st[1], st[2], st[3], st[4]]);
                }
            });

            // Meant to render the scope identifier by addr, and its children recursively.
            async function render_scope(addr) {

                if (scope_channels.get(addr) != null) {

                    let ids_seen = [];
                    const edges = scope_channels.get(addr).map(([source, target, source_port, target_port, sent]) => {
                        // if either `source` or `target` are zero, they signify a scope input or output, respectively.
                        let source1 = source != 0 ? addr_to_id[addr.concat(", ").concat(source)] : `input_${source_port}`;
                        let target1 = target != 0 ? addr_to_id[addr.concat(", ").concat(target)] : `output_${target_port}`;
                        ids_seen.push(source1);
                        ids_seen.push(target1);
                        return sent == null ? `${source1} -> ${target1} [style="dashed"]` :
                            `${source1} -> ${target1} [label="sent ${sent}"]`;
                    })

                    const children = [];
                    for (const id of scope_children.get(addr)) {
                        let name = (addr == "") ? "".concat(id) : addr.concat(", ".concat(id));
                        if (scope_channels.get(name) != null) {
                            let id = addr_to_id[name];
                            let text_name = id_to_name[id];
                            children.push([id.concat(" : ").concat(text_name), await render_scope(name)]);
                        }
                    };

                    edges.unshift('');

                    const labels = ids_seen.map((id) => {
                        let name = id_to_name[id];
                        if (name != null) {
                            if (scope_children.has(addrStr(id_to_addr[id]))) {
                                // indicate subgraphs
                                return `${id} [label="${id} : ${name}",shape=house,style=filled,color=green,fillcolor="#bbffbb"]`;
                            } else {
                                let my_records = records["".concat(id)];
                                if (my_records != null) {
                                    return `${id} [label= "${id} : ${name} \n\t records : ${my_records}",style=filled,color=red,fillcolor="#ffbbbb"]`;
                                } else {
                                    return `${id} [label="${id} : ${name}"]`;
                                }
                            }
                        } else {
                            return `${id} [label="${id}",shape=box,style=filled,color=blue,fillcolor="#bbbbff"]`;
                        }
                    });
                    labels.unshift('');

                    const dot = `digraph {
                        ${edges.join('\n')}
                        ${labels.join('\n')}
                    }`;
                    let graph = await hpccWasm.graphviz.layout(dot, 'svg', 'dot');
                    return (
                        <div>
                          { scope_channels.get(addr).length > 0 ? <div dangerouslySetInnerHTML={{ __html: graph } }></div> : <div></div> }
                          { children.map(([name, div]) => (
                            <div>
                              <button class="collapsible" onClick={toggle_active}>{name}</button>
                              <div class="content">
                                {div}
                              </div>
                            </div>
                          ))}
                        </div>
                    );
                } else {
                    return (<div> </div> )
                }
            }

            setPage(await render_scope(""));
        };

        render().catch((error) => {
            console.log("ERROR", error);
            setError(error);
        });
    });

    return (
      <div style={{ marginTop: '2em' }}>
        {loading ? (
          <div>Loading...</div>
        ) : error ? (
          <div>error: {error}</div>
        ) : (
          <div>
              {page}
          </div>
        )}
      </div>
    );
}


async function getCreateView(dataflow_name) {
  // dataflow_name is the full name of the dataflow operator. It is generally
  // of the form "Dataflow: <database>.<schema>.<index name>". We will use a
  // regex to parse these out and use them to get the fully qualified view name
  // which we will use with SHOW CREATE VIEW to show the SQL that created this
  // dataflow.
  //
  // There are known problems with this method. It doesn't know anything about
  // SQL parsing or escaping, assumes the dataflow operator's name is of a very
  // specific shape, and assumes a CREATE VIEW statement made an index which made
  // this dataflow. So we assume that problems can happen at any level here and
  // will cleanly bail if anything doesn't exactly match what we want. In that
  // case we will not show the SQL. This is intended to be good enough for most
  // users for now.
  const match = dataflow_name.match(/^Dataflow: (.*)\.(.*)\.(.*)$/);
  if (!match) {
    throw 'unknown dataflow name pattern';
  }
  const view_name_table = await query(`
    SELECT
      d.name AS database, s.schema, s.view
    FROM
      mz_catalog.mz_databases AS d
      JOIN (
          SELECT
            s.database_id, s.name AS schema, v.view
          FROM
            mz_catalog.mz_schemas AS s
            JOIN (
                SELECT
                  name AS view, schema_id
                FROM
                  mz_catalog.mz_views
                WHERE
                  id
                  = (
                      SELECT
                        DISTINCT idx.on_id
                      FROM
                        mz_catalog.mz_databases AS db,
                        mz_catalog.mz_schemas AS sc,
                        mz_catalog.mz_indexes AS idx
                      WHERE
                        db.name = '${match[1]}'
                        AND sc.name = '${match[2]}'
                        AND idx.name = '${match[3]}'
                    )
              )
                AS v ON s.id = v.schema_id
        )
          AS s ON d.id = s.database_id;
  `);
  if (view_name_table.rows.length !== 1) {
    throw 'could not determine view';
  }
  const name = view_name_table.rows[0];
  const create_table = await query(
    `SHOW CREATE VIEW "${name[0]}"."${name[1]}"."${name[2]}"`
  );
  return { name: create_table.rows[0][0], create: create_table.rows[0][1] };
}

function makeAddrStr(addrs, id, other) {
  let addr = addrs[id].slice();
  // The 0 source or target should not append itself to the address.
  if (other !== 0) {
    addr.push(other);
  }
  return addrStr(addr);
}

function addrStr(addr) {
  return addr.join(', ');
}

// dispNs displays ns nanoseconds in a human-readable string.
function dispNs(ns) {
  const timeTable = [
    [60, 's'],
    [60, 'm'],
    [60, 'h'],
  ];
  const parts = [];
  let v = ns / 1e9;
  timeTable.forEach(([div, disp], idx) => {
    const part = Math.floor(v % div);
    if (part >= 1 || idx === 0) {
      parts.unshift(`${part}${disp}`);
    }
    v = Math.floor(v / div);
  });
  return parts.join('');
}

function toggle_active(e) {
    console.log("toggling: ", e.target);
    e.target.classList.toggle("active");
    var content = e.target.nextElementSibling;
    // a null maxHeight collapses the item.
    if (content.style.maxHeight){
        content.style.maxHeight = null;
    } else {
        content.style.maxHeight = "none";
    }
}

const content = document.getElementById('content2');
ReactDOM.render(<Dataflows />, content);
