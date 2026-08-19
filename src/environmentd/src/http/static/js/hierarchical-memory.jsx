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

function formatNameForQuery(name) {
  return `'${name.replace('\'', '\'\'')}'`;
}

const { useState, useEffect } = React;

function Dataflows(props) {
  const [addrs, setAddrs] = useState(null);
  const [records, setRecords] = useState(null);
  const [opers, setOpers] = useState(null);
  const [chans, setChans] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [page, setPage] = useState(null);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      setPage(null);

      const {
        results: [_set_cluster, _set_replica, addr_table, oper_table, chan_table, records_table],
      } = await query(`
                SET cluster = ${formatNameForQuery(props.clusterName)};
                SET cluster_replica = ${formatNameForQuery(props.replicaName)};
                SELECT
                    id, address
                FROM
                    mz_introspection.mz_dataflow_addresses;

                SELECT
                    id, name
                FROM
                    mz_introspection.mz_dataflow_operators;

                SELECT
                    channels.id, channels.from_index, channels.to_index, channels.from_port, channels.to_port, counts.sent, counts.batch_sent
                FROM
                    mz_introspection.mz_dataflow_channels AS channels
                    LEFT JOIN mz_introspection.mz_message_counts AS counts
                        ON channels.id = counts.channel_id;

                SELECT
                    operator_id as id, records, size
                FROM
                    mz_introspection.mz_arrangement_sizes;
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
        chan_table.rows.map(([id, source, target, source_port, target_port, sent, batch_sent]) => [id, [source, target, source_port, target_port, sent, batch_sent]])
      );
      setChans(chans);

      const records = Object.fromEntries(
        records_table.rows.map(([id, records, size]) => [id, [records, size]])
      )
      setRecords(records);

      console.log("Loaded");
      setLoading(false);
    };

    load().catch((error) => {
      console.log("ERROR", error);
      setError(error);
      setLoading(false);
    });
  }, [props]);

  useEffect(() => {
    if (loading || error || (page != null)) {
      return;
    }

    const render = async () => {

      console.log("Starting out");

      // Establish maps to and from ids, addresses, and names.
      const id_to_addr = Object.fromEntries(Object.entries(addrs).map(([id, addr]) => [id, addr]));
      const id_to_name = Object.fromEntries(Object.entries(opers).map(([id, name]) => [id, name]));
      const addr_to_id = Object.fromEntries(Object.entries(opers).map(([id, name]) => [addrStr(id_to_addr[id]), id]));
      const max_record_count = Math.max.apply(
        Math,
        Object.values(records).map(([records, size]) => records)
      );

      // Map scopes to children.
      const scope_children = new Map();
      const scope_channels = new Map();

      Object.entries(opers).forEach(([id, name]) => {
        let addr = id_to_addr[id];
        if (addr != null) {
          // remove the last item (will re-insert later).
          let last = addr.splice(addr.length - 1, 1)[0];
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
          scope_channels.get(addr).push([st[0], st[1], st[2], st[3], st[4], st[5]]);
        }
      });

      // Meant to render the scope identifier by addr, and its children recursively.
      async function render_scope(addr) {

        if (scope_channels.get(addr) != null && scope_children.get(addr) != undefined) {

          let ids_seen = [];
          const edges = scope_channels.get(addr).map(([source, target, source_port, target_port, sent, batch_sent]) => {
            // if either `source` or `target` are zero, they signify a scope input or output, respectively.
            let source1 = source != "0" ? addr_to_id[addr.concat(", ").concat(source)] : `input_${source_port}`;
            let target1 = target != "0" ? addr_to_id[addr.concat(", ").concat(target)] : `output_${target_port}`;
            ids_seen.push(source1);
            ids_seen.push(target1);
            return sent == null ? `${source1} -> ${target1} [style="dashed"]` :
              `${source1} -> ${target1} [label="sent ${sent} (${batch_sent})"]`;
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
              const label = escapeDotLabel(`${id} : ${name}`);
              if (scope_children.has(addrStr(id_to_addr[id]))) {
                // indicate subgraphs
                return `${id} [label="${label}",shape=house,style=filled,color=green,fillcolor="#bbffbb"]`;
              } else {
                let my_records = records["".concat(id)];
                if (my_records != null) {
                  let my_size = Math.ceil(my_records[1]/1024);
                  return `${id} [label= "${label}\nrecords: ${my_records[0]}, ${my_size} KiB",style=filled,color=red,fillcolor="#ffbbbb",shape=box]`;
                } else {
                  return `${id} [label="${label}",shape=box]`;
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
              {scope_channels.get(addr).length > 0 ? <div dangerouslySetInnerHTML={{ __html: graph }}></div> : <div></div>}
              {children.map(([name, div]) => (
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
          return (<div> </div>)
        }
      }

      setPage(await render_scope(""));
    };

    render().catch((error) => {
      console.log("ERROR", error);
      setError(error);
    }, [loading]);
  });

  return (
    <div style={{ marginTop: '2em' }}>
      {loading ? (
        <div>Loading...</div>
      ) : error ? (
        <div>error: {String(error)}</div>
      ) : (
        <div>
          {page}
        </div>
      )}
    </div>
  );
}


function addrStr(addr) {
  return addr.join(', ');
}

// Escape special characters in DOT labels. Operator names routinely contain
// double quotes, e.g. `ArrangeBy[[Column(0, "id")]]`, which would otherwise
// terminate the quoted label and make the whole graph unparseable.
function escapeDotLabel(str) {
  return str.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

function toggle_active(e) {
  console.log("toggling: ", e.target);
  e.target.classList.toggle("active");
  var content = e.target.nextElementSibling;
  // a null maxHeight collapses the item.
  if (content.style.maxHeight) {
    content.style.maxHeight = null;
  } else {
    content.style.maxHeight = "none";
  }
}

const content = document.getElementById('content2');
ReactDOM.render(
  <ClusterReplicaView>
    {(clusterName, replicaName) => (
      <Dataflows clusterName={clusterName} replicaName={replicaName} />
    )}
  </ClusterReplicaView>,
  content
);
