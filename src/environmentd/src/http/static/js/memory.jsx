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

function ClusterReplicaView() {
  const [currentClusterName, setCurrentClusterName] = useState(null);
  const [currentReplicaName, setCurrentReplicaName] = useState(null);
  const [sqlResponse, setSqlResponse] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  const queryClusterReplicas = `
    SELECT
      clusters.name AS cluster_name, replicas.name AS replica_name
    FROM
      mz_catalog.mz_cluster_replicas replicas
      LEFT JOIN mz_catalog.mz_clusters clusters ON clusters.id = replicas.cluster_id
    ORDER BY cluster_name ASC, replica_name ASC
  `;

  useEffect(() => {
    const search = new URLSearchParams(location.search);
    const clusterName = search.get('cluster_name');
    const replicaName = search.get('replica_name');
    if (clusterName) {
      setCurrentClusterName(clusterName);
    }
    if (replicaName) {
      setCurrentReplicaName(replicaName);
    }

    query(queryClusterReplicas)
      .then((data) => {
        const results = data.results[0].rows;
        setSqlResponse(results);
        if (!replicaName && results.length > 0) {
          if(results.some(
            result => ('default' == result[0]) && ('r1' == result[1]))) {
            setCurrentClusterName('default');
            setCurrentReplicaName('r1');
          } else {
            setCurrentClusterName(results[0][0]);
            setCurrentReplicaName(results[0][1]);
          }
        }
        setLoading(false);
      })
      .catch((error) => {
        setError(error);
        setLoading(false);
      });
  }, []);

  useEffect(() => {
    if (!currentReplicaName) return;
    const params = new URLSearchParams(location.search);
    params.set('cluster_name', currentClusterName);
    params.set('replica_name', currentReplicaName);
    window.history.replaceState({}, '', `${location.pathname}?${params}`);
  }, [currentClusterName, currentReplicaName]);

  return (
    <div>
      {loading ? (
        <div>Loading...</div>
      ) : error ? (
        <div>error: {error}</div>
      ) : (
        <div>
          <label htmlFor="cluster_replica">Cluster Replica </label>
          <select
            id="cluster_replica"
            name="cluster_replica"
            onChange={(event) => {
              const clusterReplicaJson = event.target.value;
              const clusterReplica = JSON.parse(clusterReplicaJson);
              setCurrentClusterName(clusterReplica[0]);
              setCurrentReplicaName(clusterReplica[1]);
            }}
            defaultValue={JSON.stringify([currentClusterName, currentReplicaName])}
          >
            {sqlResponse.map((v) => (
              <option key={JSON.stringify(v)} value={JSON.stringify(v)}>
                {`${v[0]}.${v[1]}`}
              </option>
            ))}
          </select>
          <Views clusterName={currentClusterName} replicaName={currentReplicaName} />
        </div>
      )}
    </div>
  );
}

function Views(props) {
  const [currentDataflow, setCurrentDataflow] = useState(null);
  const [includeSystemCatalog, setIncludeSystemCatalog] = useState(false);
  const [records, setRecords] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    const search = new URLSearchParams(location.search);
    const dataflow = search.get('dataflow');
    if (dataflow) {
      setCurrentDataflow(dataflow);
    }
    setIncludeSystemCatalog(search.get('system_catalog') === 'true');
  }, []);

  useEffect(() => {
    setCurrentDataflow(null);
  }, [props]);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    if (currentDataflow) {
      params.set('dataflow', currentDataflow);
    } else {
      params.delete('dataflow');
    }
    window.history.replaceState({}, '', `${location.pathname}?${params}`);
  }, [currentDataflow]);

  const whereFragment = includeSystemCatalog ? `` : `WHERE name NOT LIKE 'Dataflow: mz_catalog.%'`;

  useEffect(() => {
    setLoading(true);
    setError(false);

    const load = async () => {
      const {
        results: [_set_cluster, _set_replica, records_table],
      } = await query(`
        SET cluster = ${formatNameForQuery(props.clusterName)};
        SET cluster_replica = ${formatNameForQuery(props.replicaName)};
        SELECT
          id, name, batches, records, size, capacity, allocations
        FROM
          mz_introspection.mz_records_per_dataflow
        ${whereFragment}
        ORDER BY
          records DESC
      `);

      setRecords(records_table.rows);
      setLoading(false);
    };
    load().catch((error) => {
      setError(error);
      setLoading(false);
    });
  }, [props, includeSystemCatalog]);

  return (
    <div>
      <div>
        <input
          type="checkbox"
          id="include_system_catalog"
          name="include_system_catalog"
          onChange={(event) => {
            const params = new URLSearchParams(location.search);
            if (event.target.checked) {
              params.set('system_catalog', 'true');
            } else {
              params.delete('system_catalog');
            }
            window.history.replaceState({}, '', `${location.pathname}?${params}`);

            setIncludeSystemCatalog(event.target.checked === true);
          }}
          checked={includeSystemCatalog}
        />
        <label htmlFor="include_system_catalog">Include system catalog</label>
      </div>
      {loading ? (
        <div>Loading...</div>
      ) : error ? (
        <div>error: {error}</div>
      ) : (
        <div>
          <table class="dataflows">
            <thead>
              <tr>
                <th>dataflow id</th>
                <th>index name</th>
                <th>batches</th>
                <th>records</th>
                <th>size [KiB]</th>
                <th>capacity [KiB]</th>
                <th>allocations</th>
              </tr>
            </thead>
            <tbody>
              {records.map((v) => (
                <tr key={v[1]}>
                  <td>{v[0]}</td>
                  <td>
                    <button
                      onClick={() => { setCurrentDataflow(v[0]); }}
                    >
                      +
                    </button>
                    {v[1]}
                  </td>
                  <td>{v[2]}</td>
                  <td>{v[3]}</td>
                  <td>{Math.round(v[4]/1024)}</td>
                  <td>{Math.round(v[5]/1024)}</td>
                  <td>{v[6]}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div>{currentDataflow ?
            <View
              dataflowId={currentDataflow}
              clusterName={props.clusterName}
              replicaName={props.replicaName} /> :
            null}</div>
        </div>
      )}
    </div>
  );
}

function View(props) {
  const [stats, setStats] = useState(null);
  const [operators, setOperators] = useState(null);
  const [channels, setChannels] = useState(null);
  const [view, setView] = useState(null);
  const [graph, setGraph] = useState(null);
  const [dot, setDot] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    setLoading(true);
    setError(false);
    setGraph(null);

    const load = async () => {
      // Use mz_dataflow_operator_parents for hierarchy - much cleaner than computing from addresses.
      // The query computes:
      // 1. All operators in the dataflow with their parent_id
      // 2. Which operators are scopes (have children)
      // 3. Memory and elapsed time stats
      // 4. All addresses (operators + channels) for edge lookups
      const {
        results: [_set_cluster, _set_replica, stats_table, operators_table, addresses_table, channels_table],
      } = await query(`
        SET cluster = ${formatNameForQuery(props.clusterName)};
        SET cluster_replica = ${formatNameForQuery(props.replicaName)};

        -- Dataflow stats
        SELECT
          name, batches, records, size
        FROM
          mz_introspection.mz_records_per_dataflow
        WHERE
          id = ${props.dataflowId};

        -- All operators with parent relationships, scope detection, and stats
        WITH dataflow_operators AS (
            SELECT
                o.id,
                o.name,
                p.parent_id,
                a.address
            FROM mz_introspection.mz_dataflow_operators o
            LEFT JOIN mz_introspection.mz_dataflow_operator_parents p ON o.id = p.id
            JOIN mz_introspection.mz_dataflow_addresses a ON o.id = a.id
            WHERE a.address[1] = ${props.dataflowId}
        ),
        scope_ids AS (
            SELECT DISTINCT parent_id as id FROM dataflow_operators WHERE parent_id IS NOT NULL
        )
        SELECT
            o.id,
            o.name,
            o.parent_id,
            o.address,
            CASE WHEN s.id IS NOT NULL THEN true ELSE false END as is_scope,
            r.records,
            r.size,
            e.elapsed_ns
        FROM dataflow_operators o
        LEFT JOIN scope_ids s ON o.id = s.id
        LEFT JOIN mz_introspection.mz_records_per_dataflow_operator r ON o.id = r.id
        LEFT JOIN mz_introspection.mz_scheduling_elapsed e ON o.id = e.id
        ORDER BY o.id;

        -- All addresses in the dataflow (for channel edge lookups)
        SELECT id, address
        FROM mz_introspection.mz_dataflow_addresses
        WHERE address[1] = ${props.dataflowId};

        -- Channel edges with message counts and ports (for tracing through region boundaries)
        SELECT
            channels.id,
            channels.from_index,
            channels.to_index,
            channels.from_port,
            channels.to_port,
            counts.sent,
            counts.batch_sent
        FROM mz_introspection.mz_dataflow_channels AS channels
        LEFT JOIN mz_introspection.mz_message_counts AS counts
            ON channels.id = counts.channel_id
        WHERE channels.id IN (
            SELECT id FROM mz_introspection.mz_dataflow_addresses
            WHERE address[1] = ${props.dataflowId}
        );
      `);

      if (stats_table.rows.length !== 1) {
        throw `unknown dataflow id ${props.dataflowId}`;
      }
      const stats_row = stats_table.rows[0];
      const stats = {
        name: stats_row[0],
        batches: stats_row[1],
        records: stats_row[2],
        size: stats_row[3],
      };
      setStats(stats);

      // Build operators map: id -> {name, parent_id, address, is_scope, records, size, elapsed_ns}
      const operators = {};
      operators_table.rows.forEach(([id, name, parent_id, address, is_scope, records, size, elapsed_ns]) => {
        operators[id] = { name, parent_id, address, is_scope, records, size, elapsed_ns };
      });
      setOperators(operators);

      // Build addresses map: id -> address (for both operators and channels)
      const addresses = {};
      addresses_table.rows.forEach(([id, address]) => {
        if (!addresses[id]) {
          addresses[id] = address;
        }
      });

      // Build channels list with ports for tracing through region boundaries
      const channels = channels_table.rows.map(([id, from_index, to_index, from_port, to_port, sent, batch_sent]) => ({
        id, from_index, to_index, from_port, to_port, sent, batch_sent, address: addresses[id]
      }));
      setChannels(channels);

      try {
        const view = await getCreateView(stats.name);
        setView(view);
      } catch (error) {
        console.debug('could not get create view:', error);
        setView(null);
      }

      setLoading(false);
    };
    load().catch((error) => {
      setError(error);
      setLoading(false);
    });
  }, [props]);

  useEffect(() => {
    if (loading || error || !operators || !channels) {
      return;
    }

    const max_record_count = Math.max(
      1, // Avoid division by zero
      ...Object.values(operators).map(op => op.records || 0)
    );

    // Build address-to-id lookup map (for operators only)
    const addrToId = {};
    Object.entries(operators).forEach(([id, op]) => {
      if (op.address) {
        addrToId[op.address.join(',')] = id;
      }
    });

    // Helper to compute address string for a channel endpoint
    function makeAddrStr(channelAddr, index) {
      if (!channelAddr) {
        return null;
      }
      const addr = channelAddr.slice();
      if (index !== "0") {
        addr.push(index);
      }
      return addr.join(',');
    }

    // Track boundary ports that are used (for creating virtual port nodes)
    const boundaryPorts = {};
    channels.forEach(({ address, from_index, to_index, from_port, to_port }) => {
      if (!address) return;
      const addrStr = address.join(',');

      if (from_index === "0") {
        boundaryPorts[`${addrStr},in,${from_port}`] = true;
      }
      if (to_index === "0") {
        boundaryPorts[`${addrStr},out,${to_port}`] = true;
      }
      if (to_index !== "0") {
        const targetAddr = makeAddrStr(address, to_index);
        const targetOp = operators[addrToId[targetAddr]];
        if (targetOp && targetOp.is_scope) {
          boundaryPorts[`${targetAddr},in,${to_port}`] = true;
        }
      }
      if (from_index !== "0") {
        const sourceAddr = makeAddrStr(address, from_index);
        const sourceOp = operators[addrToId[sourceAddr]];
        if (sourceOp && sourceOp.is_scope) {
          boundaryPorts[`${sourceAddr},out,${from_port}`] = true;
        }
      }
    });

    // Get port nodes for a given scope
    function getPortNodes(scope_id) {
      const scope = operators[scope_id];
      if (!scope || !scope.address) return [];
      const addrStr = scope.address.join(',');
      const ports = [];
      Object.keys(boundaryPorts).forEach(key => {
        if (key.startsWith(`${addrStr},`)) {
          const [, direction, port] = key.split(/,(in|out),/);
          const nodeId = `port_${addrStr.replace(/,/g, '_')}_${direction}_${port}`;
          ports.push(nodeId);
        }
      });
      return ports;
    }

    // Build nested subgraphs based on parent_id relationships.
    function buildCluster(scope_id) {
      const scope = operators[scope_id];
      if (!scope || !scope.is_scope) {
        return null;
      }

      const children = Object.entries(operators)
        .filter(([_, op]) => String(op.parent_id) === String(scope_id))
        .map(([id, op]) => ({ id, ...op }));

      const lines = [`subgraph "cluster_${scope_id}" {`];
      lines.push(`label="${escapeDotLabel(scope.name)}"`);
      lines.push(`style=rounded`);

      // Add the scope operator itself
      lines.push(`_${scope_id};`);

      // Add boundary port nodes for this scope
      const portNodes = getPortNodes(scope_id);
      portNodes.forEach(nodeId => lines.push(`${nodeId};`));

      // Add all children
      children.forEach(child => {
        if (child.is_scope) {
          const nested = buildCluster(child.id);
          if (nested) {
            lines.push(nested);
          }
        } else {
          lines.push(`_${child.id};`);
        }
      });

      lines.push('}');
      return lines.join('\n');
    }

    // Build all top-level clusters (operators whose parent is the dataflow root or has no parent in our set)
    const dataflowRoot = Object.entries(operators).find(([_, op]) => op.parent_id === null);
    const clusters = [];
    if (dataflowRoot) {
      const [rootId] = dataflowRoot;
      // Find all direct children of the root
      Object.entries(operators)
        .filter(([_, op]) => String(op.parent_id) === String(rootId) && op.is_scope)
        .forEach(([id]) => {
          const cluster = buildCluster(id);
          if (cluster) {
            clusters.push(cluster);
          }
        });
    }

    // Build edges using boundary port nodes for region crossings
    const edges = channels.map(({ id, from_index, to_index, from_port, to_port, sent, batch_sent, address }) => {
      if (!address) {
        return `// channel ${id} skipped (no address)`;
      }

      const addrStr = address.join(',');
      let from_node, to_node;

      // Determine source
      if (from_index === "0") {
        // Reading from region input port
        from_node = `port_${addrStr.replace(/,/g, '_')}_in_${from_port}`;
      } else {
        const fromAddr = makeAddrStr(address, from_index);
        const fromOp = operators[addrToId[fromAddr]];
        if (fromOp && fromOp.is_scope) {
          // Reading from child scope's output port
          from_node = `port_${fromAddr.replace(/,/g, '_')}_out_${from_port}`;
        } else if (addrToId[fromAddr] !== undefined) {
          from_node = `_${addrToId[fromAddr]}`;
        } else {
          return `// channel ${id} skipped (source not found: ${fromAddr})`;
        }
      }

      // Determine destination
      if (to_index === "0") {
        // Writing to region output port
        to_node = `port_${addrStr.replace(/,/g, '_')}_out_${to_port}`;
      } else {
        const toAddr = makeAddrStr(address, to_index);
        const toOp = operators[addrToId[toAddr]];
        if (toOp && toOp.is_scope) {
          // Writing to child scope's input port
          to_node = `port_${toAddr.replace(/,/g, '_')}_in_${to_port}`;
        } else if (addrToId[toAddr] !== undefined) {
          to_node = `_${addrToId[toAddr]}`;
        } else {
          return `// channel ${id} skipped (dest not found: ${toAddr})`;
        }
      }

      return sent == null
        ? `${from_node} -> ${to_node} [style="dashed"];`
        : `${from_node} -> ${to_node} [label="sent ${sent} (${batch_sent})"];`;
    });

    // Generate boundary port node labels
    const portLabels = Object.keys(boundaryPorts).map(key => {
      const [addr, direction, port] = key.split(/,(in|out),/);
      const nodeId = `port_${addr.replace(/,/g, '_')}_${direction}_${port}`;
      const label = direction === 'in' ? `▶ ${port}` : `${port} ▶`;
      const color = direction === 'in' ? '#90EE90' : '#FFB6C1';
      return `${nodeId} [label="${label}",shape=circle,style=filled,fillcolor="${color}",width=0.3,height=0.3,fontsize=10];`;
    });

    // Build node labels
    const oper_labels = Object.entries(operators).map(([id, op]) => {
      if (op.address && op.address.length === 0) {
        return '';
      }
      const notes = [`id: ${id}, addr: [${op.address ? op.address.join(',') : ''}]`];
      let style = '';
      if (op.records && op.records > 0) {
        const record_count = op.records;
        const size = Math.ceil((op.size || 0) / 1024);
        const pct = Math.floor((record_count / max_record_count) * 0xa0);
        const alpha = pct.toString(16).padStart(2, '0');
        notes.push(`${record_count} rows, ${size} KiB`);
        style = `,style=filled,color=red,fillcolor="#ff0000${alpha}"`;
      }
      if (op.elapsed_ns && op.elapsed_ns > 1e9) {
        notes.push(`${dispNs(op.elapsed_ns)} elapsed`);
      }
      let name = op.name;
      const maxLen = 40;
      if (name.length > maxLen + 3) {
        name = name.slice(0, maxLen) + '...';
      }
      const shape = op.is_scope ? 'house' : 'box';
      return `_${id} [label="${escapeDotLabel(name)}\n${notes.join(', ')}"${style},shape=${shape}]`;
    });

    clusters.unshift('');
    edges.unshift('');
    oper_labels.unshift('');
    portLabels.unshift('');

    const dot = `digraph {
      rankdir=TB
      ${clusters.join('\n')}
      ${edges.join('\n')}
      ${oper_labels.join('\n')}
      ${portLabels.join('\n')}
    }`;
    console.debug(dot);
    setDot(dot);
    hpccWasm.graphviz.layout(dot, 'svg', 'dot').then(setGraph);
  }, [loading, operators, channels]);

  let viewText = null;
  if (view) {
    viewText = (
      <div style={{ margin: '1em' }}>
        View: {view.name}
        <div style={{ padding: '.5em', backgroundColor: '#f5f5f5' }}>{view.create}</div>
      </div>
    );
  }

  // Check if output=dot parameter is set to return raw DOT
  const outputDot = new URLSearchParams(location.search).get('output') === 'dot';

  let dotLink = null;
  if (dot) {
    const link = new URL('https://materialize.com/memory-visualization/');
    // Pass information as a JSON object to allow for easily extending this in
    // the future. The hash is used instead of search because it reduces privacy
    // concerns and avoids server-side URL size limits.
    let data = { dot: dot };
    if (view) {
      data.view = view;
    }
    data = JSON.stringify(data);
    // Compress data and encode as base64 so it's URL-safe.
    data = pako.deflate(data, { to: 'string' });
    link.hash = btoa(data);
    dotLink = <a href={link}>share</a>;
  }

  // If output=dot, just return the raw DOT text
  if (outputDot && dot) {
    return (
      <pre style={{
        whiteSpace: 'pre-wrap',
        fontFamily: 'monospace',
        fontSize: '12px',
        backgroundColor: '#f5f5f5',
        padding: '1em',
        overflow: 'auto'
      }}>
        {dot}
      </pre>
    );
  }

  return (
    <div style={{ marginTop: '2em' }}>
      {loading ? (
        <div>Loading...</div>
      ) : error ? (
        <div>error: {error}</div>
      ) : (
        <div>
          <h3>
            Name: {stats.name}, dataflow_id: {props.dataflowId}, records: {stats.records}
          </h3>
          {dotLink}
          {viewText}
          <div dangerouslySetInnerHTML={{ __html: graph }}></div>
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
  const create_table = await query(`SHOW CREATE VIEW "${name[0]}"."${name[1]}"."${name[2]}"`);
  return { name: create_table.rows[0][0], create: create_table.rows[0][1] };
}

// Escape special characters in DOT labels
function escapeDotLabel(str) {
  return str.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
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

const content = document.getElementById('content');
ReactDOM.render(<ClusterReplicaView />, content);
