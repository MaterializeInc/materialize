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

function useSQL(sql) {
  const [response, setResponse] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    query(sql)
      .then((data) => {
        setResponse(data);
        setLoading(false);
      })
      .catch((error) => {
        setError(error);
        setLoading(false);
      });
  }, [sql]);

  return [response, loading, error];
}

function Views() {
  useEffect(() => {
    const search = new URLSearchParams(location.search);
    const dataflow = search.get('dataflow');
    if (dataflow) {
      setCurrent(dataflow);
    }
    setIncludeSystemCatalog(search.get('system_catalog') === 'true');
  }, []);

  const [current, setCurrent] = useState(null);
  const [includeSystemCatalog, setIncludeSystemCatalog] = useState(false);

  const where_fragment = includeSystemCatalog ? `` : `WHERE name NOT LIKE 'Dataflow: mz_catalog.%'`;

  const queryMaterializedViews = `
    SELECT
      id, name, records
    FROM
      mz_catalog.mz_records_per_dataflow_global
    ${where_fragment}
    ORDER BY
      records DESC
  `;

  const [data, loading, error] = useSQL(queryMaterializedViews);

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
          <table>
            <thead>
              <tr>
                <th>dataflow id</th>
                <th>index name</th>
                <th>records</th>
              </tr>
            </thead>
            <tbody>
              {data.results[0].rows.map((v) => (
                <tr key={v[1]}>
                  <td>{v[0]}</td>
                  <td>
                    <button
                      onClick={() => {
                        const params = new URLSearchParams(location.search);
                        params.set('dataflow', v[0]);
                        window.history.replaceState({}, '', `${location.pathname}?${params}`);

                        setCurrent(v[0]);
                      }}
                    >
                      +
                    </button>
                    {v[1]}
                  </td>
                  <td>{v[2]}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div>{current ? <View dataflow_id={current} /> : null}</div>
        </div>
      )}
    </div>
  );
}

function View(props) {
  const [stats, setStats] = useState(null);
  const [addrs, setAddrs] = useState(null);
  const [records, setRecords] = useState(null);
  const [opers, setOpers] = useState(null);
  const [chans, setChans] = useState(null);
  const [elapsed, setElapsed] = useState(null);
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
      const {
        results: [stats_table, addr_table, oper_table, chan_table, elapsed_table, records_table],
      } = await query(`
        SELECT
          name, records
        FROM
          mz_catalog.mz_records_per_dataflow_global
        WHERE
          id = ${props.dataflow_id};

        -- 1) Find the address id's value for this dataflow (innermost subselect).
        -- 2) Find all address ids whose first slot value is that (second innermost subselect).
        -- 3) Find all address values in that set (top select).
        -- DISTINCT is useful (but not necessary) because it removes the duplicates
        -- caused by multiple workers.
        SELECT DISTINCT
          id, address
        FROM
          mz_catalog.mz_dataflow_operator_addresses
        WHERE
          id
          IN (
              SELECT
                id
              FROM
                mz_catalog.mz_dataflow_operator_addresses
              WHERE
                address[1]
                  = (
                      SELECT DISTINCT
                        address[1]
                      FROM
                        mz_catalog.mz_dataflow_operator_addresses
                      WHERE
                        id = ${props.dataflow_id}
                    )
            );

        SELECT DISTINCT
          id, name
        FROM
          mz_catalog.mz_dataflow_operators
        WHERE
          id
          IN (
              SELECT
                id
              FROM
                mz_catalog.mz_dataflow_operator_addresses
              WHERE
                address[1]
                  = (
                      SELECT DISTINCT
                        address[1]
                      FROM
                        mz_catalog.mz_dataflow_operator_addresses
                      WHERE
                        id = ${props.dataflow_id}
                    )
            );

        SELECT
          id, source_node, target_node, sum(sent) as sent
        FROM
          mz_catalog.mz_dataflow_channels AS channels
          LEFT JOIN mz_catalog.mz_message_counts AS counts
              ON channels.id = counts.channel AND channels.worker = counts.source_worker
        WHERE
          id
          IN (
              SELECT
                id
              FROM
                mz_catalog.mz_dataflow_operator_addresses
              WHERE
                address[1]
                  = (
                      SELECT DISTINCT
                        address[1]
                      FROM
                        mz_catalog.mz_dataflow_operator_addresses
                      WHERE
                        id = ${props.dataflow_id}
                    )
            )
        GROUP BY id, source_node, target_node
        ;

        SELECT
          id, sum(elapsed_ns)
        FROM
          mz_catalog.mz_scheduling_elapsed
        WHERE
          id
          IN (
              SELECT
                id
              FROM
                mz_catalog.mz_dataflow_operator_addresses
              WHERE
                address[1]
                  = (
                      SELECT DISTINCT
                        address[1]
                      FROM
                        mz_catalog.mz_dataflow_operator_addresses
                      WHERE
                        id = ${props.dataflow_id}
                    )
            )
        GROUP BY
          id;

        SELECT
          id, sum(records)
        FROM
          mz_catalog.mz_records_per_dataflow_operator
        WHERE
          dataflow_id = ${props.dataflow_id}
        GROUP BY
          id;
      `);
      if (stats_table.rows.length !== 1) {
        throw `unknown dataflow id ${props.dataflow_id}`;
      }
      const stats_row = stats_table.rows[0];
      const stats = {
        name: stats_row[0],
        records: stats_row[1],
      };
      setStats(stats);

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
        chan_table.rows.map(([id, source, target, sent]) => [id, [source, target, sent]])
      );
      setChans(chans);

      setRecords(Object.fromEntries(records_table.rows));

      setElapsed(Object.fromEntries(elapsed_table.rows));

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
    if (loading || error) {
      return;
    }

    // Create a map from address to id.
    const lookup = Object.fromEntries(
      Object.entries(addrs).map(([id, addr]) => [addrStr(addr), id])
    );
    const max_record_count = Math.max.apply(Math, Object.values(records));
    const scopes = {};
    // Find all the scopes.
    Object.entries(opers).forEach(([id, name]) => {
      if (name.startsWith('Region')) {
        scopes[addrStr(addrs[id])] = [];
      }
    });
    // Populate scopes.
    Object.keys(opers).forEach((id) => {
      const addr = addrs[id];
      addr.pop();
      const str = addrStr(addr);
      if (str in scopes) {
        scopes[str].push(id);
      }
    });
    const clusters = Object.entries(scopes).map(([addr, ids]) => {
      const scope_id = lookup[addr];
      const sg = [`subgraph "cluster_${addr}" {`];
      //sg.push(`label="${opers[scope_id]} (id: ${scope_id})"`);
      sg.push(`_${scope_id};`);
      ids.forEach((id) => {
        sg.push(`_${id};`);
      });
      sg.push('}');
      return sg.join('\n');
    });
    const edges = Object.entries(chans).map(([id, [source, target, sent]]) => {
      if (!(id in addrs)) {
        return `// ${id} not in addrs`;
      }
      const from = makeAddrStr(addrs, id, source);
      const to = makeAddrStr(addrs, id, target);
      const from_id = lookup[from];
      const to_id = lookup[to];
      if (from_id === undefined) {
        return `// ${from} or not in lookup`;
      }
      if (to_id === undefined) {
        return `// ${to} or not in lookup`;
      }
      return sent == null
        ? `_${from_id} -> _${to_id} [style="dashed"];`
        : `_${from_id} -> _${to_id} [label="sent ${sent}"];`;
    });
    const oper_labels = Object.entries(opers).map(([id, name]) => {
      if (!addrs[id].length) {
        return '';
      }
      const notes = [`id: ${id}`];
      let style = '';
      if (id in records) {
        const record_count = records[id];
        // Any operator that can have records will have a red border (even if it
        // currently has 0 records). The fill color is a deeper red based on how many
        // records this operator has compared to the operator with the most records.
        const pct = record_count ? Math.floor((record_count / max_record_count) * 0xff) : 0;
        const alpha = pct.toString(16).padStart(2, '0');
        notes.push(`${record_count} records`);
        style = `,style=filled,color=red,fillcolor="#ff0000${alpha}"`;
      }
      // Only display elapsed time if it's more than 1s.
      if (id in elapsed && elapsed[id] > 1e9) {
        notes.push(`${dispNs(elapsed[id])} elapsed`);
      }
      const maxLen = 40;
      if (name.length > maxLen + 3) {
        name = name.slice(0, maxLen) + '...';
      }
      return `_${id} [label="${name} (${notes.join(', ')})"${style}]`;
    });
    oper_labels.unshift('');
    clusters.unshift('');
    edges.unshift('');
    const dot = `digraph {
      ${clusters.join('\n')}
      ${edges.join('\n')}
      ${oper_labels.join('\n')}
    }`;
    console.debug(dot);
    setDot(dot);
    hpccWasm.graphviz.layout(dot, 'svg', 'dot').then(setGraph);
  }, [loading]);

  let viewText = null;
  if (view) {
    viewText = (
      <div style={{ margin: '1em' }}>
        View: {view.name}
        <div style={{ padding: '.5em', backgroundColor: '#f5f5f5' }}>{view.create}</div>
      </div>
    );
  }

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

  return (
    <div style={{ marginTop: '2em' }}>
      {loading ? (
        <div>Loading...</div>
      ) : error ? (
        <div>error: {error}</div>
      ) : (
        <div>
          <h3>
            Name: {stats.name}, dataflow_id: {props.dataflow_id}, records: {stats.records}
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

const content = document.getElementById('content');
ReactDOM.render(<Views />, content);
