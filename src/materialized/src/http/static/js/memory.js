// Copyright Materialize, Inc. All rights reserved.
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
	const body = new URLSearchParams();
	body.append('sql', sql);
	const response = await fetch('/sql', {
		method: 'POST',
		body: body,
		headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
	});
	if (!response.ok) {
		const text = await response.text();
		throw (
			'request failed: ' +
			response.status +
			' ' +
			response.statusText +
			': ' +
			text
		);
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
				console.debug(typeof error);
				console.debug(JSON.stringify(error));
				setError(error);
				setLoading(false);
			});
	}, [sql]);

	return [response, loading, error];
}

function Views() {
	const queryMaterializedViews = `
		SELECT
			*
		FROM
			mz_catalog.mz_records_per_dataflow_global
			--materialized_records_per_dataflow_global
		WHERE
			name NOT LIKE 'Dataflow: mz_catalog.%'
	`;

	useEffect(() => {
		const search = new URLSearchParams(location.search);
		const dataflow = search.get('dataflow');
		if (dataflow) {
			setCurrent([dataflow, dataflow]);
		}
	}, []);

	const [current, setCurrent] = useState(null);
	const [data, loading, error] = useSQL(queryMaterializedViews);

	return (
		<div>
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
							{data.rows.map((v) => (
								<tr key={v[1]}>
									<td>{v[0]}</td>
									<td>
										<button
											onClick={() => {
												const params = new URLSearchParams(location.search);
												params.set('dataflow', v[0]);
												window.history.replaceState(
													{},
													'',
													`${location.pathname}?${params}`
												);

												setCurrent(v);
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
					<div>{current ? <View dataflow_id={current[0]} /> : null}</div>
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
	const [graph, setGraph] = useState(null);
	const [dot, setDot] = useState(null);
	const [loading, setLoading] = useState(true);
	const [error, setError] = useState(false);

	useEffect(() => {
		setLoading(true);
		setError(false);
		setGraph(null);

		const load = async () => {
			const stats_table = await query(`
				SELECT
					name, records
				FROM
					mz_catalog.mz_records_per_dataflow_global
				WHERE
					id = ${props.dataflow_id};
			`);
			const stats_row = stats_table.rows[0];
			setStats({
				name: stats_row[0],
				records: stats_row[1],
			});

			// 1) Find the address id's value for this dataflow (innermost subselect).
			// 2) Find all address ids whose first slot value is that (second innermost subselect).
			// 3) Find all address values in that set (top select).
			// DISTINCT is useful (but not necessary) because it removes the duplicates
			// caused by multiple workers.
			const addr_table = await query(`
				SELECT DISTINCT
					id, slot, value
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
								slot = 0
								AND value
									= (
											SELECT
												value
											FROM
												mz_catalog.mz_dataflow_operator_addresses
											WHERE
												id = ${props.dataflow_id}
										)
						);
			`);
			// Map from id to address (array). {320: [11], 321: [11, 1]}.
			const addrs = {};
			addr_table.rows.forEach(([id, slot, value]) => {
				if (!addrs[id]) {
					addrs[id] = [];
				}
				addrs[id][slot] = value;
			});
			setAddrs(addrs);

			const oper_table = await query(`
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
								slot = 0
								AND value
									= (
											SELECT
												value
											FROM
												mz_catalog.mz_dataflow_operator_addresses
											WHERE
												id = ${props.dataflow_id}
										)
						);
			`);
			// Map from id to operator name. {320: 'name'}.
			const opers = Object.fromEntries(oper_table.rows);
			setOpers(opers);

			const chan_table = await query(`
				SELECT DISTINCT
					id, source_node, target_node
				FROM
					mz_catalog.mz_dataflow_channels
				WHERE
					id
					IN (
							SELECT
								id
							FROM
								mz_catalog.mz_dataflow_operator_addresses
							WHERE
								slot = 0
								AND value
									= (
											SELECT
												value
											FROM
												mz_catalog.mz_dataflow_operator_addresses
											WHERE
												id = ${props.dataflow_id}
										)
						);
			`);
			// {id: [source, target]}.
			const chans = Object.fromEntries(
				chan_table.rows.map(([id, source, target]) => [id, [source, target]])
			);
			setChans(chans);

			const records_table = await query(`
				SELECT
					id, sum(records)
				FROM
					mz_catalog.mz_records_per_dataflow_operator
				WHERE
					dataflow_id = ${props.dataflow_id}
				GROUP BY
					id
			`);
			setRecords(Object.fromEntries(records_table.rows));

			setLoading(false);
		};
		load().catch((error) => {
			console.debug(typeof error, error);
			setError(JSON.stringify(error));
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
		const edges = Object.entries(chans).map(([id, [source, target]]) => {
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
			return `_${from_id} -> _${to_id};`;
		});
		const oper_labels = Object.entries(opers).map(([id, name]) => {
			if (!addrs[id].length) {
				return '';
			}
			let style = '';
			if (id in records) {
				const record_count = records[id];
				// Any operator that can have records will have a red border (even if it
				// currently has 0 records). The fill color is a deeper red based on how many
				// records this operator has compared to the operator with the most records.
				const alpha = ((record_count / max_record_count) * 0xff).toString(16);
				name += ` (${record_count} records)`;
				style = `,style=filled,color=red,fillcolor="#ff0000${alpha}"`;
			}
			return `_${id} [label="${name} (id: ${id})"${style}]`;
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

	let dotLink = null;
	if (dot) {
		const search = new URLSearchParams();
		search.set('graph', dot);
		const link = 'https://mz-graphviz.netlify.app/?' + search;
		dotLink = <a href={link}>static graph link</a>;
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
						Name: {stats.name}, dataflow_id: {props.dataflow_id}, records:{' '}
						{stats.records}
					</h3>
					<div>{dotLink}</div>
					<div dangerouslySetInnerHTML={{ __html: graph }}></div>
				</div>
			)}
		</div>
	);
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

function Table(props) {
	return (
		<table>
			<thead>
				<tr>
					{props.col_names.map((v, i) => (
						<th key={i}>{v}</th>
					))}
				</tr>
			</thead>
			<tbody>
				{props.rows.map((row, i) => (
					<tr key={i}>
						{row.map((v, i) => (
							<td key={i}>{v}</td>
						))}
					</tr>
				))}
			</tbody>
		</table>
	);
}

async function init() {
	{
		/*
		const res = await query(
			`CREATE MATERIALIZED VIEW materialized_records_per_dataflow_global AS SELECT * FROM mz_catalog.mz_records_per_dataflow_global`
		);
		*/
	}

	const content = document.getElementById('content');
	ReactDOM.render(<Views />, content);
}

init().catch(console.debug);
