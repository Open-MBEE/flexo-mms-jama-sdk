import type {BindingIri, BindingLiteral, GenericBindingRow, Iri, ItemFieldPropertyRow, ItemRelationshipRow, ItemRow, ItemTypeFieldRow, PicklistOptionRow, PicklistRow} from './queries.ts';
import {UserRow} from './queries.ts';

export interface ConnectionConfig {
	auth: {
		user: string;
		pass: string;
	};
	endpoint: string | {
		host: string;
		org: string;
		repo: string;
		branch?: string;
	};
	root: string;
	debug?: {
		pagination?: boolean;
	};
}

export interface SparqlPagination {
	order: string;
	limit: number;
	offset: number;
}

type Arrayable<w_thing> = w_thing | w_thing[];

export type QueryQualifiers<
   g_row extends object=object
> = {
   [si_var in keyof g_row]?: g_row[si_var] extends infer z_binding
      ? z_binding extends BindingIri
         ? Arrayable<`<${Iri}>` | `${string}:${string}`>
         : z_binding extends BindingLiteral
            ? Arrayable<string | RegExp>
            : never
      : never;
};


const A_QUERIES = [
	'item-field-properties.rq',
	'item-relationships.rq',
	'item-type-fields.rq',
	'items.rq',
	'picklist-options.rq',
	'picklists.rq',
	'project.rq',
	'users.rq',
];

interface PreprocessConfig<g_row extends object=object> {
	query: QueryQualifiers<g_row>;
}

enum ItemVisitation {
	NONE = 0b00,
	INCOMING = 0b01,
	OUTGOING = 0b10,
	BOTH = 0b11,
}

type PropertiesMap = Record<Iri, Array<BindingIri | BindingLiteral>>;

export const __dirname = new URL('.', import.meta.url).pathname.replace(/\/$/, '');

// prep static prefixes
const H_PREFIXES = {
	rdf: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
	xsd: 'http://www.w3.org/2001/XMLSchema#',
	oge: 'https://openmbee.org/openapi-graph-extractor#',
	hint: 'http://aws.amazon.com/neptune/vocab/v01/QueryHints#',
};


function transform_query_primitive(si_var: string, z_primitive: string | RegExp): string {
	if('string' === typeof z_primitive) {
		return `?${si_var} = ${z_primitive}`;
	}
	else if(z_primitive instanceof RegExp) {
		return `regex(?${si_var}, "${z_primitive.source.replace(/"/g, '\\"').replace(/\\/g, '\\\\')}", "${z_primitive.flags}")`;
	}

	throw new TypeError(`Invalid query primitive: ${typeof z_primitive} (${z_primitive})`);
}

export class JamaMms5Connection {
	protected _p_endpoint: string;
	protected _p_root: string;
	protected _s_user: string;
	protected _s_pass: string;
	protected _sx_bearer = '';
	protected _sq_prefixes = '';
	protected _b_debug_pagination = false;

	protected _h_queries: Record<string, string> = {};

	// cache state
	protected _b_all_items = false;
	protected _b_all_relations = false;
	protected _b_all_picklists = false;
	protected _b_all_options = false;

	protected _h_items: Record<Iri, Item> = {};
	protected _h_type_maps: Record<Iri, Record<Iri, ItemTypeFieldRow>> = {};
	protected _h_types: Record<Iri, ItemType> = {};
	protected _h_property_sets: Record<Iri, PropertiesMap> = {};
	protected _h_users: Record<Iri, User> = {};

	protected _h_relations: Record<Iri, Relation> = {};
	protected _h_outgoing: Record<Iri, Record<Iri, Relation>> = {};
	protected _h_incoming: Record<Iri, Record<Iri, Relation>> = {};
	protected _h_visited: Record<Iri, ItemVisitation> = {};

	protected _h_picklists: Record<Iri, Picklist> = {};
	protected _h_options: Record<Iri, PicklistOption> = {};
	protected _h_options_for_picklist: Record<Iri, Iri[]> = {};

	/**
	 * Creates a new {@link JamaMms5Connection} and initializes it
	 * @param gc_conn
	 * @param f_loader - an asynchronous callback that handles reading the given path (relative to project root) from disk
	 * @returns the new {@link JamaMms5Connection}
	 */
	static async init(gc_conn: ConnectionConfig, f_loader?: (p_file: string) => Promise<string>): Promise<JamaMms5Connection> {
		const k_new = new JamaMms5Connection(gc_conn);

		if(!f_loader) {
			// deno
			if('undefined' !== typeof globalThis['Deno']) {
				f_loader = sr => Deno.readTextFile(`${__dirname}/${sr}`);
			}
			// node
			else if('undefined' !== typeof process) {
				// cjs require'able
				if('function' === typeof require) {
					const {promises:{readFile}} = require('fs');

					f_loader = sr => readFile(`${__dirname}/${sr}`, 'utf-8');
				}
				// esm only
				else {
					const {promises:{readFile}} = await import('fs');

					f_loader = sr => readFile(`${__dirname}/${sr}`, 'utf-8');
				}
			}
			else {
				throw new TypeError(`Missing required loader argument for current environment`);
			}
		}

		const a_loaded = await Promise.all(A_QUERIES.map(sr => f_loader!(`queries/${sr}`)));

		for(let i_query=0; i_query<a_loaded.length; i_query++) {
			k_new._h_queries[A_QUERIES[i_query]] = a_loaded[i_query];
		}

		return k_new;
	}

	protected constructor(gc_conn: ConnectionConfig) {
		this._p_endpoint = 'string' === typeof gc_conn.endpoint
			? gc_conn.endpoint
			: `https://${gc_conn.endpoint.host}/orgs/${gc_conn.endpoint.org}/repos/${gc_conn.endpoint.repo}/branches/${gc_conn.endpoint.branch}/query`;

		this._s_user = gc_conn.auth.user;
		this._s_pass = gc_conn.auth.pass;

		if(gc_conn.debug?.pagination) this._b_debug_pagination = true;

		const p_root = this._p_root = gc_conn.root;

		// normalize prefixes string
		this._sq_prefixes = Object.entries({
			...H_PREFIXES,
			'': p_root,
			def: `${p_root}/definitions/`,
		}).map(([si_prefix, p_iri]) => `prefix ${si_prefix}: <${p_iri}>`).join('\n');
	}

	get root(): string {
		return this._p_root;
	}

	protected _query(si_file: string, gc_preprocess?: PreprocessConfig) {
		// query string
		let sq_query = this._h_queries[si_file];

		// query preprocessing
		if(gc_preprocess?.query) {
			// prep values/filters string
			let sx_values_filters = '';
			for(const [si_variable, z_values] of Object.entries(gc_preprocess.query)) {
				// coerce into array
				const a_values: Array<string | RegExp> = Array.isArray(z_values)? z_values: [z_values];

				// all strings; use values
				if(a_values.every(w => 'string' === typeof w)) {
					sx_values_filters += `values ?${si_variable} { ${a_values.join(' ')} } `;
				}
				// mixed; use filter(s)
				else {
					const a_filters = a_values.map((w_value) => transform_query_primitive(si_variable, w_value));
					sx_values_filters += `filter(${a_filters.join(' || ')})`;
				}
			}

			// replace values
			sq_query = sq_query.replace(/#\s*@MARK:VALUES/, sx_values_filters);
		}

		// prepend prefixes
		return this._sq_prefixes+'\n'+sq_query;
	}

	protected async *_exec<RowType=GenericBindingRow>(sq_input: string, gc_pagination?: SparqlPagination, b_nested=false): AsyncIterableIterator<RowType[]> {
		let sq_exec = sq_input;
		if(gc_pagination) {
			sq_exec += ` order by ?${gc_pagination.order} limit ${gc_pagination.limit} offset ${gc_pagination.offset}`;
		}

		// submit request
		const d_res = await fetch(this._p_endpoint, {
			method: 'POST',
			headers: {
				'accept': 'application/sparql-results+json',
				'content-type': 'application/sparql-query',
				'accept-encoding': 'gzip',
				...this._sx_bearer? {
					'authorization': `Bearer ${this._sx_bearer}`,
				}: {},
			},
			body: sq_exec,
		});

		// read response text
		const s_text = await d_res.text();

		// assert status
		if(!d_res.ok) {
			// auth error
			if(401 === d_res.status && !this._sx_bearer) {
				const {_s_user, _s_pass} = this;

				if(!_s_user || !_s_pass) {
					throw new Error(`Missing MMS5 auth credentials`);
				}

				// login
				const d_auth = await fetch((new URL(this._p_endpoint)).origin+'/login', {
					headers: {
						'Authorization': `Basic ${btoa(`${_s_user}:${_s_pass}`)}`,
						'Accept': 'application/json',
					},
				});

				// not ok
				if(!d_auth.ok) {
					throw new Error(`${d_auth.status} ${await d_auth.text()}`);
				}

				// parse response json
				const g_auth = await d_auth.json();

				// save bearer token
				this._sx_bearer = g_auth.token;

				// retry
				yield* await this._exec(sq_input, gc_pagination, b_nested);

				// done
				return;
			}

			// unexpected HTTP error
			throw new Error(`MMS5 SPARQL Endpoint returned an HTTP error status: ${d_res.status}\n${s_text}`);
		}

		// parse results
		const a_bindings = JSON.parse(s_text).results.bindings as RowType[];

		// yield
		yield a_bindings;

		// pagination is being used
		if(gc_pagination) {
			if(this._b_debug_pagination) {
				console.debug(`Processed paginated rows ${gc_pagination.offset} - ${gc_pagination.offset + a_bindings.length}`);
			}

			// not in a nested context
			if(!b_nested) {
				// prepare to repeat limit/offset requests at 1 level call stack depth
				let a_next = a_bindings;

				// track offset
				let i_offset = gc_pagination.offset + a_next.length;

				// possibly more results
				while(a_next.length === gc_pagination.limit) {
					// fetch next batch of results
					for await(const a_batch of this._exec<RowType>(sq_input, {
						...gc_pagination,
						offset: i_offset,
					}, true)) {
						// yield to top caller
						yield a_batch;

						// there will only be exactly one yield from the above call
						a_next = a_batch;

						// advance offset
						i_offset += a_next.length;
					}
				}
			}
		}
	}


	/**
	 * Exhausts the given async iterator for side-effects (i.e., caching)
	 * @param di_iter - the async iterator
	 */
	async exhaust(di_iter: AsyncIterableIterator<unknown>): Promise<void> {
		for await(const _w of di_iter) { /**/ }
	}


	/**
	 * Similar to exhaust but collects the iterator's output to a list
	 * @param di_iter - the async iterator
	 * @returns the collection
	 */
	async collect<w_collect>(di_iter: AsyncIterableIterator<w_collect>): Promise<w_collect[]> {
		const a_collect: w_collect[] = [];

		for await(const _w of di_iter) {
			a_collect.push(_w as w_collect);
		}

		return a_collect;
	}

	
	/**
	 * Similar to exhaust but collects the iterator's output to a list
	 * @param di_iter - the async iterator
	 * @returns the collection
	 */
	async index<w_yield>(di_iter: AsyncIterableIterator<w_yield> | Iterable<w_yield>, f_by: (k: w_yield) => string): Promise<Record<string, w_yield>> {
		const h_index: Record<string, w_yield> = Object.create(null);

		for await(const w_each of di_iter) {
			h_index[f_by(w_each)] = w_each;
		}

		return h_index;
	}


	/**
	 * Start fetching all items with the given pagination limit
	 * @param n_pagination - number of rows to limit each query
	 * @yields an {@link Item} one at a time
	 */
	async *allItems(n_pagination=1000): AsyncIterableIterator<Item> {
		const {_h_items} = this;

		// paginated batch querying
		for await(const a_rows of this._exec<ItemRow>(this._query('items.rq'), {
			order: 'item',
			limit: n_pagination,
			offset: 0,
		})) {
			const a_yields: Item[] = [];

			// first, cache all items returned in this query
			for(const g_row of a_rows) {
				a_yields.push(_h_items[g_row.item.value] = new Item(g_row, this));
			}

			// then, yield each one
			yield* a_yields;
		}

		// all items have been cached
		this._b_all_items = true;
	}


	/**
	 * Attempt to find a specific item by its IRI, fetching from MMS5 if not yet cached
	 * @param p_item - IRI of the item to fetch
	 * @returns the {@link Item}
	 */
	async fetchItem(p_item: Iri): Promise<Item> {
		const {_h_items} = this;

		// cache hit; return
		if(_h_items[p_item]) return _h_items[p_item];

		// fetch via query
		for await(const k_item of this.queryItems({
			item: `<${p_item}>`,
		}, 1)) {
			return k_item;
		}

		// item not found
		throw new Error(`Item is missing from dataset: <${p_item}>`);
	}


	/**
	 * Query for items
	 */
	async *queryItems(h_query: QueryQualifiers<ItemRow>, n_pagination=1000): AsyncIterableIterator<Item> {		
		const {_h_items} = this;

		// fetch via query
		for await(const a_rows of this._exec<ItemRow>(this._query('items.rq', {
			query: h_query,
		}), {
			order: 'item',
			limit: n_pagination,
			offset: 0,
		})) {
			// each row
			for(const g_row of a_rows) {
				const p_item = g_row.item.value;

				// not yet cached; cache
				if(!_h_items[p_item]) _h_items[p_item] = new Item(g_row, this);
				
				// yield
				yield _h_items[p_item]
			}
		}
	}


	protected _process_type_rows(a_rows: ItemTypeFieldRow[]) {
		const {_h_type_maps} = this;

		// first, cache all items returned in this query
		for(const g_row of a_rows) {
			const p_item_type = g_row.itemType.value;

			// build fields table for item type
			Object.assign(_h_type_maps[p_item_type] = _h_type_maps[p_item_type] || {}, {
				[g_row.field.value]: g_row,
			});
		}
	}


	/**
	 * Start fetching all item types with the given pagination limit
	 * @param n_pagination - number of rows to limit each query
	 * @yields an {@link ItemType} one at a time
	 */
	async *allItemTypes(n_pagination=1000): AsyncIterableIterator<ItemType> {
		const {_h_types, _h_type_maps} = this;

		// paginated batch querying
		for await(const a_rows of this._exec<ItemTypeFieldRow>(this._query('item-type-fields.rq'), {
			order: 'itemType',
			limit: n_pagination,
			offset: 0,
		})) {
			this._process_type_rows(a_rows);

			// then, yield each one
			for(const p_item_type in _h_type_maps) {
				yield _h_types[p_item_type] = new ItemType(p_item_type as Iri, _h_type_maps[p_item_type], this);
			}
		}
	}


	/**
	 * Attempt to find a specific item type by its IRI, fetching from MMS5 if not yet cached
	 * @param p_item_type - IRI of the item type to fetch
	 * @returns the {@link ItemType}
	 */
	async fetchItemType(p_item_type: Iri): Promise<ItemType> {
		const {_h_types} = this;

		// type already cached
		const k_type = _h_types[p_item_type];
		if(k_type) return k_type;

		// single-batch mode
		for await(const a_rows of this._exec<ItemTypeFieldRow>(this._query('item-type-fields.rq', {
			query: {
				itemType: [`<${p_item_type}>`],
			},
		}))) {
			this._process_type_rows(a_rows);
		}

		// create, cache and return instance
		return _h_types[p_item_type] = new ItemType(p_item_type, this._h_type_maps[p_item_type], this);
	}


	protected _new_relation(g_row: ItemRelationshipRow): Relation {
		const {_h_relations, _h_incoming, _h_outgoing} = this;

		// create and cache relation
		const k_relation = _h_relations[g_row.relation.value] = new Relation(g_row, this);

		// create bi-directional associations
		const p_dst = k_relation.raw.dst.value;
		const p_src = k_relation.raw.src.value
		Object.assign(_h_incoming[p_dst] = _h_incoming[p_dst] || {}, {[k_relation.iri]: k_relation});
		Object.assign(_h_outgoing[p_src] = _h_outgoing[p_src] || {}, {[k_relation.iri]: k_relation});

		// return relation
		return k_relation;
	}


	/**
	 * Start fetching all relations with the given pagination limit
	 * @param n_pagination - number of rows to limit each query
	 * @yields a {@link Relation} one at a time
	 */
	async *allRelations(n_pagination=1000) {
		// paginated batch querying
		for await(const a_rows of this._exec<ItemRelationshipRow>(this._query('item-relationships.rq'), {
			order: 'relation',
			limit: n_pagination,
			offset: 0,
		})) {
			const a_yields: Relation[] = [];

			// first, cache all relations returned in this query
			for(const g_row of a_rows) {
				a_yields.push(this._new_relation(g_row));
			}

			// then, yield each one
			yield* a_yields;
		}

		// all relations have been cached
		this._b_all_relations = true;
	}


	/**
	 * Attempt to find a specific relation by its IRI, fetching from MMS5 if not yet cached
	 * @param p_relation - IRI of the relation to fetch
	 * @returns the {@link Relation}
	 */
	async fetchRelation(p_relation: Iri) {
		const {_h_relations} = this;

		// relation already cached
		const k_type = _h_relations[p_relation];
		if(k_type) return k_type;

		// single-batch mode
		for await(const a_rows of this._exec<ItemRelationshipRow>(this._query('item-relationships.rq', {
			query: {
				itemType: [`<${p_relation}>`],
			},
		}))) {
			// the first row; create, cache and return new relation
			for(const g_row of a_rows) {
				return this._new_relation(g_row);
			}
		}

		throw new Error(`Relation not found: <${p_relation}>`);
	}


	/**
	 * Query for relations by src and dst item IRIs
	 * @param p_src - IRI of src Item
	 * @param p_dst - IRI of dst Item
	 * @returns Array of {@link Relation}s
	 */
	async queryRelations(p_src: Iri | null, p_dst: Iri | null): Promise<Relation[]> {
		const {_h_relations, _h_incoming, _h_outgoing, _h_visited} = this;

		// prep relations results
		const a_relations: Relation[] = [];

		// track visitations
		let xc_cached = ItemVisitation.NONE;

		// outgoing is fully cached
		if(p_src && (this._b_all_relations || _h_visited[p_src] & ItemVisitation.OUTGOING)) {
			// mark cache hit
			xc_cached |= ItemVisitation.OUTGOING;

			// add to results
			a_relations.push(...Object.values(_h_outgoing[p_src]));
		}

		// incoming is fully cached
		if(p_dst && (this._b_all_relations || _h_visited[p_dst] & ItemVisitation.INCOMING)) {
			// mark cache hit
			xc_cached |= ItemVisitation.INCOMING;

			// add to results
			a_relations.push(...Object.values(_h_incoming[p_dst]));
		}

		// full cache hit
		if(xc_cached === ItemVisitation.BOTH) {
			return a_relations;
		}


		// query with values
		for await(const a_rows of this._exec<ItemRelationshipRow>(this._query('item-relationships.rq', {
			query: {
				...p_src && !(xc_cached & ItemVisitation.OUTGOING)? {
					src: [`<${p_src}>`],
				}: {},
				...p_dst && !(xc_cached & ItemVisitation.INCOMING)? {
					dst: [`<${p_dst}>`],
				}: {},
			},
		}))) {
			// each relation
			for(const g_row of a_rows) {
				const p_relation = g_row.relation.value;

				// relation already cached
				const k_relation = _h_relations[p_relation];
				if(k_relation) {
					a_relations.push(k_relation);
				}
				// new relation; create and cache
				else {
					a_relations.push(this._new_relation(g_row));
				}
			}
		}

		// mark as cached
		if(p_src) _h_visited[p_src] = ItemVisitation.OUTGOING;
		if(p_dst) _h_visited[p_dst] = ItemVisitation.INCOMING;

		// return results
		return a_relations;
	}


	/**
	 * Attempt to find a picklist by its IRI, fetching from MMS5 if not yet cached
	 * @param p_picklist - IRI of the picklist
	 * @returns array of {@link PicklistOption}s
	 */
	async *allPicklists(n_pagination=1000): AsyncIterableIterator<Picklist> {
		const {_h_picklists} = this;

		// paginated batch querying
		for await(const a_rows of this._exec<PicklistRow>(this._query('picklists.rq'), {
			order: 'picklist',
			limit: n_pagination,
			offset: 0,
		})) {
			const a_yields: Picklist[] = [];

			// first, cache all picklists returned in this query
			for(const g_row of a_rows) {
				a_yields.push(_h_picklists[g_row.picklist.value] = new Picklist(g_row, this));
			}

			// then, yield each one
			yield* a_yields;
		}

		// all picklists have been cached
		this._b_all_picklists = true;
	}


	/**
	 * Attempt to find a picklist by its IRI, fetching from MMS5 if not yet cached
	 * @param p_picklist - IRI of the picklist
	 * @returns array of {@link PicklistOption}s
	 */
	async fetchPicklist(p_picklist: Iri): Promise<Picklist> {
		const {_h_picklists} = this;

		// picklist already cached
		const k_picklist = _h_picklists[p_picklist];
		if(k_picklist) return k_picklist;

		// single-batch mode
		for await(const a_rows of this._exec<PicklistRow>(this._query('picklists.rq', {
			query: {
				picklist: [`<${p_picklist}>`],
			},
		}))) {
			// the first row; create, cache and return new relation
			for(const g_row of a_rows) {
				return _h_picklists[g_row.picklist.value] = new Picklist(g_row, this);
			}
		}

		throw new Error(`Picklist not found: <${p_picklist}>`);
	}

	/**
	 * Start fetching all picklist options
	 * @param n_pagination - pagination limit
	 * @yields a {@link PicklistOption} one at a time
	 */
	async *allPicklistOptions(n_pagination=1000): AsyncIterableIterator<PicklistOption> {
		const {_h_options} = this;

		// paginated batch querying
		for await(const a_rows of this._exec<PicklistOptionRow>(this._query('picklist-options.rq'), {
			order: 'option',
			limit: n_pagination,
			offset: 0,
		})) {
			const a_yields: PicklistOption[] = [];

			// first, cache all picklist options returned in this query
			for(const g_row of a_rows) {
				a_yields.push(_h_options[g_row.option.value] = new PicklistOption(g_row, this));
			}

			// then, yield each one
			yield* a_yields;
		}

		// all picklist options have been cached
		this._b_all_options = true;
	}



	async fetchPicklistOptions(a_options: Iri[]): Promise<Record<Iri, PicklistOption>> {
		const {_h_options} = this;

		// result map
		const h_results: Record<Iri, PicklistOption> = {};

		// prep list of options to fetch
		const a_fetch: Iri[] = [];

		// each option IRI
		for(const p_option of a_options) {
			const k_option = _h_options[p_option];

			// option already cached
			if(k_option) {
				h_results[p_option] = k_option;
			}
			// need to fetch
			else {
				a_fetch.push(p_option);
			}
		}

		// something(s) to fetch
		if(a_fetch.length) {
			// single-batch mode
			for await(const a_rows of this._exec<PicklistOptionRow>(this._query('picklist-options.rq', {
				query: {
					option: a_options.map(p => `<${p}>`),
				},
			}))) {
				// each option
				for(const g_row of a_rows) {
					const p_option = g_row.option.value;

					// create, cache and new picklist option to results
					h_results[p_option] = _h_options[p_option] = new PicklistOption(g_row, this);
				}
			}
		}

		// return options map
		return h_results;
	}


	/**
	 * Attempt to find all options for a given picklist by its IRI, fetching from MMS5 if not yet cached
	 * @param p_picklist - IRI of the picklist
	 * @returns array of {@link PicklistOption}s
	 */
	async fetchPicklistOptionsFor(p_picklist: Iri): Promise<PicklistOption[]> {
		const {_h_options, _h_options_for_picklist} = this;

		// option iris are cached
		let a_option_iris = _h_options_for_picklist[p_picklist];
		if(a_option_iris) {
			// resolve and return options
			return Object.values(await this.fetchPicklistOptions(a_option_iris));
		}

		// prep iri list
		a_option_iris = [];

		// prep new options list
		const a_options: PicklistOption[] = [];

		// single-batch mode
		for await(const a_rows of this._exec<PicklistOptionRow>(this._query('picklist-options.rq', {
			query: {
				picklist: [`<${p_picklist}>`],
			},
		}))) {
			// each option
			for(const g_row of a_rows) {
				const p_option = g_row.option.value;

				// option not yet cached; create and cache
				let k_option = _h_options[p_option];
				if(!k_option) {
					k_option = _h_options[p_option] = new PicklistOption(g_row, this);
				}

				// queue iri to be cached
				a_option_iris.push(p_option);

				// add option to results list
				a_options.push(k_option);
			}
		}

		// save association to cache
		_h_options_for_picklist[p_picklist] = a_option_iris;

		// return options list
		return a_options;
	}


	/**
	 * Start fetching all items with the given pagination limit
	 * @param n_pagination - number of rows to limit each query
	 * @yields an {@link Item} one at a time
	 */
	async *allProperties(n_pagination=1000): AsyncIterableIterator<Record<Iri, PropertiesMap>> {
		const {_h_property_sets} = this;

		// local item and properties
		let p_item_local = '' as Iri;
		let h_properties_local: PropertiesMap = {};

		// paginated batch querying
		for await(const a_rows of this._exec<ItemFieldPropertyRow>(this._query('item-field-properties.rq'), {
			order: 'item',
			limit: n_pagination,
			offset: 0,
		})) {
			// each property row
			for(const g_row of a_rows) {
				const p_property = g_row.property.value;
				const p_item = g_row.item.value;

				// different item means previous can be cached (since ordered by ?item)
				if(p_item !== p_item_local && p_item_local) {
					// save to cache and yield
					yield {
						[p_item_local]: _h_property_sets[p_item_local] = h_properties_local,
					};

					// reset local properties map
					h_properties_local = {};
				}

				// advance local pointer
				p_item_local = p_item;

				// save property to local properties map
				(h_properties_local[p_property] = h_properties_local[p_property] || []).push(g_row.value);
			}
		}

		// cache and yield final
		yield {
			[p_item_local]: _h_property_sets[p_item_local] = h_properties_local,
		};
	}


	/**
	 * Attempt to find all properties for a given item by its IRI, fetching from MMS5 if not yet cached
	 * @param p_item - IRI of the item
	 * @returns 
	 */
	async fetchPropertiesFor(p_item: Iri): Promise<PropertiesMap> {
		const {_h_property_sets} = this;
		
		// property set is already cached
		let h_properties = _h_property_sets[p_item];
		if(h_properties) return h_properties;

		// init property set
		h_properties = {};

		// single-batch mode
		for await(const a_rows of this._exec<ItemFieldPropertyRow>(this._query('item-field-properties.rq', {
			query: {
				item: [`<${p_item}>`],
			},
		}))) {
			// each property
			for(const g_row of a_rows) {
				const p_property = g_row.property.value;

				// add value to mapping set
				(h_properties[p_property] = h_properties[p_property] || []).push(g_row.value);
			}
		}

		// cache and return
		return _h_property_sets[p_item] = h_properties;
	}


	/**
	 * Start fetching all users with the given pagination limit
	 * @param n_pagination - number of rows to limit each query
	 * @yields a {@link User} one at a time
	 */
	async *allUsers(n_pagination=1000): AsyncIterableIterator<User> {
		const {_h_users} = this;

		// paginated batch querying
		for await(const a_rows of this._exec<UserRow>(this._query('users.rq'), {
			order: 'user',
			limit: n_pagination,
			offset: 0,
		})) {
			const a_yields: User[] = [];

			// first, cache all users returned in this query
			for(const g_row of a_rows) {
				a_yields.push(_h_users[g_row.user.value] = new User(g_row, this));
			}

			// then, yield each one
			yield* a_yields;
		}
	}


	/**
	 * Attempt to find a specific user by its IRI, fetching from MMS5 if not yet cached
	 * @param p_item - IRI of the user to fetch
	 * @returns the {@link User}
	 */
	async fetchUser(p_user: Iri): Promise<User> {
		const {_h_users} = this;

		// cache hit; return
		if(_h_users[p_user]) return _h_users[p_user];

		// fetch via query
		for await(const a_rows of this._exec<UserRow>(this._query('items.rq', {
			query: {
				user: [`<${p_user}>`],
			},
		}))) {
			// user not found
			if(!a_rows.length) throw new Error(`User is missing from dataset: <${p_user}>`);
	
			// cache and return
			return _h_users[p_user] = new User(a_rows[0], this);
		}

		throw new Error(`Critical item query failure`);
	}


	/**
	 * Query for users by username
	 * @param h_values - the values to use for querying
	 * @returns the first matching {@link User} or `null` if not found
	 */
	async queryUsers(h_query: QueryQualifiers<UserRow>): Promise<User[]> {
		const {_h_users} = this;

		// prep results
		const a_users: User[] = [];

		// query with values
		for await(const a_rows of this._exec<UserRow>(this._query('users.rq', {
			query: h_query,
		}))) {
			// each user
			for(const g_row of a_rows) {
				const p_user = g_row.user.value;

				// user already cached
				const k_user = _h_users[p_user];
				if(k_user) {
					a_users.push(k_user);
				}
				// new user; create and cache
				else {
					a_users.push(new User(g_row, this));
				}
			}
		}

		// return results
		return a_users;
	}

}

export class Resource {
	protected constructor(protected _p_iri: Iri, protected _k_connection: JamaMms5Connection) {}

	get iri(): Iri {
		return this._p_iri;
	}

	get suffix(): string {
		return this._p_iri.slice(this._k_connection.root.length-1);
	}

	get connection(): JamaMms5Connection {
		return this._k_connection;
	}
}

export class Item extends Resource {
	constructor(protected _g_row: ItemRow, _k_conn: JamaMms5Connection) {
		super(_g_row.item.value, _k_conn);
	}

	get raw(): ItemRow {
		return this._g_row;
	}

	get hasParent(): boolean {
		return !!this._g_row.parent?.value;
	}

	async parent(): Promise<Item> {
		if(!this.hasParent) throw new Error(`Must check '.hasParent' before calling '.parent()'`);

		// fetch parent item
		return await this._k_connection.fetchItem(this._g_row.parent!.value);
	}

	get itemTypeDisplay(): string {
		return this._g_row.itemTypeDisplay.value;
	}

	get itemKey(): string {
		return this._g_row.itemKey.value;
	}

	get itemName(): string {
		return this._g_row.itemName.value;
	}

	get createdDate(): Date {
		return new Date(this._g_row.createdDate.value);
	}

	get modifiedDate(): Date {
		return new Date(this._g_row.modifiedDate.value);
	}

	get lastActivityDate(): Date {
		return new Date(this._g_row.lastActivityDate.value);
	}

	async itemType(): Promise<ItemType> {
		return this._k_connection.fetchItemType(this._g_row.itemType.value);
	}

	// async fields(): Promise<ItemTypeField> {
	// 	this._k_connection.fetchItemTypeField(this._g_row.itemType);
	// }

	outgoingRelations(): Promise<Relation[]> {
		return this._k_connection.queryRelations(this.iri, null);
	}

	incomingRelations(): Promise<Relation[]> {
		return this._k_connection.queryRelations(null, this.iri);
	}

	async properties(): Promise<Properties> {
		const h_properties = await this._k_connection.fetchPropertiesFor(this.iri);

		return new Properties(h_properties, this);
	}
}

export class ItemType extends Resource {
	protected _h_fields: Record<Iri, ItemTypeField> = {};

	constructor(p_item_type: Iri, protected _h_rows: Record<Iri, ItemTypeFieldRow>, _k_conn: JamaMms5Connection) {
		super(p_item_type, _k_conn);
	}

	get raw(): Record<string, ItemTypeFieldRow> {
		return this._h_rows;
	}

	fieldByIri(p_field: Iri): ItemTypeField {
		const {_h_fields} = this;

		// already cached
		const k_field = _h_fields[p_field];
		if(k_field) return k_field;

		// not found in map
		const g_field = this._h_rows[p_field];
		if(!g_field) throw new Error(`ItemType <${this.iri}> has no such field <${p_field}>`);
	
		// create, cache and return
		return _h_fields[p_field] = new ItemTypeField(g_field, this, this._k_connection);
	}

	fields(): [Iri, ItemTypeField][] {
		return Object.keys(this._h_rows).map(p => [p as Iri, this.fieldByIri(p as Iri)]);
	}

	fieldByName(s_name: string): ItemTypeField | null {
		for(const [p_field, g_row] of Object.entries(this._h_rows)) {
			if(s_name === g_row.fieldName.value) {
				return this.fieldByIri(p_field as Iri);
			}
		}

		return null;
	}
}

type JamaFieldTypeRegistry = {
	BOOLEAN: {};
	INTEGER: {};
	STRING: {};
	TEXT: {};
	LOOKUP: {};
	MULTI_LOOKUP: {};
	DATE: {};
	USER: {};
}

export type JamaFieldType = keyof JamaFieldTypeRegistry;

export class ItemTypeField extends Resource {
	constructor(protected _g_row: ItemTypeFieldRow, protected _k_item_type: ItemType, _k_conn: JamaMms5Connection) {
		super(_g_row.field.value, _k_conn);
	}

	get itemType(): ItemType {
		return this._k_item_type;
	}

	get type(): JamaFieldType {
		return this._g_row.fieldType.value as JamaFieldType;
	}

	get name(): string {
		return this._g_row.fieldName.value;
	}

	get label(): string {
		return this._g_row.fieldLabel.value;
	}

	get hasPicklist(): boolean {
		return !!this._g_row.picklistId;
	}

	async picklist(): Promise<Picklist> {
		// assert picklist presence
		if(!this.hasPicklist) throw new Error(`Must check '.hasPicklist' before calling '.picklist()'`);

		// ref picklist id
		const si_picklist = this._g_row.picklistId!.value;

		// construct picklist IRI
		const p_picklist = this._k_connection.root.replace(/#$/, `/picklists/${si_picklist}`) as Iri;

		// fetch picklist
		return this._k_connection.fetchPicklist(p_picklist);
	}
}

export class Relation extends Resource {
	constructor(protected _g_row: ItemRelationshipRow, _k_conn: JamaMms5Connection) {
		super(_g_row.relation.value, _k_conn);
	}

	get raw(): ItemRelationshipRow {
		return this._g_row;
	}

	get name(): string {
		return this._g_row.relationName.value;
	}

	async src(): Promise<Item> {
		return this._k_connection.fetchItem(this._g_row.src.value);
	}

	async dst(): Promise<Item> {
		return this._k_connection.fetchItem(this._g_row.dst.value);
	}
}


export class Picklist extends Resource {
	constructor(protected _g_row: PicklistRow, _k_conn: JamaMms5Connection) {
		super(_g_row.picklist.value, _k_conn);
	}

	get raw(): PicklistRow {
		return this._g_row;
	}

	get id(): string {
		return this._g_row.picklistId.value;
	}

	get name(): string {
		return this._g_row.picklistName.value;
	}

	get description(): string {
		return this._g_row.picklistDescription.value;
	}

	async options(): Promise<PicklistOption[]> {
		return this._k_connection.fetchPicklistOptionsFor(this.iri);
	}
}

export class PicklistOption extends Resource {
	constructor(protected _g_row: PicklistOptionRow, _k_conn: JamaMms5Connection) {
		super(_g_row.option.value, _k_conn);
	}

	get id(): string {
		return this._g_row.optionId.value;
	}

	get name(): string {
		return this._g_row.optionName.value;
	}

	get description(): string {
		return this._g_row.optionaDescription.value;
	}

	get value(): string {
		return this._g_row.optionValue.value;
	}
}

export class Properties {
	constructor(protected _h_properties: PropertiesMap, protected _k_item: Item) {}

	protected get _s_root(): string {
		return this._k_item.connection.root
	}

	get item(): Item {
		return this._k_item;
	}

	get raw(): PropertiesMap {
		return this._h_properties;
	}

	/**
	 * Returns the property set as an array
	 */
	get asArray(): Property[] {
		return Object.entries(this._h_properties).map(([p, a]) => new Property(p as Iri, a, this));
	}

	/**
	 * Access the property identified by its name
	 * @param si_property - property name
	 * @returns 
	 */
	accessByName(si_property: string): Property | null {
		const p_property = this._s_root+si_property as Iri;

		if(!this._h_properties[p_property]) return null;
		
		return new Property(p_property, this._h_properties[p_property], this);
	}

	/**
	 * Attempts to find a property by label as a case-sensitive string or regex
	 * @param z_label - the label to find
	 * @returns 
	 */
	async findByLabel(z_label: string | RegExp): Promise<Property | null> {
		for(const k_property of this.asArray) {
			const s_label = (await k_property.field())?.label || '';

			if('string' === typeof z_label? z_label === s_label: z_label.test(s_label)) {
				return k_property;
			}
		}

		return null;
	}
}

export class Property extends Resource {
	constructor(p_property: Iri, protected _a_values: Array<BindingIri | BindingLiteral>, protected _k_props: Properties) {
		super(p_property, _k_props.item.connection);
	}

	get name(): string {
		return this._p_iri.slice(this._k_connection.root.length);
	}

	async field(): Promise<ItemTypeField | null> {
		const k_type = await this._k_props.item.itemType();

		return await k_type?.fieldByName(this.name) || null;
	}

	get isEmpty(): boolean {
		return 0 === this._a_values.length;
	}

	get isSingular(): boolean {
		return 1 === this._a_values.length;
	}

	get isMulti(): boolean {
		return this._a_values.length > 1;
	}

	get values(): string[] {
		return this._a_values.map(g => g.value);
	}

	get value(): string {
		if(1 !== this._a_values.length) {
			throw new Error(`Cannot access '.value' of multi-valued property`);
		}

		return this._a_values[0].value;
	}
}

export class User extends Resource {
	constructor(protected _g_row: UserRow, _k_conn: JamaMms5Connection) {
		super(_g_row.user.value, _k_conn);
	}

	get username(): string {
		return this._g_row.username.value;
	}

	get firstName(): string {
		return this._g_row.firstName.value;
	}

	get lastName(): string {
		return this._g_row.lastName.value;
	}

	get fullName(): string {
		return this.firstName+' '+this.lastName;
	}

	get email(): string {
		return this._g_row.email?.value || '';
	}
}
