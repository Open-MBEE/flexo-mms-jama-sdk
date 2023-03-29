import {JamaMms5Connection, User} from '../src/main.ts';

const k_conn = await JamaMms5Connection.init({
	auth: {
		user: Deno.env.get('MMS5_USERNAME')!,
		pass: Deno.env.get('MMS5_PASSWORD')!,
	},
	endpoint: Deno.env.get('MMS5_ENDPOINT')!,
	root: Deno.env.get('JAMA_ROOT_CONTEXT')!,
});

const SI_MODE = 'inspect';

if('all' === SI_MODE) {
	await k_conn.exhaust(k_conn.allItemTypes());
	await k_conn.exhaust(k_conn.allProperties());


	for await(const k_item of k_conn.allItems()) {
		const k_props = await k_item.properties();

		for(const k_prop of k_props.asArray) {
			const k_field = await k_prop.field();

			if(k_field) {
				k_field.label;  // e.g., Description, Global ID
				k_field.name;  // e.g., description, globalId
				k_field.type;  // e.g., TEXT, STRING

				if(!['STRING', 'TEXT'].includes(k_field.type)) {
					debugger;
					console.log({
						k_field,
					});
				}
			}

			k_prop.value;  // e.g., "", "01. Requirements"
		}
	}
}
else {
	// index all users by username
	const h_users = await k_conn.index(k_conn.allUsers(), k => k.username);

	const k_item = await k_conn.fetchItem('https://cae-jama.jpl.nasa.gov/rest/v1/items/1821237');

	const k_props = await k_item.properties();

	for(const k_prop of k_props.asArray) {
		const k_field = await k_prop.field();

		console.log(`${k_field?.label}: ${k_prop.value} (${k_field?.type})`);

		if(k_field?.hasPicklist) {
			const h_options = await k_conn.fetchPicklistOptions([k_prop.value]);

			console.log('\t'+Object.values(h_options).at(0)?.name);

			// debugger;
			// console.log({k_option});

			// const k_picklist = await k_field.picklist();

			// const h_options = await k_conn.index(await k_picklist.options(), k => k.id);
			// h_options[k_prop.value]
			// console.log({k_picklist});
		}

		const s_label = k_field?.label || '';

		if('Responsible Engineer (User)' === s_label) {
			console.log(h_users[k_prop.value]?.fullName || `@${k_prop.value}`);
		}
	}
}
