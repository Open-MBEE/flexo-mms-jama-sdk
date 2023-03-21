import {JamaMms5Connection} from '../src/main.ts';

debugger;
const k_conn = await JamaMms5Connection.init({
	auth: {
		user: Deno.env.get('MMS5_USERNAME')!,
		pass: Deno.env.get('MMS5_PASSWORD')!,
	},
	endpoint: Deno.env.get('MMS5_ENDPOINT')!,
	root: Deno.env.get('JAMA_ROOT')!,
});


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
