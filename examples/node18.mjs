import {JamaMms5Connection} from '@openmbee/mms5-jama-sdk';

(async () => {
   const connection = await JamaMms5Connection.init({
      auth: {
         user: process.env.MMS5_USERNAME,
         pass: process.env.MMS5_PASSWORD,
      },
      endpoint: process.env.MMS5_ENDPOINT,
      root: process.env.JAMA_ROOT,
   });

   // optionally precache dependencies before iterating on requirements
   await Promise.all([
      connection.exhaust(connection.allRelations()),
      connection.exhaust(connection.allProperties()),
   ]);

   for await(const requirement of connection.allItems()) {
      const outRelations = await requirement.outgoingRelations();

      const properties = await requirement.properties();

      const propertiesSummary = properties.asArray.map(property => `${property.name}: ${property.value}`);

      console.log({
         name: requirement.itemName,
         properties: propertiesSummary,
         relations: await Promise.all(outRelations.map(async relation => {
            const [src, dst] = await Promise.all([
               relation.src(),
               relation.dst(),
            ]);

            return {
               src: src.itemName,
               dst: dst.itemName,
            };
         })),
      });
   }
})();
