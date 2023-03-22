const {JamaMms5Connection} = require('../dist/main.node16.cjs');

(async () => {
   debugger;

   const k_conn = await JamaMms5Connection.init({
      auth: {
         user: process.env.MMS5_USERNAME,
         pass: process.env.MMS5_PASSWORD,
      },
      endpoint: process.env.MMS5_ENDPOINT,
      root: process.env.JAMA_ROOT,
   });

   // ...
})();
