import {JamaMms5Connection} from '../dist/main.mjs';

(async () => {
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
