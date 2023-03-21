import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import typescript from '@rollup/plugin-typescript';
import sourcemaps from 'rollup-plugin-sourcemaps';

import G_PACKAGE_JSON from './package.json' assert {type:'json'};

export default {
   input: 'src/main.ts',
   output: [
      {
         file: G_PACKAGE_JSON.main,
         format: 'commonjs',
         sourcemap: true,
      },
      {
         file: G_PACKAGE_JSON.module,
         format: 'es',
         sourcemap: true,
      },
   ],
   watch: {
      include: 'src/**',
   },
   plugins: [
      typescript(),

      commonjs(),

      resolve(),

      sourcemaps(),
   ],
};
