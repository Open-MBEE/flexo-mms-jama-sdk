import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import typescript from '@rollup/plugin-typescript';
import sourcemaps from 'rollup-plugin-sourcemaps';
import copy from 'rollup-plugin-copy';
import {defineConfig} from 'rollup';
import fs from 'fs';

import G_PACKAGE_JSON from './package.json' assert {type:'json'};

const S_EXT_NODE16 = '.node16.cjs';

export default defineConfig(() => {

	const a_outputs = [
		{
			file: G_PACKAGE_JSON.main,
			format: 'cjs',
			sourcemap: true,
		},
		{
			file: G_PACKAGE_JSON.main.replace(/\.cjs$/, S_EXT_NODE16),
			format: 'cjs',
			sourcemap: true,
		},
		{
			file: G_PACKAGE_JSON.module,
			format: 'es',
			sourcemap: true,
		},
	];

	return a_outputs.map((g_output) => ({
		input: 'src/main.ts',
		output: g_output,
		watch: {
			include: 'src/**',
		},
		plugins: [
			typescript({
				...g_output.file.endsWith(S_EXT_NODE16)? {
					compilerOptions: {
						lib: ['es2021'],
						module: 'commonjs',
						target: 'es2021',
					},
				}: {},
			}),

			commonjs(),

			resolve(),

			sourcemaps(),

			copy({
				targets: [{
					src: 'src/queries/*',
					dest: 'dist/queries',
				}],
			}),
		],
	}));
});
