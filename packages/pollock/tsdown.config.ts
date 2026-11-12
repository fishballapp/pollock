import { defineConfig } from 'tsdown';

// One self-contained bin, `effect` and `@effect/platform-node` included, so `npm i -g @fsh.ba/pollock`
// installs no dependencies.
export default defineConfig({
  entry: { bin: 'src/bin.ts' },
  format: ['esm'],
  platform: 'node',
  clean: true,
  dts: false,
  deps: { alwaysBundle: [/.*/] },
});
