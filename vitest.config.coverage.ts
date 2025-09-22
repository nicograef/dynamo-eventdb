import { coverageConfigDefaults, defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['**/*.test.ts', '**/*.integration-test.ts'],
    coverage: {
      enabled: true,
      exclude: ['**/index.ts', ...coverageConfigDefaults.exclude],
      reporter: ['text'],
      thresholds: { lines: 80, functions: 80, branches: 80, statements: 80 },
    },
  },
});
