import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'node:path';

// The output directory is intentionally inside target/ so the maven-jar-plugin picks it up as a
// classpath resource. StaticUiHandler in candybox-admin-api looks for `ui/index.html` etc., so we
// emit under `target/classes/ui` — matching that contract is the only thing the build wiring cares
// about.
export default defineConfig({
  plugins: [react()],
  // The SPA is served under /ui/ in production (admin API's StaticUiHandler), and dev mode also
  // uses /ui/ so dev↔prod URL handling matches — keeps React Router's basename consistent and
  // hot-reload links unchanged across modes.
  base: '/ui/',
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src'),
    },
  },
  build: {
    outDir: path.resolve(__dirname, 'target/classes/ui'),
    emptyOutDir: true,
    sourcemap: true,
  },
  server: {
    port: 5173,
    proxy: {
      // dev-mode: forward /api and /healthz to a locally-running admin API so the SPA can be
      // developed against a real backend without setting up CORS twice.
      '/api': 'http://localhost:9713',
      '/healthz': 'http://localhost:9713',
      '/readyz': 'http://localhost:9713',
    },
  },
});
