import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

// The dev server proxies /api and /actuator to a running API service. A directly
// launched JVM listens on 8080; the Compose stack publishes 8088. VITE_API_PROXY
// selects the target so neither topology needs a source edit.
export default defineConfig(({ mode }) => {
  // '.' is the Vite project root; loadEnv reads .env files there and merges
  // matching variables from the shell, so no @types/node dependency is needed.
  const env = loadEnv(mode, '.', 'VITE_');
  const target = env.VITE_API_PROXY || 'http://localhost:8080';
  return {
    plugins: [react()],
    server: {
      port: 5173,
      proxy: {
        '/api': target,
        '/actuator': target,
      },
    },
  };
});
