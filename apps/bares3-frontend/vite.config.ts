import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, '.', '');

  return {
    plugins: [react()],
    server: {
      proxy: {
        '/api': {
          target: env.VITE_ADMIN_PROXY || 'http://127.0.0.1:19080',
          changeOrigin: true,
        },
      },
    },
  };
});
