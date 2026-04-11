import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

function vendorChunkName(id: string) {
  // @ts-ignore
  if (id.includes('node_modules/react/') || id.includes('node_modules/react-dom/') || id.includes('node_modules/scheduler/')) {
    return 'react-vendor';
  }

  // @ts-ignore
  if (id.includes('node_modules/react-router/') || id.includes('node_modules/react-router-dom/') || id.includes('node_modules/@remix-run/router/')) {
    return 'router-vendor';
  }

  const marker = 'node_modules/';
  const markerIndex = id.lastIndexOf(marker);
  if (markerIndex === -1) {
    return undefined;
  }

  const packagePath = id.slice(markerIndex + marker.length);
  const segments = packagePath.split('/');
  // @ts-ignore
  const packageName = segments[0]?.startsWith('@') ? `${segments[0]}/${segments[1]}` : segments[0];
  if (!packageName) {
    return undefined;
  }

  if (packageName === 'antd') {
    return 'antd-core';
  }

  if (packageName === 'dayjs' || packageName === 'string-convert' || packageName === 'json2mq') {
    return 'antd-core';
  }

  if (packageName === '@ant-design/icons') {
    return 'antd-icons';
  }

  // @ts-ignore
  if (packageName.startsWith('@ant-design/')) {
    return `ant-design-${packageName.split('/')[1]}`;
  }

  // @ts-ignore
  if (packageName.startsWith('rc-')) {
    return `rc-${packageName.slice(3)}`;
  }

  return `vendor-${packageName.replace('@', '').replace(/\//g, '-')}`;
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, '.', '');

  return {
    plugins: [react()],
    server: {
      proxy: {
        '/api': {
          target: env.VITE_ADMIN_PROXY || 'http://127.0.0.1:19080',
          // Preserve the browser Host header so the backend same-origin check
          // sees the Vite dev server origin instead of the upstream target.
          changeOrigin: false,
          ws: true,
          // Keep the browser Origin header for websocket upgrades too.
          rewriteWsOrigin: false,
        },
      },
    },
    build: {
      rollupOptions: {
        output: {
          manualChunks(id) {
            return vendorChunkName(id);
          },
        },
      },
    },
  };
});
