import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

function vendorChunkName(id: string) {
  if (id.includes('node_modules/react/') || id.includes('node_modules/react-dom/') || id.includes('node_modules/scheduler/')) {
    return 'react-vendor';
  }

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

  if (packageName.startsWith('@ant-design/')) {
    return `ant-design-${packageName.split('/')[1]}`;
  }

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
          changeOrigin: true,
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
