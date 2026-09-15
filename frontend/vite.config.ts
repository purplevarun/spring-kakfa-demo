import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    proxy: {
      '/api/order': {
        target: 'http://order-service:4003',
        rewrite: (path) => path.replace(/^\/api\/order/, ''),
      },
      '/api/inventory': {
        target: 'http://inventory-service:4001',
        rewrite: (path) => path.replace(/^\/api\/inventory/, ''),
      },
      '/api/notification': {
        target: 'http://notification-service:4002',
        rewrite: (path) => path.replace(/^\/api\/notification/, ''),
      },
    },
  },
})
