import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/auth':     { target: 'http://127.0.0.1:8000', changeOrigin: true },
      '/datasets': { target: 'http://127.0.0.1:8000', changeOrigin: true },
      '/jobs':     { target: 'http://127.0.0.1:8000', changeOrigin: true, ws: true },
      '/plots':    { target: 'http://127.0.0.1:8000', changeOrigin: true }
    }
  }
})
