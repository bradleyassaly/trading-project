import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// NOTE: Default is localhost for native dev. Docker overrides via
// VITE_API_TARGET env var. Do NOT hardcode http://localhost:8001 here —
// that only resolves inside the Docker compose network.
//
// - Native dev (npm run dev): defaults to http://localhost:8001
// - Docker: set VITE_API_TARGET=http://localhost:8001 in docker-compose.yml
const API_TARGET = process.env.VITE_API_TARGET || 'http://localhost:8001'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/api': {
        target: API_TARGET,
        changeOrigin: true,
      },
    },
  },
})
