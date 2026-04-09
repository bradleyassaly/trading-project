import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Inside the Docker stack the api service is reachable as http://api:8001
// (compose network DNS). On a bare-metal dev box where vite runs from the
// host, it's http://localhost:8001. ``VITE_API_TARGET`` lets the env
// override; default to the Docker service name because that's how we
// actually deploy. The previous default of `localhost:8001` was the root
// cause of the "frontend shows no data despite API returning valid
// responses" bug — every /api/* call from the browser proxied to a
// nonexistent service inside the frontend container.
const API_TARGET = process.env.VITE_API_TARGET || 'http://api:8001'

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
