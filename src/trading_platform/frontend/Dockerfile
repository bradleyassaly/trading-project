# Multi-stage build: Vite builds static assets; nginx serves them.
# Replaces the dev-server-in-prod setup — ~5x faster page loads, no HMR
# overhead, no exposed source. For dev workflows, use
# `npm run dev` on the host or override the command in compose.

# ── Build stage ───────────────────────────────────────────────────────
FROM node:20-slim AS build

WORKDIR /app

COPY package*.json ./
RUN npm install --no-audit --no-fund

COPY . .
RUN npm run build   # produces /app/dist


# ── Serve stage ────────────────────────────────────────────────────────
FROM nginx:1.27-alpine

# SPA routing: all unknown paths fall back to index.html so React Router
# handles client-side routes. /api/* is proxied to the api container.
RUN rm -f /etc/nginx/conf.d/default.conf

COPY nginx.conf /etc/nginx/conf.d/app.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

# nginx in foreground so docker keeps it running.
CMD ["nginx", "-g", "daemon off;"]
