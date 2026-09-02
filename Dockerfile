# Hermes3D - 3D agent visualization for Hermes.
# Multi-stage build: install prod deps -> build Next.js -> run with custom server.
#
# Node 22 is required by the current dependency graph. The runner also copies the
# `typescript` devDependency from the builder so Next can load next.config.ts
# immediately instead of installing development dependencies at container startup.

FROM node:22-slim AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --omit=dev

FROM node:22-slim AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts
COPY . .
ENV NEXT_TELEMETRY_DISABLED=1
# Build-time gateway URL (overridden at runtime by HERMES3D_GATEWAY_URL).
ENV NEXT_PUBLIC_GATEWAY_URL=ws://127.0.0.1:18789
RUN npm run build

FROM node:22-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1

# Links images to the repository on GHCR, so packages created by a push
# automatically grant this repo's workflows access and show up on the repo page.
LABEL org.opencontainers.image.source="https://github.com/iamlukethedev/Hermes3D"
LABEL org.opencontainers.image.description="Hermes3D — a 3D workspace for AI agents."
LABEL org.opencontainers.image.licenses="MIT"

# Copy built app + custom server + production node_modules. TypeScript is copied
# explicitly because next.config.ts is transpiled when the server starts.
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
COPY --from=builder /app/server ./server
COPY --from=deps /app/node_modules ./node_modules
COPY --from=builder /app/node_modules/typescript ./node_modules/typescript
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/next.config.ts ./next.config.ts

EXPOSE 3000

CMD ["node", "server/index.js"]
