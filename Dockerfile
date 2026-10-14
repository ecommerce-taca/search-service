# Stage 1: Build
FROM node:26-alpine AS builder

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build
RUN npm prune --omit=dev

# Stage 2: Production
FROM node:26-alpine

WORKDIR /usr/src/app

ENV NODE_ENV=production

COPY --chown=node:node --from=builder /usr/src/app/node_modules ./node_modules
COPY --chown=node:node --from=builder /usr/src/app/dist ./dist
COPY --chown=node:node --from=builder /usr/src/app/package*.json ./

USER node

EXPOSE 3000

CMD ["node", "dist/src/main"]
