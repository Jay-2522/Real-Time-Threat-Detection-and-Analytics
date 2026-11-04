FROM node:22-alpine AS base
WORKDIR /app
COPY package*.json ./
RUN npm ci

# Hot-reloading development server, used only through docker-compose.dev.yml.
FROM base AS dev
COPY . .
EXPOSE 5173
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]

FROM base AS build
ARG VITE_API_BASE_URL=http://localhost:8000/api/v1
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
COPY . .
RUN npm run build

# Default image: the compiled app served by nginx, not a development server.
FROM alpine:3.23 AS runtime
RUN apk add --no-cache nginx
COPY nginx.conf /etc/nginx/nginx.conf
COPY --from=build /app/dist /usr/share/nginx/html
USER 101:101
EXPOSE 8080
ENTRYPOINT ["nginx", "-e", "/dev/stderr", "-g", "daemon off;"]
