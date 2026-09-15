FROM node:26.8.2-alpine AS tooling
WORKDIR /app
RUN npm install --global npm@12.0.2

FROM tooling AS build
COPY package*.json .npmrc ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.28-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80