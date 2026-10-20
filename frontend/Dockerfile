# Build stage
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
# The API URL is baked in at build time (the browser calls it directly)
ARG REACT_APP_API_URL=http://localhost:4000
ENV REACT_APP_API_URL=$REACT_APP_API_URL
RUN npm run build

# Serve stage
FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 80
