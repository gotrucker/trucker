FROM golang:alpine AS base
RUN adduser -D trucker

FROM base AS dev
RUN apk add --no-cache postgresql16-client
