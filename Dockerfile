ARG GO_VERSION=1.23
ARG GO_IMAGE=golang:${GO_VERSION}-alpine
FROM $GO_IMAGE AS base
RUN adduser -D trucker

FROM --platform=${BUILDPLATFORM} base AS dev
RUN apk add --no-cache postgresql16-client delve make

FROM --platform=${BUILDPLATFORM} base AS build
ARG TARGETOS
ARG TARGETARCH
ENV GOCACHE=/root/.cache/go-build
ENV CGO_ENABLED=0
ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}
COPY . /src
WORKDIR /src
RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=bind,source=go.sum,target=go.sum \
    --mount=type=bind,source=go.mod,target=go.mod \
    --mount=type=cache,target="/root/.cache/go-build" \
    go build -ldflags="-s -w"

FROM scratch
COPY --from=build /src/trucker /trucker
WORKDIR /project
ENTRYPOINT ["/trucker"]
