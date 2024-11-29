ARG GO_VERSION=1.23
ARG GO_IMAGE=golang:${GO_VERSION}-alpine
FROM $GO_IMAGE AS base
RUN adduser -D trucker

FROM base AS dev
RUN apk add --no-cache postgresql16-client delve make

FROM --platform=${BUILDPLATFORM} base AS build
ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH
COPY . /src
WORKDIR /src
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w"

FROM scratch
COPY --from=build /src/trucker /trucker
WORKDIR /project
ENTRYPOINT ["/trucker"]
