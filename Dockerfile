ARG GO_VERSION=1.26
ARG GO_IMAGE=golang:${GO_VERSION}-alpine

FROM --platform=${BUILDPLATFORM} $GO_IMAGE AS dev
RUN apk add --no-cache postgresql17-client delve make gcc build-base git
RUN adduser -D trucker
USER trucker
RUN mkdir -p /tmp/go-cache

FROM --platform=${BUILDPLATFORM} $GO_IMAGE AS build
ARG TARGETOS
ARG TARGETARCH
ARG TRUCKER_VERSION
ENV CGO_ENABLED=0
ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}
ENV GOPATH=/tmp/go-cache
ENV TRUCKER_VERSION=${TRUCKER_VERSION}
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/tmp/go-cache \
    go build -v -buildvcs=false -ldflags="-s -w -X main.version=${TRUCKER_VERSION}"

FROM scratch
COPY --from=build /src/trucker /trucker
WORKDIR /project
USER 31873
ENTRYPOINT ["/trucker"]
