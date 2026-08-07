# Build stage
FROM golang:1.25-bookworm AS build
WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl unzip && \
    curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v29.3/protoc-29.3-linux-x86_64.zip" -o /tmp/protoc.zip && \
    unzip -o /tmp/protoc.zip -d /usr/local && \
    rm /tmp/protoc.zip && \
    rm -rf /var/lib/apt/lists/*

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

COPY . .
WORKDIR /src/stocker-informer

ENV PATH="/root/go/bin:${PATH}"
RUN CGO_ENABLED=0 go build -o /out/stocker-informer ./cmd/server

# Runtime stage
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/stocker-informer /stocker-informer
ENTRYPOINT ["/stocker-informer"]
