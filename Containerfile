# Build stage
FROM golang:1.25-bookworm AS build
WORKDIR /src

COPY . .

RUN CGO_ENABLED=0 go build -o /out/stocker-informer ./cmd/server

# Runtime stage
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/stocker-informer /stocker-informer
USER 65534:65534 # nobody
ENTRYPOINT ["/stocker-informer"]