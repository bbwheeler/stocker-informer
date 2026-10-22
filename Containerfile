# build stage
FROM docker.io/golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
# stocker-informer depends on the private module
# git.wheeli.ca/brian/stocker-store (fetched direct)
ENV GOPRIVATE=git.wheeli.ca
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/stocker-informer ./cmd/server

# runtime stage
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/stocker-informer /stocker-informer
USER 65534:65534
ENTRYPOINT ["/stocker-informer"]
