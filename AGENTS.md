# stocker-informer

Go CLI daemon that subscribes to a Kafka topic of stock events and publishes them to GoToSocial.

## Key facts

- **Module**: `github.com/example/stocker-informer` | **Go**: 1.25
- **Build/run**: `go build ./cmd/server/` | **Test**: `go test ./...`
- **Kafka dependency**: `github.com/segmentio/kafka-go` in go.mod

## Structure

```
cmd/server/main.go        # entry point
internal/config/          # config loading (env/struct)
internal/kafka/           # Kafka consumer + StockEvent decoder
internal/messenger/       # GoToSocial publisher + stock event formatter
```

## Architecture

`main()` loads config → dials GoToSocial via HTTP → subscribes to Kafka topic using StockEventFormatter for message formatting → publishes each stock event directly on GoToSocial.

## Environment variables

- `KAFKA_BOOTSTRAP_SERVERS` — comma-separated Kafka broker addresses (required)
- `KAFKA_TOPIC` — Kafka topic to subscribe to (required)
- `KAFKA_CONSUMER_GROUP` — consumer group ID for partition offset tracking (required)
- `GOTOSOCIAL_INSTANCE` — GoToSocial instance URL (required)
- `GOTOSOCIAL_USER` — GoToSocial username (required)
- `GOTOSOCIAL_TOKEN` — GoToSocial auth token (required)

No additional lint, format, or typecheck tooling beyond standard Go conventions.

## Tools

Credentials for the Forgejo instance git.wheeli.ca can be found in the parent directory (../credentials.md)