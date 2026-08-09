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

## Beginning Tasks
Before you begin a task, follow these steps:
1. Make sure all existing changes have been checked in; if there are existing changes, check them in.
2. Do a git fetch so that you have all of the latest changes.
3. Switch to a branch or create a branch appropriate for the changes that you will make

## Finishing Tasks
Once you complete any changes, additions, deletions, or modifications, follow these steps:
1. Check the code into a branch using git
2. Push the code to GitHub
3. Open a Pull Request for the changes you just pushed
4. Add me (bbwheeler) as a reviewer on the Pull Request

Your GitHub credentials can be found in the parent directory (../github.md)