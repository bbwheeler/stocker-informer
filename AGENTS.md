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

## Version Control

## Submitting Changes

All changes, edits, documents, and artifacts must be pushed to the repository when complete. To do so, these steps must be followed:
1. Commit the code into a branch using git
2. Push the code to remote origin (git.wheeli.ca)
3. Open a pull request for the changes you just pushed
4. Add me (username: brian) as a reviewer on the merge request

## Tools

The Forgeji MCP (command: forgejo_mcp) can be used to execute tasks on git.wheeli.ca such as putting up a PR or MR.

Credentials for the Forgejo instance git.wheeli.ca can be found in the parent directory (../credentials.md)