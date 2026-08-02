# tsx-informer

Go CLI daemon that queries tsx-history gRPC service and publishes top stocks to GoToSocial.

## Key facts

- **Module**: `github.com/example/tsx-informer` | **Go**: 1.25
- **Build/run**: `go build ./cmd/server/` | **Test**: `go test ./...`
- **Local dependency**: `../tsx-history` via `replace` in go.mod — must exist or module resolution fails

## Structure

```
cmd/server/main.go        # entry point
internal/config/          # config loading (env/struct)
internal/informer/        # business logic + informer_test.go
internal/messenger/       # GoToSocial publisher + top-stocks scoring + messenger_test.go
```

## Architecture

`main()` loads config → dials tsx-history via gRPC → creates TopStocksMessenger (weighting) and GoToSocialPublisher → runs Informer loop.

No additional lint, format, or typecheck tooling beyond standard Go conventions.
