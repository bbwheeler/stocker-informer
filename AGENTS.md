# stocker-informer

Go CLI daemon that queries tsx-history gRPC service and publishes top stocks to GoToSocial.

## Key facts

- **Module**: `github.com/example/stocker-informer` | **Go**: 1.25
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