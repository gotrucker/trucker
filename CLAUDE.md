# Trucker Development Guide

## Build and Run Commands
- `make dev` - Start development environment containers
- `make sh` - Get a shell in the Go container
- `make build` - Build the Trucker binary locally
- `make clean` - Clean up containers and images

## Test Commands
- `go test ./...` - Run all tests
- `go test ./pkg/postgres` - Run tests in a specific package
- `go test -run TestPostgresToPostgres ./test/system` - Run a specific test
- `go test -v ./pkg/postgres` - Run tests with verbose output

## Code Style Guidelines
- Use Go standard formatting (`go fmt`)
- Organize imports alphabetically (standard library first, then third-party)
- Error handling: check errors explicitly, propagate when appropriate
- Naming: use camelCase for variables, PascalCase for exported functions/types
- Favor explicit error returns over panics (except for init/setup functions)
- Keep functions small and focused on a single responsibility
- Use table-driven tests where appropriate
- Comments should explain why, not what (code should be self-explanatory)
- Add context to errors with fmt.Errorf("doing X: %w", err)