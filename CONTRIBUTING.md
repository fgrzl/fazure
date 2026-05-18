# Contributing

Thanks for contributing to fazure.

## Setup

1. Fork and clone the repository.
2. `go mod download`
3. Run tests: `go test ./...`
4. For integration checks, use `docker compose up` as described in the README.

## Pull requests

- Run `go fmt ./...` and `go vet ./...`.
- Update the feature matrix in the README when adding Azure API coverage.
- Update `docs/` for configuration or port changes.
- Fazure is for **local dev and CI only**—do not target production hardening without maintainer discussion.

## Changelog

Note changes under `## [Unreleased]` in [CHANGELOG.md](CHANGELOG.md).
