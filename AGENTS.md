# Go project guidance

- Load `golang-how-to` and the relevant Go skills for Go changes.
- Keep entry points under `cmd/` minimal; put application logic in `internal/` packages.
- Define small interfaces at the consuming package and wire concrete adapters in `internal/app`.
