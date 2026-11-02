# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

See [README.md](README.md) for the API, on-disk format and development setup.

- Run `task sanity` as the gate for every change. It rewrites files in place.
- Tests are testify suites, so run one with `go test ./pkg/wal -race -count=1 -run 'TestCutSuite/TestBelowFirstIsNoop'`.
- Never add `//nolint` or weaken `.golangci.yaml`; change the code instead.
- Don't reorder fsync, rename, create or delete steps in `doCut`, `RewriteFrom` or `roll`. Recovery depends on that on-disk order, and tests may still pass if it's wrong.
