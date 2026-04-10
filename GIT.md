# Git Workflow for sb-storage-server

This repository contains the Slidebolt Storage Server, providing the actual data persistence and indexing (Bleve-based) for the ecosystem.

## Dependencies
- **Internal:**
  - `sb-messenger-sdk`: Used for receiving storage requests and sending responses.
  - `sb-storage-sdk`: Shared interfaces and utilities for storage operations.
- **External:** 
  - `github.com/blevesearch/bleve/v2`: Full-text search and indexing.
  - `github.com/nats-io/nats.go`: Communication with NATS.

## Build Process
- **Type:** Pure Go Library (Shared Module).
- **Consumption:** Typically imported and started by a service (like `sb-api` or a specialized storage service).
- **Artifacts:** No standalone binary is produced by *this* repository (see consumers for binaries).
- **Validation:** 
  - Validated through unit tests: `go test -v ./...`
  - Validated by its consumers during their respective build/test cycles.

## Pre-requisites & Publishing
As the core storage implementation, `sb-storage-server` must be updated whenever the `sb-messenger-sdk` or `sb-storage-sdk` is updated.

**Before publishing:**
1. Determine current tag: `git tag | sort -V | tail -n 1`
2. Ensure all local tests pass: `go test -v ./...`

**Publishing Order:**
1. Ensure `sb-messenger-sdk` and `sb-storage-sdk` are tagged and pushed (e.g., `v1.0.4`).
2. Update `sb-storage-server/go.mod` to reference the latest SDK tags.
3. Determine next semantic version for `sb-storage-server` (e.g., `v1.0.4`).
4. Commit and push the changes to `main`.
5. Tag the repository: `git tag v1.0.4`.
6. Push the tag: `git push origin main v1.0.4`.

## Update Workflow & Verification
1. **Modify:** Update indexing logic in `bleve.go` or request handling in `handler.go`.
2. **Verify Local:**
   - Run `go mod tidy`.
   - Run `go test ./...`.
3. **Commit:** Ensure the commit message clearly describes the storage implementation change.
4. **Tag & Push:** (Follow the Publishing Order above).
