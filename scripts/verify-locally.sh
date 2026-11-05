#!/usr/bin/env bash
# Run only through the native the designated verification host resource guard in an owned run workspace.
set -euo pipefail
if [[ "${1-}" == --help ]]; then
  printf '%s\n' 'Run: python3 "$VERIFY_RESOURCE_GUARD" --cwd <unique-run-checkout> -- bash scripts/verify-locally.sh'
  exit 0
fi
[[ $# == 0 ]] || { printf '%s\n' 'verification requires the designated verification host and its native resource guard' >&2; exit 64; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$root"
[[ ! -e .local-verification ]] || { printf '%s\n' 'verification output already exists; select a clean unique run workspace' >&2; exit 64; }
evidence_dir="$root/.local-verification"
mkdir -m 700 "$evidence_dir"
export GOMAXPROCS=4
export GOFLAGS=-p=4
go vet ./...
go build ./...
go test -race -count=1 -covermode=atomic -coverprofile="$evidence_dir/coverage.out" -json ./... > "$evidence_dir/tests.json"
go tool cover -func="$evidence_dir/coverage.out" > "$evidence_dir/coverage.txt"
printf '%s\n' 'Local verification completed; preserve this run evidence before removing the owned workspace.'
