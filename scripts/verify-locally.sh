#!/usr/bin/env bash
# Run through the selected resource guard in an owned verification workspace.
set -euo pipefail
if [[ "${1-}" == --help ]]; then
  printf '%s\n' 'Run: python3 "$VERIFY_RESOURCE_GUARD" --cwd <unique-run-checkout> -- bash scripts/verify-locally.sh'
  exit 0
fi
[[ $# == 0 ]] || { printf '%s\n' 'verification requires the designated verification host and its native resource guard' >&2; exit 64; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$root"
# Generated receipts must not become inputs to the source-hygiene scan.
if [[ -n "${VERIFY_EVIDENCE_DIR-}" ]]; then
  [[ "$VERIFY_EVIDENCE_DIR" == /* ]] || { printf '%s\n' 'evidence path must be absolute' >&2; exit 64; }
  evidence_parent="$(cd "$(dirname "$VERIFY_EVIDENCE_DIR")" && pwd -P)"
  evidence_name="$(basename "$VERIFY_EVIDENCE_DIR")"
  [[ "$evidence_name" != . && "$evidence_name" != .. && "$evidence_parent" != "$root" && "$evidence_parent" != "$root/"* ]] || { printf '%s\n' 'evidence must remain outside the source checkout' >&2; exit 64; }
  evidence_dir="$evidence_parent/$evidence_name"
  [[ ! -e "$evidence_dir" && ! -L "$evidence_dir" ]] || { printf '%s\n' 'evidence output already exists' >&2; exit 64; }
  mkdir -m 700 "$evidence_dir"
else
  evidence_parent="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
  [[ "$evidence_parent" != "$root" && "$evidence_parent" != "$root/"* ]] || { printf '%s\n' 'temporary evidence must remain outside the source checkout' >&2; exit 64; }
  evidence_dir="$(mktemp -d "$evidence_parent/cambium-verification.XXXXXXXX")"
fi
printf 'Verification evidence: %s\n' "$evidence_dir"
export GOMAXPROCS=4
export GOFLAGS=-p=4
go vet ./...
go build ./...
go test -race -count=1 -covermode=atomic -coverprofile="$evidence_dir/coverage.out" -json ./... > "$evidence_dir/tests.json"
go tool cover -func="$evidence_dir/coverage.out" > "$evidence_dir/coverage.txt"
printf '%s\n' 'Local verification completed; preserve this run evidence before removing the owned workspace.'
