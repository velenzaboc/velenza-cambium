# RUNBOOK: parallel-replay a Go repo's full history into the code-graph

This runbook is written for a Claude Code agent executing on a Mac (Apple
Silicon). It ingests **every `.go`-touching commit** of a target repo into the
shared `code-graph` Spanner database, using a parallel sharded replay.

Reference target below is `velenza-cambium` (~1,487 `.go` commits). Expected wall
time on an M4: **~1 hour**.

---

## 0. Prerequisites — verify ALL of these first. If any is missing, STOP and report.

```bash
go version                       # need Go 1.27 or newer
git --version
gcloud version
```

- Obtain the exact owner-qualified code-graph service-account email from the
  current infrastructure output, and verify its grants are applied before use.
  Do not invent or reuse a generic principal.
  Local operation uses a short-lived impersonated Application Default
  Credentials carrier; exportable service-account keys are rejected.
- Prepare the keyless carrier and selector before building:

  ```bash
  export CODE_GRAPH_GCP_PROJECT='your-gcp-project-id'
  export CODE_GRAPH_SPANNER_INSTANCE='your-spanner-instance-id'
  export CODE_GRAPH_SPANNER_DATABASE='code-graph'
  export CODE_GRAPH_CREDENTIALS_SOURCE=application_default
  export CODE_GRAPH_CREDENTIALS_PROFILE=impersonated_service_account
  export CODE_GRAPH_CREDENTIALS_IDENTITY="code-graph-operator@${CODE_GRAPH_GCP_PROJECT}.iam.gserviceaccount.com"
  unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE CLOUDSDK_CONFIG \
    EXPERIMENTAL_GOOGLE_API_USE_S2A GCE_METADATA_HOST GRPC_BINARY_LOG_FILTER \
    GRPC_ENFORCE_ALPN_ENABLED HTTP_PROXY HTTPS_PROXY NO_PROXY \
    GOOGLE_API_GO_EXPERIMENTAL_DISABLE_NEW_AUTH_LIB GOOGLE_API_GO_EXPERIMENTAL_ENABLE_NEW_AUTH_LIB \
    GOOGLE_API_CERTIFICATE_CONFIG GOOGLE_API_USE_CLIENT_CERTIFICATE \
    GOOGLE_API_USE_MTLS GOOGLE_API_USE_MTLS_ENDPOINT \
    GOOGLE_AUTH_TRUST_BOUNDARY_ENABLED GOOGLE_CLOUD_DISABLE_DIRECT_PATH \
    GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS GOOGLE_CLOUD_QUOTA_PROJECT GOOGLE_CLOUD_UNIVERSE_DOMAIN \
    GOOGLE_SPANNER_DISABLE_DIRECT_ACCESS_BOUND_TOKEN GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS \
    GOOGLE_SPANNER_ENABLE_GCP_FALLBACK GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API \
    SSL_CERT_DIR SSL_CERT_FILE SPANNER_EMULATOR_HOST SPANNER_MONITORING_HOST \
    http_proxy https_proxy no_proxy
  gcloud auth application-default login \
    --impersonate-service-account="$CODE_GRAPH_CREDENTIALS_IDENTITY"
  chmod 600 "${HOME}/.config/gcloud/application_default_credentials.json"
  ```

  The binary validates the exact impersonation target, source credential type,
  scope, any declared quota project, file ownership and permissions before
  constructing any Google client. It then pins
  `GOOGLE_API_USE_CLIENT_CERTIFICATE=false` and
  `GOOGLE_API_USE_MTLS_ENDPOINT=never`, disabling automatic discovery of
  `~/.config/gcloud/certificate_config.json` by the pinned Google transport.
  `GODEBUG` may retain unrelated Go runtime tuning, but it must not contain
  `http2debug=`, `tlsmlkem=0`, or `tlssecpmlkem=0`. The binary rejects those
  values before resolving credentials so bearer material cannot enter HTTP/2
  debug logs and the Go 1.27 default hybrid ML-KEM key exchanges cannot be
  disabled.
- Sharing a machine with another impersonating tool: the well-known ADC
  location is a single slot, so a peer that impersonates a different operator
  (for example `task-graph-operator@`) would otherwise displace this one.
  Point code-graph at its own carrier instead of contending for that slot:

  ```bash
  export CODE_GRAPH_CREDENTIALS_FILE="${HOME}/.config/code-graph/adc.json"
  export CODE_GRAPH_CREDENTIALS_PROFILE=impersonated_service_account
  export CODE_GRAPH_CREDENTIALS_IDENTITY="code-graph-operator@${CODE_GRAPH_GCP_PROJECT}.iam.gserviceaccount.com"
  # Mint the carrier into the well-known slot, then move it aside:
  gcloud auth application-default login \
    --impersonate-service-account="$CODE_GRAPH_CREDENTIALS_IDENTITY"
  mkdir -p "$(dirname "$CODE_GRAPH_CREDENTIALS_FILE")"
  mv "${HOME}/.config/gcloud/application_default_credentials.json" \
    "$CODE_GRAPH_CREDENTIALS_FILE"
  chmod 600 "$CODE_GRAPH_CREDENTIALS_FILE"
  ```

  `CODE_GRAPH_CREDENTIALS_FILE` and `CODE_GRAPH_CREDENTIALS_SOURCE` are
  mutually exclusive; exactly one must be set. The path must be absolute. The
  explicit carrier is not a relaxed arm: it holds the same impersonated
  document and receives byte-identical validation — impersonation target,
  source credential type, scope, quota project, forbidden key material,
  duplicate JSON keys, symlink refusal and owner-only file mode. Every
  ambient-override and `GODEBUG` rejection above applies unchanged. The hosted
  `attached_service_account` profile refuses `CODE_GRAPH_CREDENTIALS_FILE`
  outright, so the deployed image cannot read an on-disk credential.
- Network: the replay reaches Spanner and Vertex AI over public
  `*.googleapis.com` endpoints. No VPN / tailscale needed.
- The target Spanner database (`code-graph` by default) is configured via
  `CODE_GRAPH_GCP_PROJECT`, `CODE_GRAPH_SPANNER_INSTANCE`, and
  `CODE_GRAPH_SPANNER_DATABASE` (or `ARBOR_GCP_PROJECT` / `ARBOR_SPANNER_INSTANCE`).

---

## 1. Build code-graph

You already have this repo (you're reading this file in it).

```bash
cd ~/code-graph        # or wherever this repo was cloned
go build -o bin/code-graph .
./bin/code-graph 2>&1 | head -3        # should print the usage block
```

---

## 2. Clone the target repo with all branches

```bash
git clone https://github.com/velenzaboc/velenza-cambium.git ~/velenza-cambium
cd ~/velenza-cambium && git fetch --all
git rev-list --all --count -- '*.go'   # expect ~1480-1490
```

Requires read access to the repo — run `gh auth login` first if the clone
prompts for credentials.

---

## 3. Run the governed parallel replay

`replay` runs an in-process worker pool with a **live memory governor**. It
sizes itself: `--workers 0` (the default) picks `min(cores, freeRAM / 3.5GB)`,
and the governor pauses launching any new ingest while free RAM is below
`--min-free-gb` (default 4). You do NOT launch multiple processes — one
command does the whole thing.

CRITICAL — memory is the binding constraint. **One ingest holds ~3 GB of RAM.**
Do not override `--workers` higher than the machine's RAM allows:

| RAM   | safe `--workers` | notes                                  |
|-------|------------------|----------------------------------------|
| 16 GB | 3                | close other apps first                 |
| 32 GB | 7-8              |                                        |
| 64 GB | 14-16            |                                        |

Leaving `--workers` unset (auto) is safe — it derives the number from free RAM.
Setting it too high is what OOM-kills the machine; the governor is a safety
net, not a license to oversubscribe.

```bash
cd ~/code-graph && mkdir -p ~/replay-logs
./bin/code-graph replay --go-only ~/velenza-cambium \
  > ~/replay-logs/replay.log 2>&1 &
echo "replay started (pid $!) — monitoring..."
tail -f ~/replay-logs/replay.log
```

The first line of the log states the chosen worker count and governor
threshold. If you want to pin it explicitly, add `--workers N` from the table.

**Resumable:** already-ingested commits are skipped, so if the run dies just
re-run the same command — it picks up where it left off.

If you see repeated `[governor] ... holding` lines, that is the governor
working correctly — the machine is memory-tight and it is throttling
concurrency down. It is not an error.

---

## 4. Verify and report

```bash
echo "velenza-cambium snapshots now in the graph:"
./bin/code-graph list-commits \
  | grep -c velenza-cambium

echo "final result line:"
grep "replay done" ~/replay-logs/replay.log

echo "failures, if any:"
grep "failed:" ~/replay-logs/replay.log | head -20
```

**Report back:** total commits ingested, total skipped, total failed, wall
time, and any repeated errors in the log — especially Vertex AI
`RESOURCE_EXHAUSTED` / 429, or Spanner errors.

---

## What to expect

- **~25-32 s per commit** once caches are warm (measured on M1).
- **Memory, not CPU, is the limit.** Each ingest holds ~3 GB. Concurrency is
  capped by `freeRAM / 3.5GB`, not by core count. A 32 GB box runs ~8 at once;
  a 16 GB box only ~3.
- The **first commit per worker is slower** — Go module-cache warm-up.
  Subsequent commits are fast: the content-hash caches (`SymbolBlobs` for
  embeddings, `SymbolEdges` for edges) reuse everything unchanged.
- **Old commits** may not type-check cleanly against current dependency
  versions. `walkRepo` continues on partial loads, so those snapshots ingest
  with reduced fidelity rather than failing the run. Some `failed:` lines are
  acceptable; a run that fails *every* commit is not — investigate.
- **Disk:** each worker creates one worktree of the target repo under
  `/tmp/cg-replay-*` (~50-100 MB each), auto-removed when the run finishes.

## Splitting across multiple machines (optional)

`--shard i/n` splits the commit list across MACHINES (not within one — the
governed pool already parallelizes within a machine). To use a 32 GB box and a
16 GB box together: run `replay --go-only --shard 0/2 ~/velenza-cambium` on one and
`--shard 1/2` on the other. Each still governs its own pool.

## Replaying a different repo

Replace `~/velenza-cambium` throughout with the target repo path, and the
`git clone` URL in Step 2. The repo must be a Go module (have a `go.mod`). The
repo name recorded in the graph is derived from the shared git directory, so
worktrees of the same repo always agree.
