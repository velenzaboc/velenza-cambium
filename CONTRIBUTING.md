# Contributing & Open-Source Hygiene Policy

Velenza Cambium (`velenza-cambium` / `code-graph` / `inr cambium`) is a strictly vendor-neutral, clean-room open-source project licensed under Apache-2.0.

## Strict Zero-Proprietary-IP & Config-Only Policy

To keep Velenza Cambium 100% portable and safe for public distribution, the repository enforces three non-negotiable rules through automated CI tests (`open_source_hygiene_test.go`):

1. **Zero Proprietary IP or Organization Identifiers**
   - No organization names, internal codenames, internal project identifiers, or proprietary domain names may appear anywhere in source code, comments, tests, commit messages, or documentation.
   - Use generic domains and example service-account identities (`@example.com` or `@example-project.iam.gserviceaccount.com`) in unit and integration tests.

2. **Zero Hardcoded Environment Coordinates**
   - Never hardcode workstation paths (`/Users/...`, `/Volumes/...`, `/home/...`, `C:\Users\...`), internal hostnames, cloud project IDs, database instance IDs, or private/Tailscale IP addresses (`100.x.x.x`, `10.x.x.x`, `192.168.x.x`).

3. **Everything Goes Through Configuration**
   - All deployment-specific coordinates—GCP project IDs, Spanner instance IDs, Spanner database names, and credential selectors—must be supplied at runtime via `VELENZA_CAMBIUM_*`, `ARBOR_*`, or `CODE_GRAPH_*` environment variables.
   - See [`README.md`](./README.md) and [`RUNBOOK.md`](./RUNBOOK.md) for the full configuration reference.

## Verification Before Submitting Changes

Run the full test suite (including the open-source hygiene scanner) before opening a pull request:

```sh
go vet ./...
go build ./...
go test -race -count=1 ./...
```

## License, Code of Conduct & Contact

- **License**: By contributing to Velenza Cambium, you agree that your contributions will be licensed under the [Apache License, Version 2.0](./LICENSE) and subject to the attribution requirements in [`NOTICE`](./NOTICE).
- **Code of Conduct**: Participation is governed by our [Code of Conduct](./CODE_OF_CONDUCT.md).
- **Security Vulnerabilities**: Please follow [`SECURITY.md`](./SECURITY.md) and report security issues privately via GitHub Private Vulnerability Reporting or email to **open-source@velenzaboc.com**.
- **General Open-Source Contact**: **open-source@velenzaboc.com**
