# Multi-stage build for the code-graph HTTP service.
# Cloud Run target: the `serve` subcommand binds $PORT (or :8080), exposes
# /ingest + /manifest + /healthz + /readyz; OIDC verify is on when
# OIDC_AUDIENCE is set (the service URL).

FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/code-graph .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/code-graph /code-graph
USER nonroot
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/code-graph", "serve"]
