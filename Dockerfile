# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM node:22-bookworm-slim AS web-builder

WORKDIR /src
COPY frontend/package.json frontend/package-lock.json ./frontend/
WORKDIR /src/frontend
RUN --mount=type=cache,target=/root/.npm npm ci
WORKDIR /src
COPY frontend ./frontend
RUN mkdir -p internal/web/ui/dist
WORKDIR /src/frontend
RUN npm run build

FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY --from=web-builder /src/internal/web/ui/dist ./internal/web/ui/dist
RUN CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/codex-queue-bot ./cmd/codex-queue-bot

FROM node:22-bookworm-slim AS codex-dist

ARG CODEX_VERSION=latest
RUN --mount=type=cache,target=/root/.npm \
    npm install --global "@openai/codex@${CODEX_VERSION}" \
    && CODEX_NATIVE="$(find /usr/local/lib/node_modules/@openai/codex/node_modules/@openai -type f -path '*/bin/codex' -print -quit)" \
    && test -n "${CODEX_NATIVE}" \
    && install -D -m 0755 "${CODEX_NATIVE}" /out/codex \
    && /out/codex --version

FROM debian:bookworm-slim

# Keep Debian packages on the current security-patched versions from the base image.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && groupadd --gid 10001 app \
    && useradd --create-home --uid 10001 --gid 10001 --shell /usr/sbin/nologin app \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /out/codex-queue-bot /usr/local/bin/codex-queue-bot
COPY --from=codex-dist /out/codex /usr/local/bin/codex
COPY prompts.txt /app/prompts.txt
RUN mkdir -p /app/data && chmod 0700 /app/data && chown -R 10001:10001 /app/data

USER 10001:10001
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/codex-queue-bot"]
CMD ["-db", "/app/data/codex-queue-bot.db"]
