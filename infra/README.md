# Streamer Forge Infra

This folder contains per-component Helm charts:

- `origin-server`
- `ingest-server`
- `transcoder`
- `broadcaster`
- `abr-player`

## Build images

Dockerfiles are in `Build/`:

- `Dockerfile.origin-server`
- `Dockerfile.ingest-server`
- `Dockerfile.transcoder`
- `Dockerfile.broadcaster`
- `Dockerfile.abr-player`
- `Dockerfile.orchestrator`

Example:

```bash
docker build -f Build/Dockerfile.origin-server -t your-registry/origin-server:latest .
```

## Helm deploy

```bash
helm upgrade --install origin-server infra/origin-server \
  --set image.repository=your-registry/origin-server \
  --set image.tag=latest
```

Repeat similarly for each component chart.
