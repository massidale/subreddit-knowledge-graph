# Local Dev Stack

## Start

```bash
cd docker
docker compose up -d
```

## Endpoints

- Kafka broker: `localhost:9092`
- MinIO API: `http://localhost:9000` (admin/password: `minioadmin`/`minioadmin`)
- MinIO console: `http://localhost:9001`
- Neo4j Browser: `http://localhost:7474` (neo4j/devpassword)
- Neo4j Bolt: `bolt://localhost:7687`
- Spark master UI: `http://localhost:8080`

## Buckets created on startup

`bronze`, `silver`, `gold`, `raw-images` (via `minio-init` service).

## Stop

```bash
docker compose down       # keep volumes
docker compose down -v    # also drop volumes (full reset)
```
