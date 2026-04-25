# serverless-db

`serverless-db` is a Rust database engine packaged for **Runpod queue-based Serverless endpoints**. Runpod receives queued jobs on `/run` or `/runsync`, a small Python handler adapts the job contract, and the actual SQL execution happens inside the Rust engine running in the same worker container.

## Queue API shape

Runpod queue-based endpoints accept fixed operations like `/run` and `/runsync`. The request payload should place the SQL command inside the `input` object:

```json
{
  "input": {
    "database": "app",
    "sql": "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, active BOOL)"
  }
}
```

The worker returns the database response as the Runpod job output.

## Supported SQL

- `CREATE DATABASE app`
- `CREATE TABLE users (...)`
- `SHOW DATABASES`
- `SHOW TABLES`
- `INSERT INTO users (...) VALUES (...)`
- `SELECT * FROM users WHERE ... LIMIT ...`
- `UPDATE users SET ... WHERE ...`
- `DELETE FROM users WHERE ...`

## Local run

Run the Rust engine directly:

```bash
cargo run
```

Then initialize a database against the local engine:

```bash
curl http://localhost:8080/sql \
  -H 'content-type: application/json' \
  -d '{"sql":"CREATE DATABASE app"}'
```

```bash
curl http://localhost:8080/sql \
  -H 'content-type: application/json' \
  -d '{
    "database":"app",
    "sql":"CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, active BOOL)"
  }'
```

```bash
curl http://localhost:8080/sql \
  -H 'content-type: application/json' \
  -d '{
    "database":"app",
    "sql":"INSERT INTO users (id, name, active) VALUES (1, '\''owen'\'', true), (2, '\''sam'\'', false)"
  }'
```

```bash
curl http://localhost:8080/sql \
  -H 'content-type: application/json' \
  -d '{
    "database":"app",
    "sql":"SELECT * FROM users WHERE id >= 1 LIMIT 10"
  }'
```

## Configuration

- `PORT`: bind port, useful on Runpod. Default: `8080`
- `SERVERLESS_DB_BIND`: explicit bind address override
- `SERVERLESS_DB_DATA_DIR`: storage root. Default: `./data`

## Storage

This first version uses simple durable JSON table snapshots and an on-disk catalog:

```text
data/
  catalog.json
  app/
    users.json
```

That keeps the storage engine easy to reason about while the API and execution path stabilize. The next storage step should be WAL + immutable segments, not a more complicated in-place file format.

## Runpod deployment notes

Use a **queue-based endpoint**. Runpod places jobs in a queue, invokes the Python handler with the standard `{"id": "...", "input": {...}}` payload, and the handler forwards the SQL request into the Rust engine inside the worker.

Recommended initial settings:

- CPU endpoint
- active workers: `1`
- max workers: `2-4`
- single datacenter
- set `SERVERLESS_DB_DATA_DIR=/runpod-volume/serverless-db` if using a network volume

Queued endpoints use `/run` for async requests and `/runsync` for sync requests. Keep the SQL payload inside `input`.

Example sync request:

```bash
curl -X POST "https://YOUR_ENDPOINT_ID.runpod.net/runsync" \
  -H "content-type: application/json" \
  -H "authorization: Bearer YOUR_RUNPOD_API_KEY" \
  -d '{
    "input": {
      "database": "app",
      "sql": "SHOW TABLES"
    }
  }'
```

## Current limitations

- No joins, aggregations, transactions, indexes, or `ALTER TABLE`
- One statement per request
- Basic `WHERE` support only
- No Redis coordination layer yet
- Storage engine is snapshot-based for now

## Verification

```bash
cargo check
cargo test
```

## Container publishing

GitHub Actions builds the image on every pull request and pushes a public container image to GitHub Container Registry on pushes to `main` and version tags like `v0.1.0`.

Image name:

```text
ghcr.io/<owner>/serverless-db
```

If the package is not public after the first push, change the package visibility to public in GitHub Packages once and later pushes will stay on the same package.
