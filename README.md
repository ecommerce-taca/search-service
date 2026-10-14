# Search Service

E-commerce Full-Text Product Search, Autocomplete, and Index Management Service.

## 1. Overview & Architecture

Built with **Node 26**, **NestJS 11**, **TypeScript (Strict Mode)**, **Elasticsearch 8.x**, and **KafkaJS**.

### Module Layout (per LLD §2.1)

```text
src/
├── search-query/       # Query parser, filter/facet/sort, result mapper
├── autocomplete/       # Completion suggester
├── product-index/      # Index mapping, document builder, bulk write, ElasticsearchModule
├── event-consumer/     # Kafka consumer, dedupe, version ordering, KafkaModule
├── reindex/            # Alias swap, backfill, resume checkpoint
├── security/           # Route roles guard, step-up MFA guard
├── observability/      # Pino structured JSON logger, Prom-client metrics
├── health/             # Liveness (/health/live) and readiness checks
├── common/             # Constants, enums, errors, context (AsyncLocalStorage)
└── config/             # Typed and fail-fast validated environment configuration
```

## 2. Environment Variables

| Variable | Default | Required | Description |
|---|---|:---:|---|
| `PORT` | `3000` | No | HTTP listening port |
| `SERVICE_NAME` | `search-service` | No | Service identifier in logs and metrics |
| `APP_ENV` | `development` | No | Environment (`development`, `production`, `test`) |
| `APP_VERSION` | `1.0.0` | No | Application release version |
| `ES_NODE` | — | **Yes** | Elasticsearch node URL |
| `ES_INDEX_PREFIX` | `""` | No | Prefix applied to all index names and aliases |
| `KAFKA_BROKERS` | — | **Yes** | Comma-separated list of Kafka broker endpoints |
| `KAFKA_CLIENT_ID` | `search-service` | No | Kafka client ID |
| `KAFKA_GROUP_ID` | `search-service-group` | No | Kafka consumer group ID |
| `KAFKA_TOPIC_PREFIX` | `""` | No | Prefix for domain topics and DLQ |

Refer to `.env.example` for baseline configuration.

## 3. Development & Build

```bash
# Install dependencies
npm ci

# Run in development mode
npm run start:dev

# Run linter
npm run lint

# Format code
npm run format

# Compile build
npm run build
```

## 4. Testing

```bash
# Run unit tests (src/**/*.spec.ts)
npm test

# Run e2e tests (test/**/*.e2e-spec.ts)
npm run test:e2e

# Run integration tests against Elasticsearch cluster (test/**/*.int-spec.ts)
npm run test:int
```

## 5. Observability & Endpoints

- **Liveness Endpoint:** `GET /health/live` (process-only, excluded from global prefix `api/v1`)
  - Status: 200 `{ "data": { "status": "UP", "service": "search-service", "time": "..." }, "meta": { "request_id": "..." } }`
- **Prometheus Metrics:** `GET /metrics` (excluded from global prefix `api/v1`)
  - Exposes `http_request_duration_seconds` with labels `route`, `method`, `status`
- **Correlation & Tracing:**
  - `X-Request-ID` is extracted (<=64 characters) or generated, and echoed back in response headers.
  - W3C `traceparent` (32-hex trace_id, 16-hex span_id) and `tracestate` propagation.
- **Structured JSON Logging:**
  - One JSON line per log with fields: `timestamp`, `level`, `service`, `env`, `version`, `event`, `trace_id`, `span_id`, `request_id`, `route`, `method`, `status_code`, `duration_ms`.
  - Sensitive headers (`authorization`, `cookie`) are redacted.

## 6. Docker Container

```bash
# Build multi-stage Docker image
docker build -t ecommerce-search-service:latest .

# Run container as non-root user
docker run -p 3000:3000 --env-file .env ecommerce-search-service:latest
```

## 7. Reindex & Manual Rollback Procedure (v1)

Rollback is a manual operational procedure in v1. The previous index is retained in the cluster upon alias swap. To revert `products-read` back to the previous index atomically without downtime, execute a single `_aliases` request:

```bash
curl -X POST "http://${ES_NODE}/_aliases" \
  -H "Content-Type: application/json" \
  -d '{
    "actions": [
      { "remove": { "index": "<current-active-index>", "alias": "products-read" } },
      { "add":    { "index": "<previous-retained-index>", "alias": "products-read" } }
    ]
  }'
```