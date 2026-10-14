# Reindex & Search Admin Module

This module implements the index management, background reindexing, validation gates, and alias swap operations for the Search Service.

## 1. Endpoints

All admin endpoints require role `SEARCH_ADMIN` (`X-User-Roles: SEARCH_ADMIN`).
Mutations additionally require step-up authentication (`X-MFA-Step-Up: true`).

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/api/v1/admin/search/status` | `SEARCH_ADMIN` | Returns cluster alias, active index, mapping version, doc count, consumer lag, dlq count, and reindex state (`IDLE`, `REQUESTED`, `RUNNING`, `VALIDATING`, `SWAPPED`, `FAILED`, `CANCELLED`). |
| `POST` | `/api/v1/admin/search/reindex` | `SEARCH_ADMIN` + Step-Up | Triggers a versioned reindex. Payload: `{ source: 'INDEX_COPY' \| 'KAFKA_REPLAY', mapping_version: 1, reason: '...' }`. Returns 202 Accepted. |
| `GET` | `/api/v1/admin/search/reindex/:jobId` | `SEARCH_ADMIN` | Returns details and progress of a reindex job. |
| `POST` | `/api/v1/admin/search/reindex/:jobId/cancel` | `SEARCH_ADMIN` + Step-Up | Cancels a running/validating reindex job, deletes the in-progress target index, releases lock, and preserves `products-read` alias untouched. |

## 2. Validation Gates

Before swapping the `products-read` alias to the newly created index, three automated gates must pass:
1. **Document Count Gate**: Target count must match active index count within 0.1% threshold (`REINDEX_GATE_COUNT_MISMATCH`).
2. **Version Regression Gate**: Sample of 200 documents compared; target `source_version` must be $\ge$ active index `source_version` (`REINDEX_GATE_VERSION_REGRESSION`).
3. **Smoke Query Gate**: Match all published documents with aggregations on `category_ids`, `brand`, `attribute_pairs` and completion suggest on `suggest` with 0 shard failures and no visibility leakage (`REINDEX_GATE_SMOKE_FAILED`).

## 3. Manual Rollback Procedure (v1)

Rollback is an operational manual procedure in v1. Since the old index is preserved upon alias swap, an operator can revert `products-read` back to the previous index in a single atomic Elasticsearch `_aliases` request.

### cURL

```bash
curl -X POST "http://${ES_NODE}/_aliases" \
  -H "Content-Type: application/json" \
  -d '{
    "actions": [
      { "remove": { "index": "<failed-new-index>", "alias": "products-read" } },
      { "add":    { "index": "<previous-retained-index>", "alias": "products-read" } }
    ]
  }'
```

### TypeScript / Elasticsearch Client

```typescript
await client.indices.updateAliases({
  actions: [
    { remove: { index: '<failed-new-index>', alias: 'products-read' } },
    { add:    { index: '<previous-retained-index>', alias: 'products-read' } },
  ],
});
```

Because this is executed in a single atomic `actions` array, search traffic experiences zero downtime and the alias never points to 0 or 2 indices simultaneously.
