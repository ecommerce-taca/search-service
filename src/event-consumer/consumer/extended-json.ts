// Debezium MongoEventRouter gửi payload dạng Extended JSON string nên số là {"$numberInt":"42"}.

export function normalizeExtendedJson(value: unknown): unknown {
  if (value === null || typeof value !== 'object') {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map(normalizeExtendedJson);
  }

  const record = value as Record<string, unknown>;
  const keys = Object.keys(record);

  if (keys.length === 1) {
    const key = keys[0];
    const inner = record[key];

    if (
      key === '$numberInt' ||
      key === '$numberLong' ||
      key === '$numberDouble' ||
      key === '$numberDecimal'
    ) {
      return Number(inner);
    }

    if (key === '$oid') {
      return String(inner);
    }

    if (key === '$date') {
      if (typeof inner === 'string' || typeof inner === 'number') {
        return new Date(inner).toISOString();
      }
      if (
        typeof inner === 'object' &&
        inner !== null &&
        '$numberLong' in inner
      ) {
        return new Date(
          Number((inner as Record<string, unknown>).$numberLong),
        ).toISOString();
      }
    }
  }

  const result: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(record)) {
    result[k] = normalizeExtendedJson(v);
  }
  return result;
}
