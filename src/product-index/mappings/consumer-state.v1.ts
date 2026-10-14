export const CONSUMER_STATE_MAPPING_V1 = {
  dynamic: 'strict' as const,
  properties: {
    event_id: { type: 'keyword' },
    aggregate_id: { type: 'keyword' },
    source_version: { type: 'long' },
    processed_at: { type: 'date' },
    status: { type: 'keyword' },
    error_code: { type: 'keyword' },
  },
};
