import { MappingTypeMapping } from '@elastic/elasticsearch/lib/api/types';

export const CONSUMER_STATE_MAPPING_V1: MappingTypeMapping = {
  dynamic: 'strict',
  properties: {
    event_id: { type: 'keyword' },
    aggregate_id: { type: 'keyword' },
    source_version: { type: 'long' },
    processed_at: { type: 'date' },
    status: { type: 'keyword' },
    error_code: { type: 'keyword' },
    // Events parked until their product document exists (status PENDING).
    topic: { type: 'keyword' },
    occurred_at: { type: 'date' },
    event: { type: 'object', enabled: false },
    replay_of_event_id: { type: 'keyword' },
  },
};
