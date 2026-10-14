import { MappingTypeMapping } from '@elastic/elasticsearch/lib/api/types';

export const REINDEX_JOBS_MAPPING_V1: MappingTypeMapping = {
  dynamic: 'strict',
  properties: {
    job_id: { type: 'keyword' },
    target_index: { type: 'keyword' },
    mapping_version: { type: 'integer' },
    state: { type: 'keyword' },
    checkpoint: { type: 'keyword' },
    processed_count: { type: 'long' },
    failed_count: { type: 'long' },
    total_estimate: { type: 'long' },
    started_at: { type: 'date' },
    finished_at: { type: 'date' },
    updated_at: { type: 'date' },
    error_code: { type: 'keyword' },
    trace_id: { type: 'keyword' },
    request_id: { type: 'keyword' },
  },
};
