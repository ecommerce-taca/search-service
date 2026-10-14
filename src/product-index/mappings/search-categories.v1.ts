import { MappingTypeMapping } from '@elastic/elasticsearch/lib/api/types';

export const SEARCH_CATEGORIES_MAPPING_V1: MappingTypeMapping = {
  dynamic: 'strict',
  properties: {
    category_id: { type: 'keyword' },
    name: { type: 'keyword' },
    parent_id: { type: 'keyword' },
    path_ids: { type: 'keyword' },
    status: { type: 'keyword' },
    source_version: { type: 'long' },
    updated_at: { type: 'date' },
  },
};
