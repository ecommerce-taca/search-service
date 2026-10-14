import {
  IndicesIndexSettings,
  MappingTypeMapping,
} from '@elastic/elasticsearch/lib/api/types';

export const PRODUCTS_INDEX_SETTINGS_V1: IndicesIndexSettings = {
  index: {
    refresh_interval: '1s',
  },
  analysis: {
    analyzer: {
      vi_baseline: {
        type: 'custom',
        tokenizer: 'standard',
        filter: ['lowercase', 'vi_asciifolding'],
      },
    },
    filter: {
      vi_asciifolding: {
        type: 'asciifolding',
        preserve_original: true,
      },
    },
  },
};

export const PRODUCTS_INDEX_MAPPING_V1: MappingTypeMapping = {
  dynamic: 'strict',
  properties: {
    product_id: { type: 'keyword' },
    shop_id: { type: 'keyword' },
    slug: { type: 'keyword' },
    category_ids: { type: 'keyword' },
    category_path: { type: 'keyword' },
    brand: { type: 'keyword' },
    currency: { type: 'keyword' },
    visibility_status: { type: 'keyword' },
    source_event_id: { type: 'keyword' },
    title: {
      type: 'text',
      analyzer: 'vi_baseline',
    },
    title_exact: { type: 'keyword' },
    description: { type: 'text' },
    price: { type: 'long' },
    rating_avg: { type: 'double' },
    rating_count: { type: 'integer' },
    sales_count: { type: 'long' },
    attributes: { type: 'flattened' },
    spec_attributes: { type: 'flattened' },
    attribute_pairs: { type: 'keyword' },
    variant_skus: { type: 'object', enabled: false },
    suggest: {
      type: 'completion',
      analyzer: 'vi_baseline',
    },
    source_version: { type: 'long' },
    indexed_at: { type: 'date' },
    created_at: { type: 'date' },
    cover_url: { type: 'keyword', index: false },
    shop: {
      properties: {
        shop_id: { type: 'keyword' },
        name: {
          type: 'text',
          fields: {
            keyword: { type: 'keyword' },
          },
        },
      },
    },
  },
};
