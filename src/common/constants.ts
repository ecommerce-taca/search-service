export const PAGE_SIZE_DEFAULT = 20;
export const PAGE_SIZE_MAX = 100;
export const SUGGEST_LIMIT = 10;
export const QUERY_MAX_LENGTH = 200;
export const PREFIX_MAX_LENGTH = 100;
export const ES_REQUEST_TIMEOUT_MS = 2000;
export const ES_BULK_BATCH_SIZE = 500;
export const ES_REFRESH_INTERVAL = '1s';
export const EVENT_RETRY_COUNT = 3;
export const EVENT_RETRY_BACKOFF_MS = 2000;
export const EVENT_MAX_LAG_ALERT_S = 60;
export const INTERNAL_TIMEOUT_MS = 5000;
export const MAPPING_VERSION = 1;

export const KAFKA_TOPICS = {
  PRODUCT_EVENTS: 'product.events.v1',
  SKU_EVENTS: 'sku.events.v1',
  CATEGORY_EVENTS: 'category.events.v1',
  CATALOG_EVENTS: 'catalog.events.v1',
  RATING_EVENTS: 'rating.events.v1',
  DLQ: 'search.events.dlq.v1',
} as const;

export function getKafkaTopic(topic: string, prefix = ''): string {
  return prefix ? `${prefix}${topic}` : topic;
}

export const BASE_INDEXES = {
  PRODUCTS_ALIAS: 'products-read',
  CONSUMER_STATE: 'search-consumer-state',
  REINDEX_JOBS: 'search-reindex-jobs',
  SEARCH_CATEGORIES: 'search-categories',
} as const;

export function formatUtcTimestamp(date: Date = new Date()): string {
  const yyyy = date.getUTCFullYear().toString();
  const MM = (date.getUTCMonth() + 1).toString().padStart(2, '0');
  const dd = date.getUTCDate().toString().padStart(2, '0');
  const HH = date.getUTCHours().toString().padStart(2, '0');
  const mm = date.getUTCMinutes().toString().padStart(2, '0');
  const ss = date.getUTCSeconds().toString().padStart(2, '0');
  return `${yyyy}${MM}${dd}${HH}${mm}${ss}`;
}

export function getPrefixedIndexName(name: string, prefix = ''): string {
  return prefix ? `${prefix}${name}` : name;
}

export function getProductsAlias(prefix = ''): string {
  return getPrefixedIndexName(BASE_INDEXES.PRODUCTS_ALIAS, prefix);
}

export function getVersionedIndexName(
  mapping: number = MAPPING_VERSION,
  timestamp?: string,
  prefix = '',
): string {
  const ts = timestamp || formatUtcTimestamp();
  return getPrefixedIndexName(`products-v${mapping}-${ts}`, prefix);
}

export function getConsumerStateIndex(prefix = ''): string {
  return getPrefixedIndexName(BASE_INDEXES.CONSUMER_STATE, prefix);
}

export function getReindexJobsIndex(prefix = ''): string {
  return getPrefixedIndexName(BASE_INDEXES.REINDEX_JOBS, prefix);
}

export function getSearchCategoriesIndex(prefix = ''): string {
  return getPrefixedIndexName(BASE_INDEXES.SEARCH_CATEGORIES, prefix);
}
