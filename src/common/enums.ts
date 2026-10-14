export enum VisibilityStatus {
  PUBLISHED = 'PUBLISHED',
  HIDDEN = 'HIDDEN',
  DELETED = 'DELETED',
}

export enum IndexDocumentState {
  CURRENT = 'CURRENT',
  STALE = 'STALE',
  FAILED = 'FAILED',
}

export enum ConsumerState {
  RUNNING = 'RUNNING',
  PAUSED = 'PAUSED',
  DEGRADED = 'DEGRADED',
  DLQ = 'DLQ',
}

export enum ReindexState {
  REQUESTED = 'REQUESTED',
  RUNNING = 'RUNNING',
  VALIDATING = 'VALIDATING',
  SWAPPED = 'SWAPPED',
  FAILED = 'FAILED',
  CANCELLED = 'CANCELLED',
}

export enum SuggestionType {
  QUERY = 'QUERY',
  PRODUCT = 'PRODUCT',
  BRAND = 'BRAND',
}

export enum SortOption {
  RELEVANCE = 'relevance',
  NEWEST = 'newest',
  PRICE_ASC = 'price_asc',
  PRICE_DESC = 'price_desc',
  RATING_DESC = 'rating_desc',
}
