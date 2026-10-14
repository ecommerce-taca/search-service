export interface PriceBucketDefinition {
  key: string;
  min: number;
  max: number | null;
  from?: number;
  to?: number;
}

export const PRICE_BUCKETS: readonly PriceBucketDefinition[] = [
  {
    key: '*-5000000.0',
    min: 0,
    max: 5000000,
    to: 5000000,
  },
  {
    key: '5000000.0-20000000.0',
    min: 5000000,
    max: 20000000,
    from: 5000000,
    to: 20000000,
  },
  {
    key: '20000000.0-*',
    min: 20000000,
    max: null,
    from: 20000000,
  },
] as const;
