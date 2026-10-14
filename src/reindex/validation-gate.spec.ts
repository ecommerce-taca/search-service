import { Client } from '@elastic/elasticsearch';
import { ReindexErrorCode } from './enums';
import { ValidationGate } from './validation-gate';

describe('ValidationGate', () => {
  let gate: ValidationGate;
  let mockClient: jest.Mocked<Partial<Client>>;

  beforeEach(() => {
    mockClient = {
      indices: {
        refresh: jest.fn().mockResolvedValue({}),
      } as unknown as Client['indices'],
      count: jest.fn(),
      search: jest.fn(),
      mget: jest.fn(),
    };
    gate = new ValidationGate(mockClient as Client);
  });

  describe('checkDocumentCount', () => {
    it('passes when diff is within 0.1% (10005 vs 10000)', async () => {
      (mockClient.count as jest.Mock).mockImplementation(
        async ({ index }: { index: string }) => {
          if (index === 'target') return { count: 10005 };
          if (index === 'active') return { count: 10000 };
          return { count: 0 };
        },
      );

      const res = await gate.checkDocumentCount('target', 'active');
      expect(res.status).toBe('PASS');
      expect(res.pass).toBe(true);
      expect(res.passed).toBe(true);
    });

    it('fails when diff exceeds 0.1% (9980 vs 10000)', async () => {
      (mockClient.count as jest.Mock).mockImplementation(
        async ({ index }: { index: string }) => {
          if (index === 'target') return { count: 9980 };
          if (index === 'active') return { count: 10000 };
          return { count: 0 };
        },
      );

      const res = await gate.checkDocumentCount('target', 'active');
      expect(res.status).toBe('FAIL');
      expect(res.pass).toBe(false);
      expect(res.reason).toBe(ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH);
    });

    it('fails when target is 0 but active has documents', async () => {
      (mockClient.count as jest.Mock).mockImplementation(
        async ({ index }: { index: string }) => {
          if (index === 'target') return { count: 0 };
          if (index === 'active') return { count: 48213 };
          return { count: 0 };
        },
      );

      const res = await gate.checkDocumentCount('target', 'active');
      expect(res.status).toBe('FAIL');
      expect(res.reason).toBe(ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH);
    });

    it('passes when active is 0', async () => {
      (mockClient.count as jest.Mock).mockResolvedValue({ count: 0 });

      const res = await gate.checkDocumentCount('target', 'active');
      expect(res.status).toBe('PASS');
      expect(res.pass).toBe(true);
    });

    it('passes when active is null', async () => {
      (mockClient.count as jest.Mock).mockResolvedValue({ count: 10 });

      const res = await gate.checkDocumentCount('target', null);
      expect(res.status).toBe('PASS');
      expect(res.pass).toBe(true);
    });
  });

  describe('checkSourceVersion', () => {
    it('passes when all target doc source versions are >= active', async () => {
      (mockClient.search as jest.Mock).mockImplementation(
        async ({ index }: { index: string }) => {
          if (index === 'active') {
            return {
              hits: {
                hits: [
                  {
                    _id: 'p-1',
                    _source: { product_id: 'p-1', source_version: 10 },
                  },
                  {
                    _id: 'p-2',
                    _source: { product_id: 'p-2', source_version: 4 },
                  },
                ],
              },
            };
          }
          if (index === 'target') {
            return {
              hits: {
                hits: [
                  {
                    _id: 'p-1',
                    _source: { product_id: 'p-1', source_version: 10 },
                  },
                  {
                    _id: 'p-2',
                    _source: { product_id: 'p-2', source_version: 5 },
                  },
                ],
              },
            };
          }
          return { hits: { hits: [] } };
        },
      );

      const res = await gate.checkSourceVersion('target', 'active');
      expect(res.status).toBe('PASS');
      expect(res.pass).toBe(true);
    });

    it('fails with REINDEX_GATE_VERSION_REGRESSION when target source version < active', async () => {
      (mockClient.search as jest.Mock).mockImplementation(
        async ({ index }: { index: string }) => {
          if (index === 'active') {
            return {
              hits: {
                hits: [
                  {
                    _id: 'p-1',
                    _source: { product_id: 'p-1', source_version: 8 },
                  },
                ],
              },
            };
          }
          if (index === 'target') {
            return {
              hits: {
                hits: [
                  {
                    _id: 'p-1',
                    _source: { product_id: 'p-1', source_version: 7 },
                  },
                ],
              },
            };
          }
          return { hits: { hits: [] } };
        },
      );

      const res = await gate.checkSourceVersion('target', 'active');
      expect(res.status).toBe('FAIL');
      expect(res.reason).toBe(ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION);
    });

    it('passes when activeIndex is null', async () => {
      const res = await gate.checkSourceVersion('target', null);
      expect(res.status).toBe('PASS');
      expect(res.pass).toBe(true);
    });
  });

  describe('checkSmokeQueries', () => {
    it('passes when smoke queries succeed with no shard failures and only published docs', async () => {
      (mockClient.search as jest.Mock).mockResolvedValueOnce({
        _shards: { total: 1, successful: 1, failed: 0, skipped: 0 },
        hits: {
          total: { value: 1 },
          hits: [{ _source: { visibility_status: 'PUBLISHED' } }],
        },
        aggregations: {
          category_ids: { buckets: [{ key: 'cat-1', doc_count: 1 }] },
          brand: { buckets: [{ key: 'brand-1', doc_count: 1 }] },
          attribute_pairs: { buckets: [{ key: 'k=v', doc_count: 1 }] },
        },
      });

      const res = await gate.checkSmokeQueries('target-index');
      expect(res.status).toBe('PASS');
      expect(res.pass).toBe(true);
    });

    it('fails with REINDEX_GATE_SMOKE_FAILED when shard failures occur', async () => {
      (mockClient.search as jest.Mock).mockResolvedValueOnce({
        _shards: { total: 2, successful: 1, failed: 1, skipped: 0 },
        hits: { total: { value: 0 }, hits: [] },
      });

      const res = await gate.checkSmokeQueries('target-index');
      expect(res.status).toBe('FAIL');
      expect(res.reason).toBe(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
    });

    it('fails with REINDEX_GATE_SMOKE_FAILED when visibility status leaks non-PUBLISHED docs', async () => {
      (mockClient.search as jest.Mock).mockResolvedValueOnce({
        _shards: { total: 1, successful: 1, failed: 0, skipped: 0 },
        hits: {
          total: { value: 1 },
          hits: [{ _source: { visibility_status: 'HIDDEN' } }],
        },
        aggregations: {
          category_ids: { buckets: [{ key: 'cat-1', doc_count: 1 }] },
          brand: { buckets: [{ key: 'brand-1', doc_count: 1 }] },
          attribute_pairs: { buckets: [{ key: 'k=v', doc_count: 1 }] },
        },
      });

      const res = await gate.checkSmokeQueries('target-index');
      expect(res.status).toBe('FAIL');
      expect(res.reason).toBe(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
    });

    it('fails with REINDEX_GATE_SMOKE_FAILED when total documents is 0', async () => {
      (mockClient.search as jest.Mock).mockResolvedValueOnce({
        _shards: { total: 1, successful: 1, failed: 0, skipped: 0 },
        hits: { total: { value: 0 }, hits: [] },
        aggregations: {
          category_ids: { buckets: [] },
          brand: { buckets: [] },
          attribute_pairs: { buckets: [] },
        },
      });

      const res = await gate.checkSmokeQueries('target-index');
      expect(res.status).toBe('FAIL');
      expect(res.reason).toBe(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
    });
  });

  describe('run (All gates orchestration)', () => {
    it('runs all gates sequentially and passes', async () => {
      (mockClient.count as jest.Mock).mockResolvedValue({ count: 100 });
      (mockClient.search as jest.Mock)
        .mockResolvedValueOnce({
          hits: {
            hits: [
              { _id: 'p-1', _source: { product_id: 'p-1', source_version: 5 } },
            ],
          },
        })
        .mockResolvedValueOnce({
          hits: {
            hits: [
              { _id: 'p-1', _source: { product_id: 'p-1', source_version: 5 } },
            ],
          },
        })
        .mockResolvedValueOnce({
          _shards: { total: 1, successful: 1, failed: 0, skipped: 0 },
          hits: {
            total: { value: 100 },
            hits: [{ _source: { visibility_status: 'PUBLISHED' } }],
          },
          aggregations: {
            category_ids: { buckets: [{ key: 'cat-1', doc_count: 1 }] },
            brand: { buckets: [{ key: 'brand-1', doc_count: 1 }] },
            attribute_pairs: { buckets: [{ key: 'k=v', doc_count: 1 }] },
          },
        });

      const res = await gate.run('target', 'active');
      expect(res.status).toBe('PASS');
      expect(res.pass).toBe(true);
      expect(mockClient.indices!.refresh).toHaveBeenCalledWith({
        index: 'target',
      });
    });

    it('short-circuits when count gate fails', async () => {
      (mockClient.count as jest.Mock).mockImplementation(
        async ({ index }: { index: string }) => {
          if (index === 'target') return { count: 0 };
          return { count: 100 };
        },
      );

      const res = await gate.run('target', 'active');
      expect(res.status).toBe('FAIL');
      expect(res.reason).toBe(ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH);
      expect(mockClient.search).not.toHaveBeenCalled();
    });
  });
});
