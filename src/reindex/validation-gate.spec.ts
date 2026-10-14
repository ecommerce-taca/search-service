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
    it('passes when diff is within 0.1% (10000 vs 10005)', () => {
      const res = gate.checkDocumentCount(10005, 10000);
      expect(res.status).toBe('PASS');
      expect(res.pass).toBe(true);
    });

    it('fails when diff exceeds 0.1% (9980 vs 10000)', () => {
      const res = gate.checkDocumentCount(9980, 10000);
      expect(res.status).toBe('FAIL');
      expect(res.pass).toBe(false);
      expect(res.reason).toBe(ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH);
    });

    it('fails when target is 0 but active has documents', () => {
      const res = gate.checkDocumentCount(0, 48213);
      expect(res.status).toBe('FAIL');
      expect(res.reason).toBe(ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH);
    });

    it('passes when active is 0', () => {
      const res = gate.checkDocumentCount(0, 0);
      expect(res.status).toBe('PASS');
    });
  });

  describe('checkSourceVersions', () => {
    it('passes when all target doc source versions are >= active', async () => {
      const targetDocs = [
        { product_id: 'p-1', source_version: 10 },
        { product_id: 'p-2', source_version: 5 },
      ];
      const activeDocs = [
        { product_id: 'p-1', source_version: 10 },
        { product_id: 'p-2', source_version: 4 },
      ];

      const res = await gate.checkSourceVersions(targetDocs, activeDocs);
      expect(res.status).toBe('PASS');
    });

    it('fails with REINDEX_GATE_VERSION_REGRESSION when target source version < active', async () => {
      const targetDocs = [{ product_id: 'p-1', source_version: 7 }];
      const activeDocs = [{ product_id: 'p-1', source_version: 8 }];

      const res = await gate.checkSourceVersions(targetDocs, activeDocs);
      expect(res.status).toBe('FAIL');
      expect(res.reason).toBe(ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION);
    });
  });

  describe('runSmokeQueries', () => {
    it('passes when smoke queries succeed with no shard failures and only published docs', async () => {
      (mockClient.search as jest.Mock).mockResolvedValueOnce({
        _shards: { total: 1, successful: 1, failed: 0, skipped: 0 },
        hits: {
          total: { value: 1 },
          hits: [{ _source: { visibility_status: 'PUBLISHED' } }],
        },
      });

      const res = await gate.runSmokeQueries('target-index');
      expect(res.status).toBe('PASS');
    });

    it('fails with REINDEX_GATE_SMOKE_FAILED when shard failures occur', async () => {
      (mockClient.search as jest.Mock).mockResolvedValueOnce({
        _shards: { total: 2, successful: 1, failed: 1, skipped: 0 },
        hits: { total: { value: 0 }, hits: [] },
      });

      const res = await gate.runSmokeQueries('target-index');
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
      });

      const res = await gate.runSmokeQueries('target-index');
      expect(res.status).toBe('FAIL');
      expect(res.reason).toBe(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
    });
  });
});
