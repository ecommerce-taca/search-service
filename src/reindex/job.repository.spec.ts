import { Client } from '@elastic/elasticsearch';
import { MAPPING_VERSION } from '../common/constants';
import { ReindexState } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';

describe('ReindexJobRepository', () => {
  let repository: ReindexJobRepository;
  let mockClient: jest.Mocked<Partial<Client>>;
  let mockConfigService: jest.Mocked<Partial<AppConfigService>>;

  beforeEach(() => {
    mockClient = {
      index: jest.fn(),
      get: jest.fn(),
      delete: jest.fn(),
      search: jest.fn(),
      indices: {
        putMapping: jest.fn().mockResolvedValue({}),
      } as unknown as Client['indices'],
    };
    mockConfigService = {
      esIndexPrefix: 'test-',
    };
    repository = new ReindexJobRepository(
      mockClient as Client,
      mockConfigService as AppConfigService,
    );
  });

  describe('acquireLock', () => {
    it('creates active-lock document with op_type=create', async () => {
      (mockClient.index as jest.Mock).mockResolvedValueOnce({
        _id: 'active-lock',
        result: 'created',
      });

      await repository.acquireLock('job-001');

      expect(mockClient.index).toHaveBeenCalledWith(
        expect.objectContaining({
          index: 'test-search-reindex-jobs',
          id: 'active-lock',
          op_type: 'create',
          document: expect.objectContaining({
            job_id: 'job-001',
            target_index: 'active-lock',
            mapping_version: MAPPING_VERSION,
            state: ReindexState.RUNNING,
          }),
        }),
      );
    });

    it('throws SEARCH_REINDEX_CONFLICT on lock conflict', async () => {
      const conflictError = {
        meta: {
          statusCode: 409,
          body: { error: { type: 'version_conflict_engine_exception' } },
        },
      };
      (mockClient.index as jest.Mock).mockRejectedValueOnce(conflictError);

      await expect(repository.acquireLock('job-002')).rejects.toThrow(
        SearchException,
      );
      try {
        await repository.acquireLock('job-002');
      } catch (err: unknown) {
        expect((err as SearchException).code).toBe(
          SearchErrorCode.SEARCH_REINDEX_CONFLICT,
        );
      }
    });
  });

  describe('releaseLock', () => {
    it('deletes active-lock if it holds the specified jobId', async () => {
      (mockClient.get as jest.Mock).mockResolvedValueOnce({
        found: true,
        _source: { job_id: 'job-001' },
      });
      (mockClient.delete as jest.Mock).mockResolvedValueOnce({
        result: 'deleted',
      });

      const released = await repository.releaseLock('job-001');
      expect(released).toBe(true);
      expect(mockClient.delete).toHaveBeenCalledWith(
        expect.objectContaining({
          id: 'active-lock',
        }),
      );
    });

    it('does not delete active-lock if it belongs to another job', async () => {
      (mockClient.get as jest.Mock).mockResolvedValueOnce({
        found: true,
        _source: { job_id: 'job-other' },
      });

      const released = await repository.releaseLock('job-001');
      expect(released).toBe(false);
      expect(mockClient.delete).not.toHaveBeenCalled();
    });
  });

  describe('get', () => {
    it('rejects invalid jobId format or active-lock without querying ES', async () => {
      await expect(repository.get('active-lock')).rejects.toThrow(
        SearchException,
      );
      await expect(repository.get('invalid-id')).rejects.toThrow(
        SearchException,
      );
      expect(mockClient.get).not.toHaveBeenCalled();
    });

    it('returns document with seq_no and primary_term when found', async () => {
      const mockDoc: ReindexJobDocument = {
        job_id: 'job-001',
        target_index: 'test-products-v1-20261014120000',
        mapping_version: 1,
        state: ReindexState.RUNNING,
        checkpoint: null,
        total_estimate: 100,
        processed_count: 50,
        failed_count: 0,
        started_at: '2026-10-14T10:00:00Z',
        finished_at: null,
        updated_at: '2026-10-14T10:05:00Z',
        error_code: null,
      };

      (mockClient.get as jest.Mock).mockResolvedValueOnce({
        found: true,
        _id: 'job-001',
        _source: mockDoc,
        _seq_no: 5,
        _primary_term: 1,
      });

      const res = await repository.get('job-001');
      expect(res.doc.job_id).toBe('job-001');
      expect(res._seq_no).toBe(5);
      expect(res._primary_term).toBe(1);
    });
  });

  describe('latest', () => {
    it('queries excluding active-lock and sorted by started_at desc', async () => {
      (mockClient.search as jest.Mock).mockResolvedValueOnce({
        hits: {
          hits: [
            {
              _id: 'job-latest',
              _source: { job_id: 'job-latest', state: ReindexState.SWAPPED },
              _seq_no: 10,
              _primary_term: 2,
            },
          ],
        },
      });

      const latest = await repository.latest();
      expect(latest?.doc.job_id).toBe('job-latest');
      expect(mockClient.search).toHaveBeenCalledWith(
        expect.objectContaining({
          query: {
            bool: {
              must_not: [{ ids: { values: ['active-lock'] } }],
            },
          },
          sort: [{ started_at: { order: 'desc' } }],
          size: 1,
        }),
      );
    });
  });
});
