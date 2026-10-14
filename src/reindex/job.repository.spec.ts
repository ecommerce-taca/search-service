import { Client } from '@elastic/elasticsearch';
import { Test, TestingModule } from '@nestjs/testing';
import { MAPPING_VERSION, getReindexJobsIndex } from '../common/constants';
import { ReindexState } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';

describe('ReindexJobRepository (Unit)', () => {
  let repository: ReindexJobRepository;
  let esClient: jest.Mocked<Client>;
  let configService: jest.Mocked<AppConfigService>;

  const mockIndexPrefix = 'test-reindex-';
  const expectedJobsIndex = getReindexJobsIndex(mockIndexPrefix);

  beforeEach(async () => {
    esClient = {
      index: jest.fn(),
      get: jest.fn(),
      update: jest.fn(),
      search: jest.fn(),
      delete: jest.fn(),
    } as unknown as jest.Mocked<Client>;

    configService = {
      esIndexPrefix: mockIndexPrefix,
    } as unknown as jest.Mocked<AppConfigService>;

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ReindexJobRepository,
        {
          provide: Client,
          useValue: esClient,
        },
        {
          provide: AppConfigService,
          useValue: configService,
        },
      ],
    }).compile();

    repository = module.get<ReindexJobRepository>(ReindexJobRepository);
  });

  describe('Distributed Lock Management (D21)', () => {
    const testJobId = 'job-01912fb8-0001';

    it('acquireLock creates _id: active-lock with op_type: create and strict mapping fields', async () => {
      esClient.index.mockResolvedValueOnce({
        _id: 'active-lock',
        result: 'created',
      } as never);

      await repository.acquireLock(testJobId);

      expect(esClient.index).toHaveBeenCalledWith(
        expect.objectContaining({
          index: expectedJobsIndex,
          id: 'active-lock',
          op_type: 'create',
          document: expect.objectContaining({
            job_id: testJobId,
            target_index: 'active-lock',
            mapping_version: MAPPING_VERSION,
            state: 'RUNNING',
            updated_at: expect.any(String),
          }),
        }),
      );
    });

    it('acquireLock throws SEARCH_REINDEX_CONFLICT (409) when lock document already exists', async () => {
      const conflictError = new Error(
        'version_conflict_engine_exception',
      ) as Error & { statusCode: number };
      conflictError.statusCode = 409;
      esClient.index.mockRejectedValueOnce(conflictError);

      await expect(repository.acquireLock(testJobId)).rejects.toThrow(
        SearchException,
      );

      try {
        await repository.acquireLock(testJobId);
      } catch (err: unknown) {
        const e = err as SearchException;
        expect(e.code).toBe(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
        expect(e.getStatus()).toBe(409);
      }
    });

    it('releaseLock deletes active-lock only when caller is the current holder', async () => {
      esClient.get.mockResolvedValueOnce({
        _id: 'active-lock',
        _source: {
          job_id: testJobId,
          target_index: 'active-lock',
          state: 'RUNNING',
        },
      } as never);
      esClient.delete.mockResolvedValueOnce({ result: 'deleted' } as never);

      await repository.releaseLock(testJobId);

      expect(esClient.get).toHaveBeenCalledWith({
        index: expectedJobsIndex,
        id: 'active-lock',
      });
      expect(esClient.delete).toHaveBeenCalledWith({
        index: expectedJobsIndex,
        id: 'active-lock',
      });
    });

    it('releaseLock does NOT delete active-lock if held by another job', async () => {
      esClient.get.mockResolvedValueOnce({
        _id: 'active-lock',
        _source: {
          job_id: 'other-job-9999',
          target_index: 'active-lock',
          state: 'RUNNING',
        },
      } as never);

      await repository.releaseLock(testJobId);

      expect(esClient.delete).not.toHaveBeenCalled();
    });

    it('releaseLock safely ignores if active-lock document does not exist', async () => {
      const notFoundError = new Error('document_missing_exception') as Error & {
        statusCode: number;
      };
      notFoundError.statusCode = 404;
      esClient.get.mockRejectedValueOnce(notFoundError);

      await expect(repository.releaseLock(testJobId)).resolves.not.toThrow();
      expect(esClient.delete).not.toHaveBeenCalled();
    });
  });

  describe('Active Job Conflict Checks & Queries', () => {
    it('[RX-U-16] Reindex conflict check when job in state REQUESTED exists', async () => {
      esClient.search.mockResolvedValueOnce({
        hits: {
          hits: [
            {
              _id: 'job-1',
              _source: {
                job_id: 'job-1',
                state: ReindexState.REQUESTED,
                mapping_version: 1,
              },
            },
          ],
        },
      } as never);

      const hasActive = await repository.hasActiveJob();
      expect(hasActive).toBe(true);
    });

    it('[RX-U-17] Reindex conflict check when job in state RUNNING exists', async () => {
      esClient.search.mockResolvedValueOnce({
        hits: {
          hits: [
            {
              _id: 'job-1',
              _source: {
                job_id: 'job-1',
                state: ReindexState.RUNNING,
                mapping_version: 1,
              },
            },
          ],
        },
      } as never);

      const hasActive = await repository.hasActiveJob();
      expect(hasActive).toBe(true);
    });

    it('[RX-U-18] Reindex conflict check when job in state VALIDATING exists', async () => {
      esClient.search.mockResolvedValueOnce({
        hits: {
          hits: [
            {
              _id: 'job-1',
              _source: {
                job_id: 'job-1',
                state: ReindexState.VALIDATING,
                mapping_version: 1,
              },
            },
          ],
        },
      } as never);

      const hasActive = await repository.hasActiveJob();
      expect(hasActive).toBe(true);
    });

    it('[RX-U-19] Reindex conflict check when latest job is in state SWAPPED allows new job', async () => {
      esClient.search.mockResolvedValueOnce({
        hits: {
          hits: [],
        },
      } as never);

      const hasActive = await repository.hasActiveJob();
      expect(hasActive).toBe(false);
    });

    it('[RX-U-20] Reindex conflict check when latest job is in state FAILED allows new job', async () => {
      esClient.search.mockResolvedValueOnce({
        hits: {
          hits: [],
        },
      } as never);

      const hasActive = await repository.hasActiveJob();
      expect(hasActive).toBe(false);
    });

    it('[RX-U-21] Reindex conflict check when latest job is in state CANCELLED allows new job', async () => {
      esClient.search.mockResolvedValueOnce({
        hits: {
          hits: [],
        },
      } as never);

      const hasActive = await repository.hasActiveJob();
      expect(hasActive).toBe(false);
    });

    it('queries strictly exclude _id: active-lock from all search and latest() lookups', async () => {
      esClient.search.mockResolvedValueOnce({
        hits: {
          hits: [
            {
              _id: 'job-01912fb8-0001',
              _source: {
                job_id: 'job-01912fb8-0001',
                state: ReindexState.SWAPPED,
                mapping_version: 1,
              },
            },
          ],
        },
      } as never);

      await repository.latest();

      expect(esClient.search).toHaveBeenCalledWith(
        expect.objectContaining({
          index: expectedJobsIndex,
          query: expect.objectContaining({
            bool: expect.objectContaining({
              must_not: expect.arrayContaining([
                expect.objectContaining({
                  ids: { values: ['active-lock'] },
                }),
              ]),
            }),
          }),
        }),
      );
    });
  });

  describe('CRUD & Optimistic Concurrency Control', () => {
    it('create indexes job document with job_id as document _id', async () => {
      const jobDoc: ReindexJobDocument = {
        job_id: 'job-01912fb8-0001',
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Mapping update',
        state: ReindexState.REQUESTED,
        checkpoint: null,
        processed_count: 0,
        failed_count: 0,
        total_estimate: 48213,
        started_at: '2026-10-14T12:00:00Z',
        finished_at: null,
        updated_at: '2026-10-14T12:00:00Z',
        error_code: null,
        trace_id: 'trace-1',
        request_id: 'req-1',
      };

      esClient.index.mockResolvedValueOnce({
        _id: jobDoc.job_id,
        _seq_no: 1,
        _primary_term: 1,
      } as never);

      await repository.create(jobDoc);

      const expectedDoc = { ...jobDoc };
      delete expectedDoc.source;
      delete expectedDoc.reason;
      expect(esClient.index).toHaveBeenCalledWith({
        index: expectedJobsIndex,
        id: jobDoc.job_id,
        document: expectedDoc,
        refresh: 'wait_for',
      });
    });

    it('[R1 Techlead Fix] does not expose ensureMapping and never modifies shared mapping at runtime', async () => {
      const repo = repository as unknown as Record<string, unknown>;
      expect(repo.ensureMapping).toBeUndefined();
      expect(repo.mappingChecked).toBeUndefined();
    });

    it('[RX-U-36] Checkpoint persistence: batch indexing progress updates checkpoint and processed_count', async () => {
      esClient.update.mockResolvedValueOnce({
        _id: 'job-01912fb8-0001',
        result: 'updated',
        _seq_no: 2,
        _primary_term: 1,
      } as never);

      await repository.updateProgress('job-01912fb8-0001', {
        checkpoint: 'product-01912f9a',
        processed_count: 500,
        failed_count: 0,
      });

      expect(esClient.update).toHaveBeenCalledWith(
        expect.objectContaining({
          index: expectedJobsIndex,
          id: 'job-01912fb8-0001',
          doc: expect.objectContaining({
            checkpoint: 'product-01912f9a',
            processed_count: 500,
            updated_at: expect.any(String),
          }),
        }),
      );
    });

    it('update with optimistic concurrency passes if_seq_no and if_primary_term', async () => {
      esClient.index.mockResolvedValueOnce({
        _id: 'job-01912fb8-0001',
        _seq_no: 3,
        _primary_term: 1,
      } as never);

      await repository.update(
        {
          job_id: 'job-01912fb8-0001',
          state: ReindexState.RUNNING,
        } as unknown as ReindexJobDocument,
        {
          ifSeqNo: 2,
          ifPrimaryTerm: 1,
        },
      );

      expect(esClient.index).toHaveBeenCalledWith(
        expect.objectContaining({
          index: expectedJobsIndex,
          id: 'job-01912fb8-0001',
          if_seq_no: 2,
          if_primary_term: 1,
        }),
      );
    });

    it('get returns job document or null if not found or if id is active-lock', async () => {
      esClient.get.mockResolvedValueOnce({
        _id: 'job-01912fb8-0001',
        _source: {
          job_id: 'job-01912fb8-0001',
          state: ReindexState.RUNNING,
        },
        _seq_no: 1,
        _primary_term: 1,
      } as never);

      const job = await repository.get('job-01912fb8-0001');
      expect(job).not.toBeNull();
      expect(job?.job_id).toBe('job-01912fb8-0001');

      // Attempting to get active-lock directly via repository.get returns null
      const lockDoc = await repository.get('active-lock');
      expect(lockDoc).toBeNull();
    });
  });
});
