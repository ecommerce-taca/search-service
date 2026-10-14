import { Client } from '@elastic/elasticsearch';
import { ConsumerState, ReindexState } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';
import { IConsumerStatusProvider } from '../event-consumer/interfaces';
import { ReindexErrorCode, ReindexSource } from './enums';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';
import { ReindexRunner } from './reindex.runner';
import { ReindexService } from './reindex.service';

describe('ReindexService', () => {
  let service: ReindexService;
  let mockClient: jest.Mocked<Partial<Client>>;
  let mockConfigService: jest.Mocked<Partial<AppConfigService>>;
  let mockJobRepo: jest.Mocked<Partial<ReindexJobRepository>>;
  let mockRunner: jest.Mocked<Partial<ReindexRunner>>;
  let mockConsumerStatus: jest.Mocked<IConsumerStatusProvider>;

  beforeEach(() => {
    mockClient = {
      indices: {
        getAlias: jest.fn().mockResolvedValue({
          'test-products-v1-20260815': {},
        }),
        create: jest.fn().mockResolvedValue({}),
        delete: jest.fn().mockResolvedValue({}),
        updateAliases: jest.fn().mockResolvedValue({}),
      } as unknown as Client['indices'],
      count: jest.fn().mockResolvedValue({ count: 48213 }),
    };

    mockConfigService = {
      esIndexPrefix: 'test-',
    };

    mockJobRepo = {
      findActiveJob: jest.fn().mockResolvedValue(null),
      acquireLock: jest.fn().mockResolvedValue(undefined),
      releaseLock: jest.fn().mockResolvedValue(true),
      create: jest.fn().mockResolvedValue(undefined),
      get: jest.fn(),
      update: jest.fn(),
      latest: jest.fn(),
    };

    mockRunner = {
      executeJob: jest.fn().mockResolvedValue(undefined),
    };

    mockConsumerStatus = {
      getStatus: jest.fn().mockResolvedValue({
        state: ConsumerState.RUNNING,
        lagByTopic: { 'product.events.v1': 0, 'sku.events.v1': 2 },
        maxLagSeconds: 5,
        lastEventAt: '2026-08-30T08:59:58Z',
        dlqCount: 0,
      }),
    };

    service = new ReindexService(
      mockClient as Client,
      mockConfigService as AppConfigService,
      mockJobRepo as ReindexJobRepository,
      mockRunner as ReindexRunner,
      mockConsumerStatus,
    );
  });

  describe('generateTargetIndexName', () => {
    it('generates name matching ^products-v1-[0-9]{8,14}$', () => {
      const name = ReindexService.generateTargetIndexName(
        1,
        '2026-08-30T09:00:00Z',
        '',
      );
      expect(name).toMatch(/^products-v1-[0-9]{8,14}$/);
      expect(name).toBe('products-v1-20260830090000');
    });
  });

  describe('start', () => {
    it('throws SEARCH_INVALID_INPUT when mapping_version is not 1', async () => {
      await expect(
        service.start({
          source: ReindexSource.INDEX_COPY,
          mapping_version: 2,
          reason: 'Try mapping 2',
        }),
      ).rejects.toThrow(SearchException);

      try {
        await service.start({
          source: ReindexSource.INDEX_COPY,
          mapping_version: 2,
          reason: 'Try mapping 2',
        });
      } catch (err: unknown) {
        expect((err as SearchException).code).toBe(
          SearchErrorCode.SEARCH_INVALID_INPUT,
        );
      }
    });

    it('throws SEARCH_REINDEX_CONFLICT when active job already exists', async () => {
      (mockJobRepo.findActiveJob as jest.Mock).mockResolvedValueOnce({
        job_id: 'job-active-001',
        state: ReindexState.RUNNING,
      });

      await expect(
        service.start({
          source: ReindexSource.INDEX_COPY,
          mapping_version: 1,
          reason: 'Valid trigger',
        }),
      ).rejects.toThrow(SearchException);
    });

    it('creates target index, persists job REQUESTED, and returns 202 response data', async () => {
      const res = await service.start({
        source: ReindexSource.INDEX_COPY,
        mapping_version: 1,
        reason: 'Regular update',
      });

      expect(res.job_id).toMatch(/^job-/);
      expect(res.state).toBe(ReindexState.REQUESTED);
      expect(res.target_index).toMatch(/^test-products-v1-[0-9]{8,14}$/);

      expect(mockJobRepo.acquireLock).toHaveBeenCalled();
      expect(mockClient.indices!.create).toHaveBeenCalled();
      expect(mockJobRepo.create).toHaveBeenCalledWith(
        expect.objectContaining({
          state: ReindexState.REQUESTED,
          total_estimate: 48213,
        }),
      );
      expect(mockRunner.executeJob).toHaveBeenCalled();
    });
  });

  describe('cancel', () => {
    it('rejects cancellation of SWAPPED job with SEARCH_REINDEX_CONFLICT', async () => {
      (mockJobRepo.get as jest.Mock).mockResolvedValue({
        doc: {
          job_id: 'job-001',
          state: ReindexState.SWAPPED,
          target_index: 'test-products-v1-20261014120000',
        },
      });

      await expect(service.cancel('job-001')).rejects.toThrow(SearchException);
      try {
        await service.cancel('job-001');
      } catch (err: unknown) {
        expect((err as SearchException).code).toBe(
          SearchErrorCode.SEARCH_REINDEX_CONFLICT,
        );
      }
    });

    it('cancels RUNNING job, deletes target index, and releases lock', async () => {
      const runningDoc: ReindexJobDocument = {
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

      (mockJobRepo.get as jest.Mock).mockResolvedValueOnce({
        doc: runningDoc,
        _seq_no: 1,
        _primary_term: 1,
      });

      const res = await service.cancel('job-001');
      expect(res.state).toBe(ReindexState.CANCELLED);
      expect(mockClient.indices!.delete).toHaveBeenCalledWith({
        index: 'test-products-v1-20261014120000',
      });
      expect(mockJobRepo.releaseLock).toHaveBeenCalledWith('job-001');
    });
  });

  describe('getJob', () => {
    it('throws 404 SEARCH_REINDEX_NOT_FOUND when job is unknown', async () => {
      (mockJobRepo.get as jest.Mock).mockRejectedValueOnce(
        new SearchException(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND),
      );

      await expect(service.getJob('job-unknown-9999')).rejects.toThrow(
        SearchException,
      );
    });

    it('marks timed-out RUNNING job as FAILED REINDEX_TIMEOUT on read', async () => {
      const staleTime = new Date(Date.now() - 20 * 60 * 1000).toISOString();
      const runningDoc: ReindexJobDocument = {
        job_id: 'job-stale-001',
        target_index: 'test-products-v1-20261014120000',
        mapping_version: 1,
        state: ReindexState.RUNNING,
        checkpoint: null,
        total_estimate: 100,
        processed_count: 50,
        failed_count: 0,
        started_at: staleTime,
        finished_at: null,
        updated_at: staleTime,
        error_code: null,
      };

      (mockJobRepo.get as jest.Mock).mockResolvedValueOnce({
        doc: runningDoc,
        _seq_no: 1,
        _primary_term: 1,
      });

      const res = await service.getJob('job-stale-001');
      expect(res.state).toBe(ReindexState.FAILED);
      expect(res.error_code).toBe(ReindexErrorCode.REINDEX_TIMEOUT);
      expect(mockJobRepo.releaseLock).toHaveBeenCalledWith('job-stale-001');
    });
  });

  describe('getStatus', () => {
    it('returns virtual IDLE when no job exists', async () => {
      (mockJobRepo.latest as jest.Mock).mockResolvedValueOnce(null);

      const status = await service.getStatus();
      expect(status.reindex_state).toBe('IDLE');
      expect(status.alias).toBe('test-products-read');
      expect(status.active_index).toBe('test-products-v1-20260815');
      expect(status.document_count).toBe(48213);
      expect(status.consumer_lag).toEqual({
        'product.events.v1': 0,
        'sku.events.v1': 2,
      });
      expect(status.dlq_count).toBe(0);
    });
  });

  describe('executeAliasSwap & rollbackAliasSwap', () => {
    it('executes atomic remove + add on alias swap', async () => {
      await service.executeAliasSwap('old-idx', 'new-idx', 'alias');
      expect(mockClient.indices!.updateAliases).toHaveBeenCalledWith({
        actions: [
          { remove: { index: 'old-idx', alias: 'alias' } },
          { add: { index: 'new-idx', alias: 'alias' } },
        ],
      });
    });

    it('executes rollback alias swap', async () => {
      await service.rollbackAliasSwap(
        'current-active',
        'previous-idx',
        'alias',
      );
      expect(mockClient.indices!.updateAliases).toHaveBeenCalledWith({
        actions: [
          { remove: { index: 'current-active', alias: 'alias' } },
          { add: { index: 'previous-idx', alias: 'alias' } },
        ],
      });
    });
  });
});
