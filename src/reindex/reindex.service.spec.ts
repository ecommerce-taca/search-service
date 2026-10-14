import { Client } from '@elastic/elasticsearch';
import { Test, TestingModule } from '@nestjs/testing';
import {
  MAPPING_VERSION,
  getProductsAlias,
  getVersionedIndexName,
} from '../common/constants';
import { RequestContext } from '../common/context/request-context';
import { ReindexState } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';
import { CONSUMER_STATUS_PROVIDER } from '../event-consumer/interfaces';
import { ReindexRequestDto, ReindexSource } from './dto/reindex-request.dto';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';
import { ReindexRunner } from './reindex.runner';
import {
  REINDEX_ERROR_CODES,
  ReindexErrorCode,
  ReindexService,
} from './reindex.service';

describe('ReindexService (Unit)', () => {
  let service: ReindexService;
  let jobRepo: jest.Mocked<ReindexJobRepository>;
  let esClient: jest.Mocked<Client>;
  let runner: jest.Mocked<ReindexRunner>;
  let configService: jest.Mocked<AppConfigService>;
  let consumerStatusProvider: any;

  const mockPrefix = 'test-reindex-';
  const aliasName = getProductsAlias(mockPrefix);

  beforeEach(async () => {
    jobRepo = {
      acquireLock: jest.fn().mockResolvedValue(undefined),
      releaseLock: jest.fn().mockResolvedValue(undefined),
      hasActiveJob: jest.fn().mockResolvedValue(false),
      create: jest.fn().mockImplementation(async (doc) => doc),
      get: jest.fn(),
      update: jest.fn().mockImplementation(async (doc) => doc),
      updateProgress: jest.fn().mockResolvedValue(undefined),
      latest: jest.fn(),
    } as unknown as jest.Mocked<ReindexJobRepository>;

    esClient = {
      indices: {
        existsAlias: jest.fn().mockResolvedValue(true),
        getAlias: jest.fn().mockResolvedValue({
          [`${mockPrefix}products-v1-20261014100000`]: {},
        }),
        create: jest.fn().mockResolvedValue({ acknowledged: true }),
        delete: jest.fn().mockResolvedValue({ acknowledged: true }),
        updateAliases: jest.fn().mockResolvedValue({ acknowledged: true }),
      },
      count: jest.fn().mockResolvedValue({ count: 48213 }),
    } as unknown as jest.Mocked<Client>;

    runner = {
      run: jest.fn().mockResolvedValue(undefined),
      abort: jest.fn(),
    } as unknown as jest.Mocked<ReindexRunner>;

    configService = {
      esIndexPrefix: mockPrefix,
    } as unknown as jest.Mocked<AppConfigService>;

    consumerStatusProvider = {
      getStatus: jest.fn().mockResolvedValue({
        lastEventAt: '2026-10-14T12:00:00Z',
        lagByTopic: { 'product.events.v1': 0, 'sku.events.v1': 2 },
        dlqCount: 0,
      }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ReindexService,
        { provide: ReindexJobRepository, useValue: jobRepo },
        { provide: Client, useValue: esClient },
        { provide: ReindexRunner, useValue: runner },
        { provide: AppConfigService, useValue: configService },
        {
          provide: CONSUMER_STATUS_PROVIDER,
          useValue: consumerStatusProvider,
        },
      ],
    }).compile();

    service = module.get<ReindexService>(ReindexService);
  });

  describe('Naming & Version Helpers', () => {
    it('[RX-U-14] ReindexService.generateTargetIndexName generates string matching products-v{mapping}-{yyyyMMddHHmmss} (D18)', () => {
      const fixedDate = new Date('2026-10-14T12:30:45.000Z');
      const indexName = service.generateTargetIndexName(1, fixedDate);

      expect(indexName).toMatch(/^products-v1-[0-9]{14}$/);
      expect(indexName).toBe('products-v1-20261014123045');
    });

    it('[RX-U-15] Client attempts to force custom index name via request: ignored, always server-generated', async () => {
      const payload: ReindexRequestDto = {
        source: ReindexSource.INDEX_COPY,
        mapping_version: MAPPING_VERSION,
        reason: 'Upgrade index',
      };
      // Inject unexpected custom property
      (payload as any).target_index = 'my-custom-forced-index';

      const result = await service.start(payload);
      expect(result.target_index).not.toBe('my-custom-forced-index');
      expect(result.target_index).toMatch(
        new RegExp(`^${mockPrefix}products-v1-[0-9]{14}$`),
      );
    });
  });

  describe('start() Lifecycle', () => {
    it('acquires lock, creates target index, sets total_estimate from active index, and invokes runner asynchronously', async () => {
      const dto: ReindexRequestDto = {
        source: ReindexSource.INDEX_COPY,
        mapping_version: 1,
        reason: 'Regular index rebuild',
      };

      const contextData = {
        requestId: 'req-test-123',
        traceId: 'trace-test-456',
        spanId: 'span-test-789',
        traceparent: '00-trace-span-01',
        actor: { roles: ['SEARCH_ADMIN'], permissions: [], mfaStepUp: true },
      };

      const result = await RequestContext.run(contextData, () =>
        service.start(dto),
      );

      expect(result.job_id).toMatch(/^job-[0-9a-f-]{36}$/);
      expect(result.state).toBe(ReindexState.REQUESTED);
      expect(result.target_index).toContain('products-v1-');

      expect(jobRepo.acquireLock).toHaveBeenCalledWith(result.job_id);
      expect(esClient.indices.create).toHaveBeenCalledWith(
        expect.objectContaining({
          index: result.target_index,
        }),
      );
      expect(jobRepo.create).toHaveBeenCalledWith(
        expect.objectContaining({
          job_id: result.job_id,
          state: ReindexState.REQUESTED,
          total_estimate: 48213,
          request_id: 'req-test-123',
          trace_id: 'trace-test-456',
        }),
      );

      // Async execution via runner
      expect(runner.run).toHaveBeenCalledWith(
        expect.objectContaining({
          job_id: result.job_id,
        }),
      );
    });

    it('sets total_estimate to null if active index does not exist (D20)', async () => {
      esClient.indices.existsAlias.mockResolvedValueOnce(false);

      const dto: ReindexRequestDto = {
        source: ReindexSource.INDEX_COPY,
        mapping_version: 1,
        reason: 'Bootstrap initial index',
      };

      await service.start(dto);

      expect(jobRepo.create).toHaveBeenCalledWith(
        expect.objectContaining({
          total_estimate: null,
        }),
      );
    });
  });

  describe('cancel() Flow (D17, S-API-12)', () => {
    const validJobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001';

    it('cancels active job in RUNNING state, signals runner, deletes target index, and releases lock', async () => {
      const activeJob: ReindexJobDocument = {
        job_id: validJobId,
        target_index: `${mockPrefix}products-v1-20261014120000`,
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.RUNNING,
        checkpoint: null,
        processed_count: 100,
        failed_count: 0,
        total_estimate: 1000,
        started_at: '2026-10-14T11:55:00Z',
        finished_at: null,
        updated_at: '2026-10-14T11:56:00Z',
        error_code: null,
        trace_id: 'trace-1',
        request_id: 'req-1',
      };

      jobRepo.get.mockResolvedValueOnce(activeJob);

      const cancelledJob = await service.cancel(validJobId);

      expect(runner.abort).toHaveBeenCalledWith(validJobId);
      expect(esClient.indices.delete).toHaveBeenCalledWith({
        index: activeJob.target_index,
      });
      expect(jobRepo.releaseLock).toHaveBeenCalledWith(validJobId);
      expect(cancelledJob.state).toBe(ReindexState.CANCELLED);
      expect(cancelledJob.finished_at).toBeDefined();
      expect(cancelledJob.error_code).toBeNull();
    });

    it('throws SEARCH_REINDEX_NOT_FOUND (404) when cancelling non-existent jobId', async () => {
      jobRepo.get.mockResolvedValueOnce(null);

      await expect(service.cancel(validJobId)).rejects.toThrow(SearchException);
      try {
        await service.cancel(validJobId);
      } catch (err: any) {
        expect(err.code).toBe(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
        expect(err.getStatus()).toBe(404);
      }
    });

    it('throws SEARCH_REINDEX_CONFLICT (409) when cancelling already SWAPPED or FAILED job', async () => {
      const swappedJob: ReindexJobDocument = {
        job_id: validJobId,
        target_index: 'index-1',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.SWAPPED,
        checkpoint: null,
        processed_count: 1000,
        failed_count: 0,
        total_estimate: 1000,
        started_at: '2026-10-14T10:00:00Z',
        finished_at: '2026-10-14T10:30:00Z',
        updated_at: '2026-10-14T10:30:00Z',
        error_code: null,
        trace_id: 't',
        request_id: 'r',
      };

      jobRepo.get.mockResolvedValueOnce(swappedJob);

      await expect(service.cancel(validJobId)).rejects.toThrow(SearchException);
      try {
        await service.cancel(validJobId);
      } catch (err: any) {
        expect(err.code).toBe(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
        expect(err.getStatus()).toBe(409);
      }
    });
  });

  describe('getJob() by ID', () => {
    it('validates jobId format (job-<uuid v4>) and rejects arbitrary IDs with 404 without querying DB', async () => {
      const invalidJobIds = [
        'invalid-format',
        'job-12345',
        'job-../../etc/passwd',
        'job-xyz',
      ];

      for (const invalidId of invalidJobIds) {
        await expect(service.getJob(invalidId)).rejects.toThrow(
          SearchException,
        );
      }
      expect(jobRepo.get).not.toHaveBeenCalled();
    });

    it('returns job details matching api §3.6 shape for valid jobId', async () => {
      const validUuidJobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001';
      const existingJob: ReindexJobDocument = {
        job_id: validUuidJobId,
        target_index: `${mockPrefix}products-v1-20261014120000`,
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Mapping update',
        state: ReindexState.RUNNING,
        checkpoint: 'product-01912f9a',
        processed_count: 32000,
        failed_count: 4,
        total_estimate: 48213,
        started_at: '2026-10-14T11:55:00Z',
        finished_at: null,
        updated_at: '2026-10-14T12:00:00Z',
        error_code: null,
        trace_id: 'trace-1',
        request_id: 'req-1',
      };

      jobRepo.get.mockResolvedValueOnce(existingJob);

      const result = await service.getJob(validUuidJobId);

      expect(result).toEqual({
        job_id: validUuidJobId,
        state: ReindexState.RUNNING,
        target_index: existingJob.target_index,
        mapping_version: 1,
        processed_count: 32000,
        failed_count: 4,
        total_estimate: 48213,
        checkpoint: 'product-01912f9a',
        started_at: '2026-10-14T11:55:00Z',
        updated_at: '2026-10-14T12:00:00Z',
        error_code: null,
      });
    });

    it('throws SEARCH_REINDEX_NOT_FOUND (404) when valid UUID job does not exist', async () => {
      const nonExistentId = 'job-01912fb8-0000-0000-0000-000000000000';
      jobRepo.get.mockResolvedValueOnce(null);

      await expect(service.getJob(nonExistentId)).rejects.toThrow(
        SearchException,
      );
    });
  });

  describe('getStatus() & Lazy Timeout Check', () => {
    it('returns status with reindex_state: IDLE when no job has ever run', async () => {
      jobRepo.latest.mockResolvedValueOnce(null);

      const status = await service.getStatus();

      expect(status.reindex_state).toBe('IDLE');
      expect(status.alias).toBe(aliasName);
      expect(status.active_index).toBe(`${mockPrefix}products-v1-20261014100000`);
      expect(status.document_count).toBe(48213);
      expect(status.consumer_lag).toEqual({
        'product.events.v1': 0,
        'sku.events.v1': 2,
      });
      expect(status.dlq_count).toBe(0);
    });

    it('falls back to default consumer metrics when CONSUMER_STATUS_PROVIDER is absent', async () => {
      const moduleWithoutConsumer: TestingModule = await Test.createTestingModule({
        providers: [
          ReindexService,
          { provide: ReindexJobRepository, useValue: jobRepo },
          { provide: Client, useValue: esClient },
          { provide: ReindexRunner, useValue: runner },
          { provide: AppConfigService, useValue: configService },
        ],
      }).compile();

      const serviceNoConsumer =
        moduleWithoutConsumer.get<ReindexService>(ReindexService);
      jobRepo.latest.mockResolvedValueOnce(null);

      const status = await serviceNoConsumer.getStatus();
      expect(status.last_event_at).toBeNull();
      expect(status.consumer_lag).toEqual({});
      expect(status.dlq_count).toBe(0);
    });

    it('lazy check marks job FAILED with REINDEX_TIMEOUT if RUNNING with no update for > 15 minutes', async () => {
      const stuckUpdatedAt = new Date(
        Date.now() - 16 * 60 * 1000,
      ).toISOString(); // 16 min ago
      const stuckJob: ReindexJobDocument = {
        job_id: 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001',
        target_index: `${mockPrefix}products-v1-20261014110000`,
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.RUNNING,
        checkpoint: null,
        processed_count: 50,
        failed_count: 0,
        total_estimate: 1000,
        started_at: new Date(Date.now() - 20 * 60 * 1000).toISOString(),
        finished_at: null,
        updated_at: stuckUpdatedAt,
        error_code: null,
        trace_id: 'trace-1',
        request_id: 'req-1',
      };

      jobRepo.latest.mockResolvedValueOnce(stuckJob);

      const status = await service.getStatus();

      expect(status.reindex_state).toBe(ReindexState.FAILED);
      expect(jobRepo.update).toHaveBeenCalledWith(
        expect.objectContaining({
          job_id: stuckJob.job_id,
          state: ReindexState.FAILED,
          error_code: ReindexErrorCode.REINDEX_TIMEOUT,
        }),
      );
      expect(jobRepo.releaseLock).toHaveBeenCalledWith(stuckJob.job_id);
    });
  });

  describe('Alias Swap & Rollback Structure (RX-U-46, RX-U-47)', () => {
    it('[RX-U-46] Atomic alias swap payload structure: actions [{ remove: { index: oldIndex, alias } }, { add: { index: newIndex, alias } }]', async () => {
      const oldIndex = `${mockPrefix}products-v1-20260815`;
      const newIndex = `${mockPrefix}products-v1-20260830`;

      await service.executeAliasSwap(oldIndex, newIndex, aliasName);

      expect(esClient.indices.updateAliases).toHaveBeenCalledWith({
        body: {
          actions: [
            { remove: { index: oldIndex, alias: aliasName } },
            { add: { index: newIndex, alias: aliasName } },
          ],
        },
      });
    });

    it('[RX-U-47] Rollback alias swap payload structure: removes currentActiveIndex and re-adds previousIndex', async () => {
      const currentActiveIndex = `${mockPrefix}products-v1-20260830`;
      const previousIndex = `${mockPrefix}products-v1-20260815`;

      await service.rollbackAliasSwap(
        currentActiveIndex,
        previousIndex,
        aliasName,
      );

      expect(esClient.indices.updateAliases).toHaveBeenCalledWith({
        body: {
          actions: [
            { remove: { index: currentActiveIndex, alias: aliasName } },
            { add: { index: previousIndex, alias: aliasName } },
          ],
        },
      });
    });
  });

  describe('Error Code Allowlist (RX-U-48, RX-U-49, RX-U-50, D19)', () => {
    it('[RX-U-48] Error code allowlist validation: non-failed job has error_code strictly null', () => {
      const nonFailedStates = [
        ReindexState.REQUESTED,
        ReindexState.RUNNING,
        ReindexState.VALIDATING,
        ReindexState.SWAPPED,
        ReindexState.CANCELLED,
      ];

      for (const state of nonFailedStates) {
        const error = service.formatErrorCode(state, null);
        expect(error).toBeNull();
      }
    });

    it('[RX-U-49] Error code allowlist validation: unknown internal exception mapped to REINDEX_WORKER_ERROR', () => {
      const rawError = new Error(
        'MongoTimeoutException: connection timed out to cluster',
      );
      const code = service.mapExceptionToErrorCode(rawError);

      expect(code).toBe(ReindexErrorCode.REINDEX_WORKER_ERROR);
      expect(code).not.toContain('Mongo');
      expect(code).not.toContain('stack');
    });

    it('[RX-U-50] Error code allowlist enumeration enforcement: contains exactly the D19 allowlist', () => {
      const expectedCodes = [
        'REINDEX_GATE_COUNT_MISMATCH',
        'REINDEX_GATE_VERSION_REGRESSION',
        'REINDEX_GATE_SMOKE_FAILED',
        'REINDEX_SOURCE_UNAVAILABLE',
        'REINDEX_TIMEOUT',
        'REINDEX_WORKER_ERROR',
      ];

      expect(Object.values(REINDEX_ERROR_CODES)).toEqual(
        expect.arrayContaining(expectedCodes),
      );
      expect(expectedCodes).toEqual(
        expect.arrayContaining(Object.values(REINDEX_ERROR_CODES)),
      );
    });
  });
});
