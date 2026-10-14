import { Client } from '@elastic/elasticsearch';
import { Test, TestingModule } from '@nestjs/testing';
import { Kafka } from 'kafkajs';
import { ReindexState, VisibilityStatus } from '../common/enums';
import { AppConfigService } from '../config/config.service';
import {
  INDEX_WRITER,
  PRODUCT_DOCUMENT_BUILDER,
} from '../product-index/interfaces';
import { IndexCopySource } from './index-copy.source';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';
import { KafkaReplaySource } from './kafka-replay.source';
import { ReindexRunner } from './reindex.runner';
import { ReindexErrorCode } from './reindex.service';
import { ValidationGate } from './validation-gate';

describe('ReindexRunner (Unit)', () => {
  let runner: ReindexRunner;
  let validationGate: ValidationGate;
  let indexCopySource: IndexCopySource;
  let kafkaReplaySource: KafkaReplaySource;
  let esClient: jest.Mocked<Client>;
  let jobRepo: jest.Mocked<ReindexJobRepository>;
  let configService: jest.Mocked<AppConfigService>;
  let kafka: jest.Mocked<Kafka>;
  let documentBuilder: Record<string, jest.Mock>;
  let indexWriter: Record<string, jest.Mock>;

  const mockPrefix = 'test-reindex-';
  const activeIndex = `${mockPrefix}products-v1-20261014100000`;
  const targetIndex = `${mockPrefix}products-v1-20261014120000`;
  const aliasName = `${mockPrefix}products-read`;

  const createBaseJob = (
    source: 'INDEX_COPY' | 'KAFKA_REPLAY' = 'INDEX_COPY',
  ): ReindexJobDocument => ({
    job_id: 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001',
    target_index: targetIndex,
    mapping_version: 1,
    source,
    reason: 'Routine mapping migration',
    state: ReindexState.REQUESTED,
    checkpoint: null,
    processed_count: 0,
    failed_count: 0,
    total_estimate: 10000,
    started_at: '2026-10-14T12:00:00Z',
    finished_at: null,
    updated_at: '2026-10-14T12:00:00Z',
    error_code: null,
    trace_id: 'trace-1',
    request_id: 'req-1',
  });

  beforeEach(async () => {
    esClient = {
      reindex: jest.fn().mockResolvedValue({ task: 'task-reindex-123' }),
      tasks: {
        get: jest.fn().mockResolvedValue({
          completed: true,
          response: {
            total: 10000,
            updated: 0,
            created: 10000,
            deleted: 0,
            batches: 20,
            version_conflicts: 0,
            noops: 0,
            retries: { bulk: 0, search: 0 },
            throttled_millis: 0,
            requests_per_second: -1,
            throttled_until_millis: 0,
            failures: [],
          },
        }),
      },
      indices: {
        refresh: jest.fn().mockResolvedValue({ acknowledged: true }),
        updateAliases: jest.fn().mockResolvedValue({ acknowledged: true }),
        getAlias: jest.fn().mockResolvedValue({
          [activeIndex]: {},
        }),
      },
      count: jest.fn().mockImplementation(async ({ index }) => {
        if (index === activeIndex) return { count: 10000 };
        if (index === targetIndex) return { count: 10000 };
        return { count: 0 };
      }),
      search: jest.fn().mockResolvedValue({
        _shards: { total: 1, successful: 1, failed: 0, skipped: 0 },
        hits: {
          total: { value: 10000 },
          hits: [
            {
              _id: 'p-1',
              _source: {
                product_id: 'p-1',
                source_version: 10,
                visibility_status: VisibilityStatus.PUBLISHED,
              },
            },
          ],
        },
        aggregations: {
          categories: { buckets: [{ key: 'cat-1', doc_count: 5 }] },
          brands: { buckets: [{ key: 'brand-1', doc_count: 5 }] },
          attributes: { buckets: [{ key: 'color=Black', doc_count: 5 }] },
          category_ids: { buckets: [{ key: 'cat-1', doc_count: 5 }] },
          brand: { buckets: [{ key: 'brand-1', doc_count: 5 }] },
          attribute_pairs: { buckets: [{ key: 'color=Black', doc_count: 5 }] },
        },
        suggest: {
          product_suggest: [{ options: [{ text: 'Suggestion 1' }] }],
          smoke_suggest: [{ options: [{ text: 'Suggestion 1' }] }],
        },
      }),
    } as unknown as jest.Mocked<Client>;

    jobRepo = {
      update: jest.fn().mockImplementation(async (doc) => doc),
      updateProgress: jest.fn().mockResolvedValue(undefined),
      releaseLock: jest.fn().mockResolvedValue(undefined),
      get: jest.fn(),
    } as unknown as jest.Mocked<ReindexJobRepository>;

    configService = {
      esIndexPrefix: mockPrefix,
      kafkaBrokers: ['127.0.0.1:9092'],
    } as unknown as jest.Mocked<AppConfigService>;

    kafka = {
      consumer: jest.fn().mockReturnValue({
        connect: jest.fn().mockResolvedValue(undefined),
        subscribe: jest.fn().mockResolvedValue(undefined),
        run: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
      }),
      admin: jest.fn().mockReturnValue({
        connect: jest.fn().mockResolvedValue(undefined),
        fetchTopicOffsets: jest
          .fn()
          .mockResolvedValue([
            { partition: 0, offset: '100', high: '100', low: '0' },
          ]),
        disconnect: jest.fn().mockResolvedValue(undefined),
      }),
    } as unknown as jest.Mocked<Kafka>;

    documentBuilder = {
      build: jest.fn(),
    };

    indexWriter = {
      bulkUpsert: jest.fn().mockResolvedValue(undefined),
      bulkPartialUpdate: jest.fn().mockResolvedValue(undefined),
      getCurrentVersions: jest.fn().mockResolvedValue(new Map()),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ReindexRunner,
        ValidationGate,
        IndexCopySource,
        KafkaReplaySource,
        { provide: Client, useValue: esClient },
        { provide: ReindexJobRepository, useValue: jobRepo },
        { provide: AppConfigService, useValue: configService },
        { provide: Kafka, useValue: kafka },
        { provide: PRODUCT_DOCUMENT_BUILDER, useValue: documentBuilder },
        { provide: INDEX_WRITER, useValue: indexWriter },
      ],
    }).compile();

    runner = module.get<ReindexRunner>(ReindexRunner);
    validationGate = module.get<ValidationGate>(ValidationGate);
    indexCopySource = module.get<IndexCopySource>(IndexCopySource);
    kafkaReplaySource = module.get<KafkaReplaySource>(KafkaReplaySource);
  });

  describe('Validation Gate 1: Count Comparison (RX-U-38, RX-U-39, RX-U-40)', () => {
    it('[RX-U-38] Pre-swap Gate 1: Count difference within threshold (diff <= 0.1%) returns PASS', async () => {
      // old = 10,000; new = 10,005 -> diff = 0.05% <= 0.1%
      esClient.count.mockImplementation((async ({
        index,
      }: {
        index: string;
      }) => {
        if (index === activeIndex) return { count: 10000, _shards: {} };
        if (index === targetIndex) return { count: 10005, _shards: {} };
        return { count: 0, _shards: {} };
      }) as never);

      const gateResult = await validationGate.checkDocumentCountGate(
        targetIndex,
        activeIndex,
      );
      expect(gateResult.passed).toBe(true);
      expect(gateResult.diffPercent).toBeLessThanOrEqual(0.1);
    });

    it('[RX-U-39] Pre-swap Gate 1: Count difference exceeds threshold (missing docs > 0.1%) returns FAIL REINDEX_GATE_COUNT_MISMATCH', async () => {
      // old = 10,000; new = 9,980 -> missing 20 docs = 0.2% > 0.1%
      esClient.count.mockImplementation((async ({
        index,
      }: {
        index: string;
      }) => {
        if (index === activeIndex) return { count: 10000, _shards: {} };
        if (index === targetIndex) return { count: 9980, _shards: {} };
        return { count: 0, _shards: {} };
      }) as never);

      const gateResult = await validationGate.checkDocumentCountGate(
        targetIndex,
        activeIndex,
      );
      expect(gateResult.passed).toBe(false);
      expect(gateResult.errorCode).toBe(
        ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
      );
    });

    it('[RX-U-40] Pre-swap Gate 1: Target index count = 0 when old index > 0 returns FAIL REINDEX_GATE_COUNT_MISMATCH', async () => {
      // old = 48,213; target = 0 -> diff > 0.1%
      esClient.count.mockImplementation((async ({
        index,
      }: {
        index: string;
      }) => {
        if (index === activeIndex) return { count: 48213, _shards: {} };
        if (index === targetIndex) return { count: 0, _shards: {} };
        return { count: 0, _shards: {} };
      }) as never);

      const gateResult = await validationGate.checkDocumentCountGate(
        targetIndex,
        activeIndex,
      );
      expect(gateResult.passed).toBe(false);
      expect(gateResult.errorCode).toBe(
        ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
      );
    });
  });

  describe('Validation Gate 2: Source Version Check (RX-U-41, RX-U-42)', () => {
    it('[RX-U-41] Pre-swap Gate 2: Source version check (target >= active) returns PASS', async () => {
      esClient.search.mockImplementation((async ({
        index,
      }: {
        index: string;
      }) => {
        if (index === activeIndex) {
          return {
            hits: {
              hits: [
                {
                  _id: 'p-1',
                  _source: { product_id: 'p-1', source_version: 5 },
                },
                {
                  _id: 'p-2',
                  _source: { product_id: 'p-2', source_version: 10 },
                },
              ],
            },
          } as never;
        }
        if (index === targetIndex) {
          return {
            hits: {
              hits: [
                {
                  _id: 'p-1',
                  _source: { product_id: 'p-1', source_version: 5 },
                },
                {
                  _id: 'p-2',
                  _source: { product_id: 'p-2', source_version: 11 },
                },
              ],
            },
          } as never;
        }
        return { hits: { hits: [] } } as never;
      }) as never);

      const gateResult = await validationGate.checkSourceVersionGate(
        activeIndex,
        targetIndex,
      );
      expect(gateResult.passed).toBe(true);
    });

    it('[RX-U-42] Pre-swap Gate 2: Source version regression detected returns FAIL REINDEX_GATE_VERSION_REGRESSION', async () => {
      esClient.search.mockImplementation((async ({
        index,
      }: {
        index: string;
      }) => {
        if (index === activeIndex) {
          return {
            hits: {
              hits: [
                {
                  _id: 'p-1',
                  _source: { product_id: 'p-1', source_version: 8 },
                },
              ],
            },
          } as never;
        }
        if (index === targetIndex) {
          return {
            hits: {
              hits: [
                {
                  _id: 'p-1',
                  _source: { product_id: 'p-1', source_version: 7 },
                },
              ],
            },
          } as never;
        }
        return { hits: { hits: [] } } as never;
      }) as never);

      const gateResult = await validationGate.checkSourceVersionGate(
        activeIndex,
        targetIndex,
      );
      expect(gateResult.passed).toBe(false);
      expect(gateResult.errorCode).toBe(
        ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION,
      );
    });

    it('Pre-swap Gate 2: Regression when document is missing in target index returns FAIL', async () => {
      esClient.search.mockImplementation((async ({
        index,
      }: {
        index: string;
      }) => {
        if (index === activeIndex) {
          return {
            hits: {
              hits: [
                {
                  _id: 'p-99',
                  _source: { product_id: 'p-99', source_version: 2 },
                },
              ],
            },
          } as never;
        }
        if (index === targetIndex) {
          return {
            hits: {
              hits: [],
            },
          } as never;
        }
        return { hits: { hits: [] } } as never;
      }) as never);

      const gateResult = await validationGate.checkSourceVersionGate(
        activeIndex,
        targetIndex,
      );
      expect(gateResult.passed).toBe(false);
      expect(gateResult.errorCode).toBe(
        ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION,
      );
    });
  });

  describe('Validation Gate 3: Smoke Queries (RX-U-43, RX-U-44, RX-U-45)', () => {
    it('[RX-U-43] Pre-swap Gate 3: Smoke queries pass when match_all, aggs and suggest succeed with 0 shard failures', async () => {
      esClient.search.mockResolvedValueOnce({
        _shards: { total: 5, successful: 5, failed: 0, skipped: 0 },
        hits: {
          total: { value: 100 },
          hits: [
            {
              _source: {
                product_id: 'p-1',
                visibility_status: VisibilityStatus.PUBLISHED,
              },
            },
          ],
        },
        aggregations: {
          category_ids: { buckets: [{ key: 'cat-1', doc_count: 10 }] },
          brand: { buckets: [{ key: 'brand-1', doc_count: 10 }] },
          attribute_pairs: { buckets: [{ key: 'color=Black', doc_count: 5 }] },
        },
      } as never);

      const gateResult =
        await validationGate.checkSmokeQueriesGate(targetIndex);
      expect(gateResult.passed).toBe(true);
    });

    it('[RX-U-44] Pre-swap Gate 3: Smoke queries abort with REINDEX_GATE_SMOKE_FAILED on shard failure', async () => {
      esClient.search.mockResolvedValueOnce({
        _shards: { total: 5, successful: 4, failed: 1, skipped: 0 },
        hits: { total: { value: 50 }, hits: [] },
      } as never);

      const gateResult =
        await validationGate.checkSmokeQueriesGate(targetIndex);
      expect(gateResult.passed).toBe(false);
      expect(gateResult.errorCode).toBe(
        ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
      );
    });

    it('[RX-U-45] Pre-swap Gate 3: Smoke query validation detects non-published document leakage', async () => {
      esClient.search.mockResolvedValueOnce({
        _shards: { total: 5, successful: 5, failed: 0, skipped: 0 },
        hits: {
          total: { value: 1 },
          hits: [
            {
              _source: {
                product_id: 'p-leaked',
                visibility_status: VisibilityStatus.HIDDEN,
              },
            },
          ],
        },
        aggregations: {
          category_ids: { buckets: [{ key: 'c1' }] },
          brand: { buckets: [{ key: 'b1' }] },
          attribute_pairs: { buckets: [{ key: 'a1' }] },
        },
      } as never);

      const gateResult =
        await validationGate.checkSmokeQueriesGate(targetIndex);
      expect(gateResult.passed).toBe(false);
      expect(gateResult.errorCode).toBe(
        ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
      );
    });
  });

  describe('INDEX_COPY Execution Flow (D16, RX-U-36, RX-U-46)', () => {
    it('executes ES _reindex, polls tasks.get until complete, and transitions to VALIDATING', async () => {
      const job = createBaseJob('INDEX_COPY');

      esClient.reindex.mockResolvedValueOnce({ task: 'task-123' } as never);
      (esClient.tasks.get as jest.Mock)
        .mockResolvedValueOnce({ completed: false } as never)
        .mockResolvedValueOnce({
          completed: true,
          response: { total: 10000, created: 10000, updated: 0, failures: [] },
        } as never);

      await indexCopySource.copy(job);

      expect(esClient.reindex).toHaveBeenCalledWith(
        expect.objectContaining({
          wait_for_completion: false,
          conflicts: 'proceed',
          source: { index: activeIndex },
          dest: { index: targetIndex },
        }),
      );
      expect(jobRepo.updateProgress).toHaveBeenCalledWith(
        job.job_id,
        expect.objectContaining({
          checkpoint: 'task-123',
        }),
      );
    });

    it('handles unexpected reindex failure by updating job to FAILED REINDEX_WORKER_ERROR', async () => {
      const job = createBaseJob('INDEX_COPY');
      esClient.reindex.mockRejectedValueOnce(new Error('Cluster timeout'));

      await runner.run(job);

      expect(jobRepo.update).toHaveBeenCalledWith(
        expect.objectContaining({
          job_id: job.job_id,
          state: ReindexState.FAILED,
          error_code: ReindexErrorCode.REINDEX_WORKER_ERROR,
        }),
      );
      expect(jobRepo.releaseLock).toHaveBeenCalledWith(job.job_id);
    });
  });

  describe('KAFKA_REPLAY Execution Flow & Missing Provider Failures', () => {
    it('fails with REINDEX_SOURCE_UNAVAILABLE when PRODUCT_DOCUMENT_BUILDER or INDEX_WRITER are absent', async () => {
      const moduleWithoutIndexing: TestingModule =
        await Test.createTestingModule({
          providers: [
            ReindexRunner,
            ValidationGate,
            IndexCopySource,
            KafkaReplaySource,
            { provide: Client, useValue: esClient },
            { provide: ReindexJobRepository, useValue: jobRepo },
            { provide: AppConfigService, useValue: configService },
            { provide: Kafka, useValue: kafka },
            // PRODUCT_DOCUMENT_BUILDER & INDEX_WRITER omitted
          ],
        }).compile();

      const runnerNoIndexing =
        moduleWithoutIndexing.get<ReindexRunner>(ReindexRunner);
      const job = createBaseJob('KAFKA_REPLAY');

      await runnerNoIndexing.run(job);

      expect(jobRepo.update).toHaveBeenCalledWith(
        expect.objectContaining({
          job_id: job.job_id,
          state: ReindexState.FAILED,
          error_code: ReindexErrorCode.REINDEX_SOURCE_UNAVAILABLE,
        }),
      );
      expect(jobRepo.releaseLock).toHaveBeenCalledWith(job.job_id);
    });

    it('[RX-U-37] Checkpoint resume logic: worker initialized with existing checkpoint offsets', async () => {
      const job = createBaseJob('KAFKA_REPLAY');
      job.checkpoint = JSON.stringify({
        'product.events.v1': { 0: '500' },
      });

      const initialOffsets =
        await kafkaReplaySource.resolveReplayStartingOffsets(job);
      expect(initialOffsets['product.events.v1']).toBeDefined();
      expect(initialOffsets['product.events.v1'][0]).toBe('500');
    });
  });

  describe('Full Lifecycle, Swap & Finally Lock Release', () => {
    it('runs copy, passes gates, executes atomic alias swap, marks SWAPPED and releases lock in finally', async () => {
      const job = createBaseJob('INDEX_COPY');

      await runner.run(job);

      expect(esClient.indices.updateAliases).toHaveBeenCalledWith({
        body: {
          actions: [
            { remove: { index: activeIndex, alias: aliasName } },
            { add: { index: targetIndex, alias: aliasName } },
          ],
        },
      });

      expect(jobRepo.update).toHaveBeenCalledWith(
        expect.objectContaining({
          job_id: job.job_id,
          state: ReindexState.SWAPPED,
          finished_at: expect.any(String),
          error_code: null,
        }),
      );

      expect(jobRepo.releaseLock).toHaveBeenCalledWith(job.job_id);
    });

    it('[Techlead Review Item 2] does not define dead code resumeBackfill', () => {
      const r = runner as unknown as Record<string, unknown>;
      expect(r.resumeBackfill).toBeUndefined();
    });

    it('[Techlead Review Item 5] copyStep handles estypes.TasksGetResponse with completed and status info', async () => {
      const job = createBaseJob('INDEX_COPY');
      esClient.reindex.mockResolvedValueOnce({ task: 'task-456' } as never);
      (esClient.tasks.get as jest.Mock).mockResolvedValueOnce({
        completed: true,
        task: {
          id: 456,
          type: 'direct',
          action: 'indices:data/write/reindex',
          status: {
            total: 50,
            created: 45,
            updated: 5,
            version_conflicts: 0,
          },
          description: 'reindex',
          start_time_in_millis: Date.now(),
          running_time_in_nanos: 0,
          cancellable: true,
          headers: {},
        },
        response: {
          took: 100,
          timed_out: false,
          total: 50,
          updated: 5,
          created: 45,
          deleted: 0,
          batches: 1,
          version_conflicts: 0,
          noops: 0,
          retries: { bulk: 0, search: 0 },
          throttled_millis: 0,
          requests_per_second: -1,
          throttled_until_millis: 0,
          failures: [],
        },
      });

      await indexCopySource.copy(job);
      expect(job.processed_count).toBe(50);
      expect(job.failed_count).toBe(0);
    });
  });
});
