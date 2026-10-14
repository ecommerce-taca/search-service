import { Client } from '@elastic/elasticsearch';
import { ReindexState } from '../common/enums';
import { AppConfigService } from '../config/config.service';
import { ReindexErrorCode, ReindexSource } from './enums';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';
import { ReindexRunner } from './reindex.runner';
import { ValidationGate } from './validation-gate';

describe('ReindexRunner', () => {
  let runner: ReindexRunner;
  let mockClient: jest.Mocked<Partial<Client>>;
  let mockConfigService: jest.Mocked<Partial<AppConfigService>>;
  let mockJobRepo: jest.Mocked<Partial<ReindexJobRepository>>;
  let mockValidationGate: jest.Mocked<Partial<ValidationGate>>;

  beforeEach(() => {
    mockClient = {
      reindex: jest.fn().mockResolvedValue({ task: 'task-123' }),
      tasks: {
        get: jest.fn().mockResolvedValue({
          completed: true,
          response: { created: 100, updated: 0, version_conflicts: 0 },
        }),
      } as unknown as Client['tasks'],
      indices: {
        refresh: jest.fn().mockResolvedValue({}),
        updateAliases: jest.fn().mockResolvedValue({}),
      } as unknown as Client['indices'],
      count: jest.fn().mockResolvedValue({ count: 100 }),
    };

    mockConfigService = {
      esIndexPrefix: 'test-',
      kafkaTopicPrefix: 'test-',
    };

    mockJobRepo = {
      update: jest.fn().mockResolvedValue({} as unknown as ReindexJobDocument),
      releaseLock: jest.fn().mockResolvedValue(true),
    };

    mockValidationGate = {
      checkDocumentCount: jest
        .fn()
        .mockReturnValue({ status: 'PASS', pass: true }),
      checkSourceVersions: jest
        .fn()
        .mockResolvedValue({ status: 'PASS', pass: true }),
      runSmokeQueries: jest
        .fn()
        .mockResolvedValue({ status: 'PASS', pass: true }),
    };

    runner = new ReindexRunner(
      mockClient as Client,
      mockConfigService as AppConfigService,
      mockJobRepo as ReindexJobRepository,
      mockValidationGate as ValidationGate,
    );
  });

  describe('executeJob - INDEX_COPY happy path', () => {
    it('executes RUNNING -> VALIDATING -> SWAPPED and releases lock', async () => {
      const job: ReindexJobDocument = {
        job_id: 'job-001',
        target_index: 'test-products-v1-new',
        mapping_version: 1,
        state: ReindexState.REQUESTED,
        checkpoint: null,
        total_estimate: 100,
        processed_count: 0,
        failed_count: 0,
        started_at: '2026-10-14T10:00:00Z',
        finished_at: null,
        updated_at: '2026-10-14T10:00:00Z',
        error_code: null,
      };

      await runner.executeJob(
        job,
        ReindexSource.INDEX_COPY,
        'test-products-v1-old',
      );

      expect(job.state).toBe(ReindexState.SWAPPED);
      expect(mockJobRepo.releaseLock).toHaveBeenCalledWith('job-001');
      expect(mockClient.indices!.updateAliases).toHaveBeenCalled();
    });
  });

  describe('executeJob - KAFKA_REPLAY with missing providers', () => {
    it('marks job FAILED with REINDEX_SOURCE_UNAVAILABLE when providers are missing', async () => {
      const job: ReindexJobDocument = {
        job_id: 'job-002',
        target_index: 'test-products-v1-new',
        mapping_version: 1,
        state: ReindexState.REQUESTED,
        checkpoint: null,
        total_estimate: 100,
        processed_count: 0,
        failed_count: 0,
        started_at: '2026-10-14T10:00:00Z',
        finished_at: null,
        updated_at: '2026-10-14T10:00:00Z',
        error_code: null,
      };

      await runner.executeJob(
        job,
        ReindexSource.KAFKA_REPLAY,
        'test-products-v1-old',
      );

      expect(job.state).toBe(ReindexState.FAILED);
      expect(job.error_code).toBe(ReindexErrorCode.REINDEX_SOURCE_UNAVAILABLE);
      expect(mockJobRepo.releaseLock).toHaveBeenCalledWith('job-002');
    });
  });

  describe('executeJob - validation gate failure', () => {
    it('marks job FAILED with gate error_code and aborts alias swap', async () => {
      (mockValidationGate.checkDocumentCount as jest.Mock).mockReturnValueOnce({
        status: 'FAIL',
        pass: false,
        reason: ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
      });

      const job: ReindexJobDocument = {
        job_id: 'job-003',
        target_index: 'test-products-v1-new',
        mapping_version: 1,
        state: ReindexState.REQUESTED,
        checkpoint: null,
        total_estimate: 100,
        processed_count: 0,
        failed_count: 0,
        started_at: '2026-10-14T10:00:00Z',
        finished_at: null,
        updated_at: '2026-10-14T10:00:00Z',
        error_code: null,
      };

      await runner.executeJob(
        job,
        ReindexSource.INDEX_COPY,
        'test-products-v1-old',
      );

      expect(job.state).toBe(ReindexState.FAILED);
      expect(job.error_code).toBe(ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH);
      expect(mockClient.indices!.updateAliases).not.toHaveBeenCalled();
      expect(mockJobRepo.releaseLock).toHaveBeenCalledWith('job-003');
    });
  });
});
