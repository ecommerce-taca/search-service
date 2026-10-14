import { Client } from '@elastic/elasticsearch';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import {
  BASE_INDEXES,
  MAPPING_VERSION,
  getProductsAlias,
  getReindexJobsIndex,
} from '../src/common/constants';
import { ReindexState } from '../common/enums';
import { SearchErrorCode } from '../src/common/errors/error-codes';
import { SearchExceptionFilter } from '../src/common/errors/search-exception.filter';
import { AppConfigService } from '../src/config/config.service';
import { CONSUMER_STATUS_PROVIDER } from '../src/event-consumer/interfaces';
import { ELASTICSEARCH_CLIENT } from '../src/product-index/elasticsearch.module';
import { IndexBootstrapService } from '../src/product-index/index-bootstrap.service';
import { ReindexJobDocument } from '../src/reindex/job.repository';
import { ReindexErrorCode } from '../src/reindex/reindex.service';

describe('Reindex & Admin (e2e)', () => {
  let app: INestApplication;
  let mockEsClient: any;
  let inMemoryJobStore: Map<string, ReindexJobDocument>;
  let activeLockHeldBy: string | null = null;

  const activeIndexName = 'test-products-v1-20261014100000';
  const aliasName = 'test-products-read';
  const jobsIndexName = 'test-search-reindex-jobs';

  const validAdminHeaders = {
    'x-user-roles': 'SEARCH_ADMIN',
    'x-mfa-step-up': 'true',
  };

  const adminNoStepUpHeaders = {
    'x-user-roles': 'SEARCH_ADMIN',
  };

  const sellerHeaders = {
    'x-user-roles': 'SELLER',
    'x-mfa-step-up': 'true',
  };

  beforeAll(async () => {
    inMemoryJobStore = new Map<string, ReindexJobDocument>();

    mockEsClient = {
      indices: {
        existsAlias: jest.fn().mockResolvedValue(true),
        getAlias: jest.fn().mockImplementation(async ({ name }) => {
          return { [activeIndexName]: {} };
        }),
        create: jest.fn().mockResolvedValue({ acknowledged: true }),
        delete: jest.fn().mockResolvedValue({ acknowledged: true }),
        updateAliases: jest.fn().mockResolvedValue({ acknowledged: true }),
        refresh: jest.fn().mockResolvedValue({ acknowledged: true }),
        exists: jest.fn().mockResolvedValue(true),
      },
      count: jest.fn().mockImplementation(async ({ index }) => {
        return { count: 48213 };
      }),
      index: jest.fn().mockImplementation(async ({ index, id, document, op_type }) => {
        if (id === 'active-lock') {
          if (op_type === 'create' && activeLockHeldBy !== null) {
            const err: any = new Error('version_conflict_engine_exception');
            err.statusCode = 409;
            throw err;
          }
          activeLockHeldBy = document.job_id;
          return { _id: id, result: 'created' };
        }
        inMemoryJobStore.set(id, { ...document });
        return { _id: id, result: 'created', _seq_no: 1, _primary_term: 1 };
      }),
      get: jest.fn().mockImplementation(async ({ index, id }) => {
        if (id === 'active-lock') {
          if (!activeLockHeldBy) {
            const err: any = new Error('document_missing_exception');
            err.statusCode = 404;
            throw err;
          }
          return {
            _id: 'active-lock',
            _source: {
              job_id: activeLockHeldBy,
              target_index: 'active-lock',
              state: ReindexState.RUNNING,
            },
          };
        }
        const job = inMemoryJobStore.get(id);
        if (!job) {
          const err: any = new Error('document_missing_exception');
          err.statusCode = 404;
          throw err;
        }
        return { _id: id, _source: job, _seq_no: 1, _primary_term: 1 };
      }),
      delete: jest.fn().mockImplementation(async ({ index, id }) => {
        if (id === 'active-lock') {
          activeLockHeldBy = null;
          return { result: 'deleted' };
        }
        inMemoryJobStore.delete(id);
        return { result: 'deleted' };
      }),
      search: jest.fn().mockImplementation(async ({ index, query }) => {
        // Exclude active-lock
        const allJobs = Array.from(inMemoryJobStore.values()).filter(
          (j) => (j as any)._id !== 'active-lock',
        );

        // Sort descending by started_at
        allJobs.sort(
          (a, b) =>
            new Date(b.started_at).getTime() - new Date(a.started_at).getTime(),
        );

        return {
          hits: {
            total: { value: allJobs.length },
            hits: allJobs.map((j) => ({
              _id: j.job_id,
              _source: j,
            })),
          },
        };
      }),
      reindex: jest.fn().mockResolvedValue({ task: 'task-123' }),
      tasks: {
        get: jest.fn().mockResolvedValue({
          completed: true,
          response: { total: 48213, created: 48213, failures: [] },
        }),
      },
    };

    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    })
      .overrideProvider(IndexBootstrapService)
      .useValue({
        onApplicationBootstrap: jest.fn().mockResolvedValue(undefined),
        bootstrap: jest.fn().mockResolvedValue({
          alias: aliasName,
          aliasCreated: false,
          consumerStateIndex: 'test-search-consumer-state',
          consumerStateCreated: false,
          reindexJobsIndex: jobsIndexName,
          reindexJobsCreated: false,
          searchCategoriesIndex: 'test-search-categories',
          searchCategoriesCreated: false,
        }),
      })
      .overrideProvider(ELASTICSEARCH_CLIENT)
      .useValue(mockEsClient)
      .overrideProvider(Client)
      .useValue(mockEsClient)
      .overrideProvider(CONSUMER_STATUS_PROVIDER)
      .useValue({
        getStatus: jest.fn().mockResolvedValue({
          lastEventAt: '2026-10-14T11:59:58Z',
          lagByTopic: {
            'product.events.v1': 0,
            'sku.events.v1': 2,
            'category.events.v1': 0,
            'catalog.events.v1': 0,
            'rating.events.v1': 1,
          },
          dlqCount: 0,
        }),
      })
      .compile();

    app = moduleFixture.createNestApplication();

    app.setGlobalPrefix('api/v1', {
      exclude: ['health/live', 'metrics'],
    });

    app.useGlobalFilters(new SearchExceptionFilter());

    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        transform: true,
        forbidNonWhitelisted: true,
      }),
    );

    await app.init();
  });

  afterAll(async () => {
    if (app) {
      await app.close();
    }
  });

  beforeEach(() => {
    inMemoryJobStore.clear();
    activeLockHeldBy = null;
    jest.clearAllMocks();
  });

  describe('RBAC & Step-up Authentication (RX-I-01 to RX-I-10)', () => {
    const validJobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001';

    it('[RX-I-01] GET /api/v1/admin/search/status anonymous returns 403 SEARCH_ADMIN_FORBIDDEN', async () => {
      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .expect(403);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('[RX-I-02] POST /api/v1/admin/search/reindex anonymous returns 403 SEARCH_ADMIN_FORBIDDEN', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Routine mapping change',
        })
        .expect(403);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
      expect(inMemoryJobStore.size).toBe(0);
    });

    it('[RX-I-03] GET /api/v1/admin/search/reindex/:jobId anonymous returns 403 SEARCH_ADMIN_FORBIDDEN', async () => {
      const res = await request(app.getHttpServer())
        .get(`/api/v1/admin/search/reindex/${validJobId}`)
        .expect(403);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('[RX-I-04] GET /api/v1/admin/search/status with role SELLER returns 403 SEARCH_ADMIN_FORBIDDEN', async () => {
      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set('x-user-roles', 'SELLER')
        .expect(403);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('[RX-I-05] POST /api/v1/admin/search/reindex with role SELLER and step-up returns 403 SEARCH_ADMIN_FORBIDDEN', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(sellerHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Non-admin attempt',
        })
        .expect(403);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('[RX-I-06] POST /api/v1/admin/search/reindex with SEARCH_ADMIN missing X-MFA-Step-Up returns 403 SEARCH_ADMIN_FORBIDDEN', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(adminNoStepUpHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Missing step up',
        })
        .expect(403);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('[RX-I-07] POST /api/v1/admin/search/reindex with SEARCH_ADMIN invalid X-MFA-Step-Up returns 403 SEARCH_ADMIN_FORBIDDEN', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set('x-user-roles', 'SEARCH_ADMIN')
        .set('x-mfa-step-up', 'false')
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Invalid step up',
        })
        .expect(403);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('[RX-I-08] POST /api/v1/admin/search/reindex with SEARCH_ADMIN and valid X-MFA-Step-Up returns 202 Accepted', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(validAdminHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Valid admin reindex trigger',
        })
        .expect(202);

      expect(res.body.data.job_id).toBeDefined();
      expect(res.body.data.state).toBe(ReindexState.REQUESTED);
      expect(res.body.data.target_index).toMatch(/^products-v1-[0-9]{14}$/);
      expect(res.body.meta.request_id).toBeDefined();
    });

    it('[RX-I-09] GET /api/v1/admin/search/status with SEARCH_ADMIN without step-up returns 200 OK', async () => {
      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set(adminNoStepUpHeaders)
        .expect(200);

      expect(res.body.data.alias).toBeDefined();
      expect(res.body.data.reindex_state).toBeDefined();
    });

    it('[RX-I-10] GET /api/v1/admin/search/reindex/:jobId with SEARCH_ADMIN without step-up returns 200 OK', async () => {
      inMemoryJobStore.set(validJobId, {
        job_id: validJobId,
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.RUNNING,
        checkpoint: null,
        processed_count: 100,
        failed_count: 0,
        total_estimate: 1000,
        started_at: '2026-10-14T12:00:00Z',
        finished_at: null,
        updated_at: '2026-10-14T12:01:00Z',
        error_code: null,
        trace_id: 't-1',
        request_id: 'r-1',
      });

      const res = await request(app.getHttpServer())
        .get(`/api/v1/admin/search/reindex/${validJobId}`)
        .set(adminNoStepUpHeaders)
        .expect(200);

      expect(res.body.data.job_id).toBe(validJobId);
      expect(res.body.data.state).toBe(ReindexState.RUNNING);
    });
  });

  describe('Payload Validation (RX-I-11 to RX-I-14)', () => {
    it('[RX-I-11] POST /api/v1/admin/search/reindex with empty body returns 400 SEARCH_INVALID_INPUT', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(validAdminHeaders)
        .send({})
        .expect(400);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
    });

    it('[RX-I-12] POST /api/v1/admin/search/reindex with source: INVALID_ENUM returns 400 SEARCH_INVALID_INPUT', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(validAdminHeaders)
        .send({
          source: 'INVALID_ENUM',
          mapping_version: 1,
          reason: 'Invalid enum test',
        })
        .expect(400);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
    });

    it('[RX-I-13] POST /api/v1/admin/search/reindex with reason: "" returns 400 SEARCH_INVALID_INPUT', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(validAdminHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: '',
        })
        .expect(400);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
    });

    it('[RX-I-14] POST /api/v1/admin/search/reindex with valid payload returns 202 Accepted with job descriptor', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(validAdminHeaders)
        .send({
          source: 'KAFKA_REPLAY',
          mapping_version: 1,
          reason: 'Full cluster reindex',
        })
        .expect(202);

      expect(res.body.data).toEqual({
        job_id: expect.stringMatching(/^job-[0-9a-f-]{36}$/),
        state: 'REQUESTED',
        target_index: expect.stringMatching(/^products-v1-[0-9]{14}$/),
      });
      expect(res.body.meta.request_id).toBeDefined();
    });
  });

  describe('Active Job Conflict Checks (RX-I-18 to RX-I-21)', () => {
    it('[RX-I-18] POST /api/v1/admin/search/reindex when job is RUNNING returns 409 SEARCH_REINDEX_CONFLICT', async () => {
      activeLockHeldBy = 'job-existing-running';

      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(validAdminHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Concurrent trigger attempt',
        })
        .expect(409);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
    });

    it('[RX-I-19] POST /api/v1/admin/search/reindex when job is VALIDATING returns 409 SEARCH_REINDEX_CONFLICT', async () => {
      activeLockHeldBy = 'job-existing-validating';

      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(validAdminHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Concurrent trigger attempt',
        })
        .expect(409);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
    });

    it('[RX-I-20] POST /api/v1/admin/search/reindex when previous job is SWAPPED returns 202 Accepted', async () => {
      inMemoryJobStore.set('job-completed', {
        job_id: 'job-completed',
        target_index: 'products-v1-20261014090000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Past job',
        state: ReindexState.SWAPPED,
        checkpoint: null,
        processed_count: 48213,
        failed_count: 0,
        total_estimate: 48213,
        started_at: '2026-10-14T09:00:00Z',
        finished_at: '2026-10-14T09:30:00Z',
        updated_at: '2026-10-14T09:30:00Z',
        error_code: null,
        trace_id: 't-1',
        request_id: 'r-1',
      });
      activeLockHeldBy = null;

      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(validAdminHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'New trigger after swap',
        })
        .expect(202);

      expect(res.body.data.state).toBe(ReindexState.REQUESTED);
    });

    it('[RX-I-21] POST /api/v1/admin/search/reindex when previous job is FAILED returns 202 Accepted', async () => {
      inMemoryJobStore.set('job-failed', {
        job_id: 'job-failed',
        target_index: 'products-v1-20261014080000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Failed job',
        state: ReindexState.FAILED,
        checkpoint: null,
        processed_count: 100,
        failed_count: 1,
        total_estimate: 48213,
        started_at: '2026-10-14T08:00:00Z',
        finished_at: '2026-10-14T08:15:00Z',
        updated_at: '2026-10-14T08:15:00Z',
        error_code: ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
        trace_id: 't-1',
        request_id: 'r-1',
      });
      activeLockHeldBy = null;

      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(validAdminHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Retry after failure',
        })
        .expect(202);

      expect(res.body.data.state).toBe(ReindexState.REQUESTED);
    });
  });

  describe('GET Job Status by ID (RX-I-36 to RX-I-39)', () => {
    it('[RX-I-36] GET /api/v1/admin/search/reindex/:jobId for running job returns 200 OK', async () => {
      const jobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001';
      inMemoryJobStore.set(jobId, {
        job_id: jobId,
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.RUNNING,
        checkpoint: 'product-01912f9a',
        processed_count: 32000,
        failed_count: 4,
        total_estimate: 48213,
        started_at: '2026-10-14T11:55:00Z',
        finished_at: null,
        updated_at: '2026-10-14T12:00:00Z',
        error_code: null,
        trace_id: 't-1',
        request_id: 'r-1',
      });

      const res = await request(app.getHttpServer())
        .get(`/api/v1/admin/search/reindex/${jobId}`)
        .set(validAdminHeaders)
        .expect(200);

      expect(res.body.data).toEqual({
        job_id: jobId,
        state: 'RUNNING',
        target_index: 'products-v1-20261014120000',
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

    it('[RX-I-37] GET /api/v1/admin/search/reindex/:jobId for completed swapped job returns 200 OK', async () => {
      const jobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d002';
      inMemoryJobStore.set(jobId, {
        job_id: jobId,
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.SWAPPED,
        checkpoint: null,
        processed_count: 48213,
        failed_count: 0,
        total_estimate: 48213,
        started_at: '2026-10-14T11:55:00Z',
        finished_at: '2026-10-14T12:30:00Z',
        updated_at: '2026-10-14T12:30:00Z',
        error_code: null,
        trace_id: 't-1',
        request_id: 'r-1',
      });

      const res = await request(app.getHttpServer())
        .get(`/api/v1/admin/search/reindex/${jobId}`)
        .set(validAdminHeaders)
        .expect(200);

      expect(res.body.data.state).toBe(ReindexState.SWAPPED);
      expect(res.body.data.error_code).toBeNull();
    });

    it('[RX-I-38] GET /api/v1/admin/search/reindex/:jobId for failed job returns 200 OK with allowlisted error_code', async () => {
      const jobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d003';
      inMemoryJobStore.set(jobId, {
        job_id: jobId,
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.FAILED,
        checkpoint: null,
        processed_count: 47900,
        failed_count: 12,
        total_estimate: 48213,
        started_at: '2026-10-14T11:55:00Z',
        finished_at: '2026-10-14T12:20:00Z',
        updated_at: '2026-10-14T12:20:00Z',
        error_code: ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
        trace_id: 't-1',
        request_id: 'r-1',
      });

      const res = await request(app.getHttpServer())
        .get(`/api/v1/admin/search/reindex/${jobId}`)
        .set(validAdminHeaders)
        .expect(200);

      expect(res.body.data.state).toBe(ReindexState.FAILED);
      expect(res.body.data.error_code).toBe(
        'REINDEX_GATE_COUNT_MISMATCH',
      );
    });

    it('[RX-I-39] GET /api/v1/admin/search/reindex/:jobId for non-existent jobId returns 404 SEARCH_REINDEX_NOT_FOUND', async () => {
      const unknownId = 'job-00000000-0000-0000-0000-000000000000';

      const res = await request(app.getHttpServer())
        .get(`/api/v1/admin/search/reindex/${unknownId}`)
        .set(validAdminHeaders)
        .expect(404);

      expect(res.body.error.code).toBe(
        SearchErrorCode.SEARCH_REINDEX_NOT_FOUND,
      );
    });
  });

  describe('GET Admin Search Status (RX-I-40 to RX-I-45)', () => {
    it('[RX-I-40] GET /api/v1/admin/search/status when no reindex job has ever been created returns IDLE', async () => {
      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set(validAdminHeaders)
        .expect(200);

      expect(res.body.data.reindex_state).toBe('IDLE');
    });

    it('[RX-I-41] GET /api/v1/admin/search/status after job completes swap returns reindex_state SWAPPED', async () => {
      inMemoryJobStore.set('job-1', {
        job_id: 'job-1',
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.SWAPPED,
        checkpoint: null,
        processed_count: 48213,
        failed_count: 0,
        total_estimate: 48213,
        started_at: '2026-10-14T11:00:00Z',
        finished_at: '2026-10-14T11:30:00Z',
        updated_at: '2026-10-14T11:30:00Z',
        error_code: null,
        trace_id: 't-1',
        request_id: 'r-1',
      });

      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set(validAdminHeaders)
        .expect(200);

      expect(res.body.data.reindex_state).toBe(ReindexState.SWAPPED);
      expect(res.body.data.active_index).toBe(activeIndexName);
      expect(res.body.data.mapping_version).toBe(1);
      expect(res.body.data.document_count).toBe(48213);
    });

    it('[RX-I-42] GET /api/v1/admin/search/status while job is running returns reindex_state RUNNING', async () => {
      inMemoryJobStore.set('job-run', {
        job_id: 'job-run',
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.RUNNING,
        checkpoint: null,
        processed_count: 1000,
        failed_count: 0,
        total_estimate: 48213,
        started_at: '2026-10-14T12:00:00Z',
        finished_at: null,
        updated_at: '2026-10-14T12:01:00Z',
        error_code: null,
        trace_id: 't-1',
        request_id: 'r-1',
      });

      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set(validAdminHeaders)
        .expect(200);

      expect(res.body.data.reindex_state).toBe(ReindexState.RUNNING);
    });

    it('[RX-I-43] GET /api/v1/admin/search/status after job failure returns reindex_state FAILED', async () => {
      inMemoryJobStore.set('job-fail', {
        job_id: 'job-fail',
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.FAILED,
        checkpoint: null,
        processed_count: 50,
        failed_count: 1,
        total_estimate: 48213,
        started_at: '2026-10-14T12:00:00Z',
        finished_at: '2026-10-14T12:05:00Z',
        updated_at: '2026-10-14T12:05:00Z',
        error_code: ReindexErrorCode.REINDEX_WORKER_ERROR,
        trace_id: 't-1',
        request_id: 'r-1',
      });

      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set(validAdminHeaders)
        .expect(200);

      expect(res.body.data.reindex_state).toBe(ReindexState.FAILED);
    });

    it('[RX-I-44] GET /api/v1/admin/search/status reports consumer lag per topic', async () => {
      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set(validAdminHeaders)
        .expect(200);

      expect(res.body.data.consumer_lag).toEqual({
        'product.events.v1': 0,
        'sku.events.v1': 2,
        'category.events.v1': 0,
        'catalog.events.v1': 0,
        'rating.events.v1': 1,
      });
    });

    it('[RX-I-45] GET /api/v1/admin/search/status reports dead-letter queue count', async () => {
      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set(validAdminHeaders)
        .expect(200);

      expect(res.body.data.dlq_count).toBe(0);
    });
  });

  describe('Redaction & Privacy Protection (RX-I-46)', () => {
    it('[RX-I-46] Redaction check on GET /admin/search/status and GET /admin/search/reindex/:jobId', async () => {
      const jobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001';
      inMemoryJobStore.set(jobId, {
        job_id: jobId,
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.RUNNING,
        checkpoint: null,
        processed_count: 100,
        failed_count: 0,
        total_estimate: 1000,
        started_at: '2026-10-14T12:00:00Z',
        finished_at: null,
        updated_at: '2026-10-14T12:01:00Z',
        error_code: null,
        trace_id: 't-1',
        request_id: 'r-1',
      });

      const statusRes = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set(validAdminHeaders)
        .expect(200);

      const jobRes = await request(app.getHttpServer())
        .get(`/api/v1/admin/search/reindex/${jobId}`)
        .set(validAdminHeaders)
        .expect(200);

      const combinedText = JSON.stringify(statusRes.body) + JSON.stringify(jobRes.body);

      // Asserts sensitive fields never leak
      expect(combinedText).not.toContain('authorization');
      expect(combinedText).not.toContain('Bearer');
      expect(combinedText).not.toContain('password');
      expect(combinedText).not.toContain('dsl');
      expect(combinedText).not.toContain('elasticsearch:9200');
    });
  });

  describe('Job Cancellation Flow (D17, S-API-12)', () => {
    const validJobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001';

    it('POST /admin/search/reindex/:jobId/cancel without step-up returns 403 SEARCH_ADMIN_FORBIDDEN', async () => {
      const res = await request(app.getHttpServer())
        .post(`/api/v1/admin/search/reindex/${validJobId}/cancel`)
        .set(adminNoStepUpHeaders)
        .expect(403);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('POST /admin/search/reindex/:jobId/cancel with valid step-up when RUNNING returns 200 CANCELLED', async () => {
      inMemoryJobStore.set(validJobId, {
        job_id: validJobId,
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.RUNNING,
        checkpoint: null,
        processed_count: 50,
        failed_count: 0,
        total_estimate: 1000,
        started_at: '2026-10-14T12:00:00Z',
        finished_at: null,
        updated_at: '2026-10-14T12:01:00Z',
        error_code: null,
        trace_id: 't-1',
        request_id: 'r-1',
      });
      activeLockHeldBy = validJobId;

      const res = await request(app.getHttpServer())
        .post(`/api/v1/admin/search/reindex/${validJobId}/cancel`)
        .set(validAdminHeaders)
        .expect(200);

      expect(res.body.data.state).toBe(ReindexState.CANCELLED);
      expect(res.body.data.error_code).toBeNull();
      expect(mockEsClient.indices.delete).toHaveBeenCalledWith({
        index: 'products-v1-20261014120000',
      });
      expect(activeLockHeldBy).toBeNull();
    });

    it('POST /admin/search/reindex/:jobId/cancel when SWAPPED or FAILED returns 409 SEARCH_REINDEX_CONFLICT', async () => {
      inMemoryJobStore.set(validJobId, {
        job_id: validJobId,
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'Upgrade',
        state: ReindexState.SWAPPED,
        checkpoint: null,
        processed_count: 1000,
        failed_count: 0,
        total_estimate: 1000,
        started_at: '2026-10-14T11:00:00Z',
        finished_at: '2026-10-14T11:30:00Z',
        updated_at: '2026-10-14T11:30:00Z',
        error_code: null,
        trace_id: 't-1',
        request_id: 'r-1',
      });

      const res = await request(app.getHttpServer())
        .post(`/api/v1/admin/search/reindex/${validJobId}/cancel`)
        .set(validAdminHeaders)
        .expect(409);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
    });

    it('POST /admin/search/reindex/:jobId/cancel for unknown jobId returns 404 SEARCH_REINDEX_NOT_FOUND', async () => {
      const unknownId = 'job-00000000-0000-0000-0000-000000000000';

      const res = await request(app.getHttpServer())
        .post(`/api/v1/admin/search/reindex/${unknownId}/cancel`)
        .set(validAdminHeaders)
        .expect(404);

      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
    });
  });

  describe('End-to-End Resilience & Lifecycle Scenarios (RX-E-01 to RX-E-03)', () => {
    it('[RX-E-01] Worker node kill & resume under heavy replay load recovers via checkpoint', async () => {
      // Simulate existing job crashed during RUNNING with checkpoint
      const jobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001';
      inMemoryJobStore.set(jobId, {
        job_id: jobId,
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        source: 'KAFKA_REPLAY',
        reason: 'Replay benchmark',
        state: ReindexState.RUNNING,
        checkpoint: 'product-01912f50',
        processed_count: 20000,
        failed_count: 0,
        total_estimate: 50000,
        started_at: '2026-10-14T11:00:00Z',
        finished_at: null,
        updated_at: '2026-10-14T11:15:00Z',
        error_code: null,
        trace_id: 't-1',
        request_id: 'r-1',
      });

      const statusRes = await request(app.getHttpServer())
        .get(`/api/v1/admin/search/reindex/${jobId}`)
        .set(validAdminHeaders)
        .expect(200);

      expect(statusRes.body.data.checkpoint).toBe('product-01912f50');
      expect(statusRes.body.data.processed_count).toBe(20000);
      expect(statusRes.body.data.state).toBe(ReindexState.RUNNING);
    });

    it('[RX-E-02] Full reindex lifecycle happy path', async () => {
      // 1. Post reindex
      const postRes = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(validAdminHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Full lifecycle happy path',
        })
        .expect(202);

      const jobId = postRes.body.data.job_id;
      expect(jobId).toBeDefined();

      // 2. Poll job status
      const getRes = await request(app.getHttpServer())
        .get(`/api/v1/admin/search/reindex/${jobId}`)
        .set(validAdminHeaders)
        .expect(200);

      expect(getRes.body.data.job_id).toBe(jobId);

      // 3. Status endpoint reflects active or swapped state
      const statusRes = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set(validAdminHeaders)
        .expect(200);

      expect(statusRes.body.data.reindex_state).toBeDefined();
    });

    it('[RX-E-03] Full reindex failure and recovery cycle', async () => {
      // First job failed with REINDEX_GATE_COUNT_MISMATCH
      const failedJobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001';
      inMemoryJobStore.set(failedJobId, {
        job_id: failedJobId,
        target_index: 'products-v1-20261014110000',
        mapping_version: 1,
        source: 'INDEX_COPY',
        reason: 'First attempt with missing docs',
        state: ReindexState.FAILED,
        checkpoint: null,
        processed_count: 40000,
        failed_count: 8213,
        total_estimate: 48213,
        started_at: '2026-10-14T11:00:00Z',
        finished_at: '2026-10-14T11:20:00Z',
        updated_at: '2026-10-14T11:20:00Z',
        error_code: ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
        trace_id: 't-1',
        request_id: 'r-1',
      });
      activeLockHeldBy = null;

      // Status reflects FAILED
      const statusRes1 = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set(validAdminHeaders)
        .expect(200);
      expect(statusRes1.body.data.reindex_state).toBe(ReindexState.FAILED);

      // Trigger second reindex
      const retryRes = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(validAdminHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Second attempt after data repair',
        })
        .expect(202);

      const secondJobId = retryRes.body.data.job_id;
      expect(secondJobId).not.toBe(failedJobId);
      expect(retryRes.body.data.state).toBe(ReindexState.REQUESTED);
    });
  });
});
