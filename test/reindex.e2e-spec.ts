import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { SearchExceptionFilter } from '../src/common/errors/search-exception.filter';
import { IndexBootstrapService } from '../src/product-index/index-bootstrap.service';
import { ReindexService } from '../src/reindex/reindex.service';
import { ReindexState } from '../src/common/enums';
import { SearchErrorCode } from '../src/common/errors/error-codes';
import { SearchException } from '../src/common/errors/search.exception';

describe('Reindex Admin (e2e)', () => {
  let app: INestApplication;
  let mockReindexService: jest.Mocked<Partial<ReindexService>>;

  beforeAll(async () => {
    mockReindexService = {
      getStatus: jest.fn().mockResolvedValue({
        alias: 'products-read',
        active_index: 'products-v1-20260815',
        mapping_version: 1,
        document_count: 48213,
        last_event_at: '2026-08-30T08:59:58Z',
        consumer_lag: { 'product.events.v1': 0 },
        dlq_count: 0,
        reindex_state: 'IDLE',
      }),
      start: jest.fn().mockResolvedValue({
        job_id: 'job-01912fb8-1111',
        state: ReindexState.REQUESTED,
        target_index: 'products-v1-20261014120000',
      }),
      getJob: jest.fn().mockImplementation(async (jobId: string) => {
        if (jobId === 'job-01912fb8-1111') {
          return {
            job_id: 'job-01912fb8-1111',
            state: ReindexState.RUNNING,
            target_index: 'products-v1-20261014120000',
            mapping_version: 1,
            processed_count: 100,
            failed_count: 0,
            total_estimate: 48213,
            checkpoint: 'product-01',
            started_at: '2026-10-14T12:00:00Z',
            updated_at: '2026-10-14T12:01:00Z',
            error_code: null,
          };
        }
        throw new SearchException(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
      }),
      cancel: jest.fn().mockImplementation(async (jobId: string) => {
        if (jobId === 'job-01912fb8-1111') {
          return {
            job_id: 'job-01912fb8-1111',
            state: ReindexState.CANCELLED,
            target_index: 'products-v1-20261014120000',
            mapping_version: 1,
            processed_count: 100,
            failed_count: 0,
            total_estimate: 48213,
            checkpoint: 'product-01',
            started_at: '2026-10-14T12:00:00Z',
            updated_at: '2026-10-14T12:01:00Z',
            error_code: null,
          };
        }
        throw new SearchException(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
      }),
    };

    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    })
      .overrideProvider(IndexBootstrapService)
      .useValue({
        onApplicationBootstrap: jest.fn().mockResolvedValue(undefined),
        bootstrap: jest.fn().mockResolvedValue({
          alias: 'products-read',
          aliasCreated: false,
          consumerStateIndex: 'search-consumer-state',
          consumerStateCreated: false,
          reindexJobsIndex: 'search-reindex-jobs',
          reindexJobsCreated: false,
          searchCategoriesIndex: 'search-categories',
          searchCategoriesCreated: false,
        }),
      })
      .overrideProvider(ReindexService)
      .useValue(mockReindexService)
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
    await app.close();
  });

  describe('GET /api/v1/admin/search/status', () => {
    it('returns 403 for anonymous requests (RX-I-01)', async () => {
      const res = await request(app.getHttpServer()).get(
        '/api/v1/admin/search/status',
      );
      expect(res.status).toBe(403);
      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('returns 403 for seller lacking SEARCH_ADMIN role (RX-I-04)', async () => {
      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set('x-user-roles', 'SELLER');
      expect(res.status).toBe(403);
      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('returns 200 without step-up for admin (RX-I-09, RX-I-40)', async () => {
      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/status')
        .set('x-user-roles', 'SEARCH_ADMIN');
      expect(res.status).toBe(200);
      expect(res.body.data.reindex_state).toBe('IDLE');
      expect(res.body.data.alias).toBe('products-read');
      expect(res.body.meta.request_id).toBeDefined();
    });
  });

  describe('POST /api/v1/admin/search/reindex', () => {
    it('returns 403 for anonymous request (RX-I-02)', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Test reindex',
        });
      expect(res.status).toBe(403);
      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('returns 403 when missing X-MFA-Step-Up header (RX-I-06)', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set('x-user-roles', 'SEARCH_ADMIN')
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Test reindex',
        });
      expect(res.status).toBe(403);
      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('returns 400 for empty body (RX-I-11)', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set('x-user-roles', 'SEARCH_ADMIN')
        .set('x-mfa-step-up', 'true')
        .send({});
      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
    });

    it('returns 400 for invalid source (RX-I-12)', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set('x-user-roles', 'SEARCH_ADMIN')
        .set('x-mfa-step-up', 'true')
        .send({
          source: 'INVALID_ENUM',
          mapping_version: 1,
          reason: 'valid reason',
        });
      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
    });

    it('returns 400 for empty reason (RX-I-13)', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set('x-user-roles', 'SEARCH_ADMIN')
        .set('x-mfa-step-up', 'true')
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: '',
        });
      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
    });

    it('returns 202 Accepted with valid payload and step-up (RX-I-08, RX-I-14)', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set('x-user-roles', 'SEARCH_ADMIN')
        .set('x-mfa-step-up', 'true')
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'mapping update',
        });
      expect(res.status).toBe(202);
      expect(res.body.data.job_id).toBe('job-01912fb8-1111');
      expect(res.body.data.state).toBe(ReindexState.REQUESTED);
      expect(res.body.data.target_index).toBe('products-v1-20261014120000');
      expect(res.body.meta.request_id).toBeDefined();
    });
  });

  describe('GET /api/v1/admin/search/reindex/:jobId', () => {
    it('returns 403 for anonymous requests (RX-I-03)', async () => {
      const res = await request(app.getHttpServer()).get(
        '/api/v1/admin/search/reindex/job-01912fb8-1111',
      );
      expect(res.status).toBe(403);
      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('returns 200 for admin (RX-I-10, RX-I-36)', async () => {
      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/reindex/job-01912fb8-1111')
        .set('x-user-roles', 'SEARCH_ADMIN');
      expect(res.status).toBe(200);
      expect(res.body.data.job_id).toBe('job-01912fb8-1111');
      expect(res.body.data.state).toBe(ReindexState.RUNNING);
      expect(res.body.meta.request_id).toBeDefined();
    });

    it('returns 404 for unknown jobId (RX-I-39)', async () => {
      const res = await request(app.getHttpServer())
        .get('/api/v1/admin/search/reindex/job-unknown-9999')
        .set('x-user-roles', 'SEARCH_ADMIN');
      expect(res.status).toBe(404);
      expect(res.body.error.code).toBe(
        SearchErrorCode.SEARCH_REINDEX_NOT_FOUND,
      );
    });
  });

  describe('POST /api/v1/admin/search/reindex/:jobId/cancel', () => {
    it('returns 403 when missing step-up header', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex/job-01912fb8-1111/cancel')
        .set('x-user-roles', 'SEARCH_ADMIN');
      expect(res.status).toBe(403);
      expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    });

    it('returns 200 and marks CANCELLED when step-up provided', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex/job-01912fb8-1111/cancel')
        .set('x-user-roles', 'SEARCH_ADMIN')
        .set('x-mfa-step-up', 'true');
      expect(res.status).toBe(200);
      expect(res.body.data.job_id).toBe('job-01912fb8-1111');
      expect(res.body.data.state).toBe(ReindexState.CANCELLED);
    });
  });
});
