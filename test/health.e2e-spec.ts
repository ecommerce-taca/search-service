import { Client, errors } from '@elastic/elasticsearch';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import {
  ERROR_MESSAGES_MAP,
  SearchErrorCode,
} from '../src/common/errors/error-codes';
import { SearchExceptionFilter } from '../src/common/errors/search-exception.filter';
import { IndexBootstrapService } from '../src/product-index/index-bootstrap.service';
import { ELASTICSEARCH_CLIENT } from '../src/product-index/elasticsearch.module';
import { ReadinessService } from '../src/health/readiness.service';

describe('Health & Readiness (e2e)', () => {
  let app: INestApplication;
  let mockEsClient: {
    cluster: { health: jest.Mock };
    indices: { existsAlias: jest.Mock };
    close: jest.Mock;
  };

  beforeAll(async () => {
    mockEsClient = {
      cluster: {
        health: jest.fn().mockResolvedValue({ status: 'green' }),
      },
      indices: {
        existsAlias: jest.fn().mockResolvedValue(true),
      },
      close: jest.fn().mockResolvedValue(undefined),
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
      .overrideProvider(ELASTICSEARCH_CLIENT)
      .useValue(mockEsClient as unknown as Client)
      .overrideProvider(Client)
      .useValue(mockEsClient as unknown as Client)
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

  beforeEach(() => {
    mockEsClient.cluster.health.mockReset();
    mockEsClient.cluster.health.mockResolvedValue({ status: 'green' });
    mockEsClient.indices.existsAlias.mockReset();
    mockEsClient.indices.existsAlias.mockResolvedValue(true);
  });

  it('HL-E-01: GET /api/v1/search/health returns 200 with UP status, alias, and meta', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/search/health')
      .set('X-Request-ID', '01912f91-test')
      .expect(200);

    expect(res.body).toEqual({
      data: {
        elasticsearch: 'UP',
        alias: 'products-read',
        consumer: 'PAUSED',
        event_lag_seconds: 0,
      },
      meta: {
        request_id: '01912f91-test',
      },
    });
    expect(res.headers['x-request-id']).toBe('01912f91-test');
  });

  it('HL-E-02: GET /api/v1/search/health returns 503 SEARCH_UNAVAILABLE when ES is unreachable', async () => {
    mockEsClient.cluster.health.mockRejectedValue(
      new errors.ConnectionError(
        'Elasticsearch unreachable',
        {} as unknown as ConstructorParameters<
          typeof errors.ConnectionError
        >[1],
      ),
    );

    const res = await request(app.getHttpServer())
      .get('/api/v1/search/health')
      .expect(503);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_UNAVAILABLE,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_UNAVAILABLE],
        details: 'Elasticsearch connection refused or timed out',
        trace_id: expect.any(String),
      },
    });
  });

  it('HL-E-03: GET /api/v1/search/health returns 503 SEARCH_INDEX_NOT_READY when alias is missing', async () => {
    mockEsClient.indices.existsAlias.mockResolvedValue(false);

    const res = await request(app.getHttpServer())
      .get('/api/v1/search/health')
      .expect(503);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_INDEX_NOT_READY,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INDEX_NOT_READY],
        details: 'Required alias products-read not found',
        trace_id: expect.any(String),
      },
    });
  });

  it('HL-E-04: GET /api/v1/search/health returns 200 with consumer DEGRADED and event_lag_seconds 120 (D14)', async () => {
    const readinessService = app.get(ReadinessService);
    jest.spyOn(readinessService, 'check').mockResolvedValueOnce({
      elasticsearch: 'UP',
      alias: 'products-read',
      consumer: 'DEGRADED',
      event_lag_seconds: 120,
    });

    const res = await request(app.getHttpServer())
      .get('/api/v1/search/health')
      .expect(200);

    expect(res.body).toEqual({
      data: {
        elasticsearch: 'UP',
        alias: 'products-read',
        consumer: 'DEGRADED',
        event_lag_seconds: 120,
      },
      meta: {
        request_id: expect.any(String),
      },
    });
  });

  it('HL-E-05: GET /health/live returns 200 even when Elasticsearch is DOWN', async () => {
    mockEsClient.cluster.health.mockRejectedValue(
      new errors.ConnectionError(
        'Elasticsearch down',
        {} as unknown as ConstructorParameters<
          typeof errors.ConnectionError
        >[1],
      ),
    );

    const res = await request(app.getHttpServer())
      .get('/health/live')
      .expect(200);

    expect(res.body).toEqual({
      data: {
        status: 'UP',
        service: 'search-service',
        time: expect.any(String),
      },
      meta: {
        request_id: expect.any(String),
      },
    });
  });

  it('HL-E-06: GET /api/v1/search/health complies with standard JSON success envelope and headers', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/search/health')
      .expect(200);

    expect(res.body).toHaveProperty('data');
    expect(res.body).toHaveProperty('meta');
    expect(res.body.meta).toHaveProperty('request_id');
    expect(typeof res.body.data.elasticsearch).toBe('string');
    expect(typeof res.body.data.alias).toBe('string');
  });
});
