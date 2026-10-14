import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Client, errors } from '@elastic/elasticsearch';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import {
  ERROR_MESSAGES_MAP,
  SearchErrorCode,
} from '../src/common/errors/error-codes';
import { SearchExceptionFilter } from '../src/common/errors/search-exception.filter';
import { IndexBootstrapService } from '../src/product-index/index-bootstrap.service';
import { CATEGORY_PROJECTION } from '../src/product-index/interfaces';

describe('SearchQuery (e2e)', () => {
  let app: INestApplication;
  let mockSearchFn: jest.Mock;
  let mockGetManyFn: jest.Mock;

  beforeAll(async () => {
    mockSearchFn = jest.fn();
    mockGetManyFn = jest.fn().mockResolvedValue(new Map());

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
      .overrideProvider(Client)
      .useValue({
        search: mockSearchFn,
        close: jest.fn().mockResolvedValue(undefined),
      })
      .overrideProvider(CATEGORY_PROJECTION)
      .useValue({
        getMany: mockGetManyFn,
        resolvePathIds: jest.fn().mockResolvedValue([]),
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
    await app.close();
  });

  beforeEach(() => {
    mockSearchFn.mockReset();
    mockGetManyFn.mockReset();
    mockGetManyFn.mockResolvedValue(new Map());
  });

  it('GET /api/v1/products/search?size=200 returns 400 SEARCH_INVALID_INPUT (SQ-E-07)', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?size=200')
      .expect(400);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_INVALID_INPUT,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        details: expect.any(Array),
        trace_id: expect.any(String),
      },
    });
  });

  it('GET /api/v1/products/search?foo=bar returns 400 SEARCH_INVALID_INPUT', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?foo=bar')
      .expect(400);

    expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
  });

  it('GET /api/v1/products/search?attribute[$where]=sleep(5000) returns 400 (SQ-E-11)', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?attribute[$where]=sleep(5000)')
      .expect(400);

    expect(res.body.error.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
  });

  it('GET /api/v1/products/search fulfills contract and propagates X-Request-ID (SQ-E-01, SQ-E-02, SQ-E-12)', async () => {
    mockSearchFn.mockResolvedValue({
      hits: {
        total: { value: 1, relation: 'eq' },
        hits: [
          {
            _source: {
              product_id: 'prod-001',
              title: 'Áo khoác cotton',
              slug: 'ao-khoac-cotton',
              shop: { shop_id: 'shop-001', name: 'Fashion House' },
              price: 350000,
              currency: 'VND',
              cover_url: 'https://cdn.example.com/p1.jpg',
              rating_avg: 4.8,
              indexed_at: '2026-09-01T10:00:00Z',
            },
          },
        ],
      },
      aggregations: {
        category: { values: { buckets: [] } },
        brand: { values: { buckets: [] } },
        price: {
          values: {
            buckets: [{ doc_count: 1 }, { doc_count: 0 }, { doc_count: 0 }],
          },
        },
        attr_rest: { values: { buckets: [] } },
        index_as_of: { value_as_string: '2026-09-01T10:00:00Z' },
      },
    });

    const customReqId = 'gateway-req-trace-uuid-001';
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?q=cotton')
      .set('X-Request-ID', customReqId)
      .expect(200);

    expect(res.headers['x-request-id']).toBe(customReqId);
    expect(res.body.meta.request_id).toBe(customReqId);
    expect(res.body.meta.index_as_of).toMatch(
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/,
    );
    expect(res.body.data).toEqual([
      {
        product_id: 'prod-001',
        title: 'Áo khoác cotton',
        slug: 'ao-khoac-cotton',
        shop: { shop_id: 'shop-001', name: 'Fashion House' },
        price: { amount: 350000, currency: 'VND' },
        cover_url: 'https://cdn.example.com/p1.jpg',
        rating_avg: 4.8,
      },
    ]);
    expect(res.body.meta.facets).toBeDefined();
    expect(res.body.meta.facets.price).toEqual([
      { min: 0, max: 5000000, count: 1 },
      { min: 5000000, max: 20000000, count: 0 },
      { min: 20000000, max: null, count: 0 },
    ]);
  });

  it('GET /api/v1/products/search?include_facets=false omits facets key (SQ-E-04, D7)', async () => {
    mockSearchFn.mockResolvedValue({
      hits: {
        total: { value: 0 },
        hits: [],
      },
    });

    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?include_facets=false')
      .expect(200);

    expect(res.body.data).toEqual([]);
    expect(res.body.meta.facets).toBeUndefined();
    expect('facets' in res.body.meta).toBe(false);
  });

  it('GET /api/v1/products/search returns 504 SEARCH_TIMEOUT on ES timeout (SQ-E-05, D5)', async () => {
    mockSearchFn.mockRejectedValue(
      new errors.TimeoutError('Request timed out after 2000ms'),
    );

    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?q=slowquery')
      .expect(504);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_TIMEOUT,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_TIMEOUT],
        details: null,
        trace_id: expect.any(String),
      },
    });
  });

  it('GET /api/v1/products/search returns 503 SEARCH_UNAVAILABLE on ES connection error (SQ-E-06)', async () => {
    mockSearchFn.mockRejectedValue(
      new errors.ConnectionError('Connection refused to ES node'),
    );

    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?q=test')
      .expect(503);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_UNAVAILABLE,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_UNAVAILABLE],
        details: null,
        trace_id: expect.any(String),
      },
    });
  });
});
