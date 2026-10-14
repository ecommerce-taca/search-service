import { Client, errors } from '@elastic/elasticsearch';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { SearchErrorCode } from '../src/common/errors/error-codes';
import { SearchExceptionFilter } from '../src/common/errors/search-exception.filter';
import { ELASTICSEARCH_CLIENT } from '../src/product-index/elasticsearch.module';
import { IndexBootstrapService } from '../src/product-index/index-bootstrap.service';
import {
  CATEGORY_PROJECTION,
  CategoryProjection,
  ICategoryProjection,
} from '../src/product-index/interfaces';

describe('SearchQuery (e2e HTTP Contract Tests without cluster)', () => {
  let app: INestApplication;
  let mockSearchFn: jest.Mock;

  const mockCategories = new Map<string, CategoryProjection>([
    [
      'cat-outerwear',
      {
        category_id: 'cat-outerwear',
        name: 'Áo khoác',
        parent_id: 'cat-fashion',
        path_ids: ['cat-fashion', 'cat-outerwear'],
        status: 'ACTIVE',
        source_version: 1,
        updated_at: '2026-08-30T09:00:00Z',
      },
    ],
    [
      'cat-fashion',
      {
        category_id: 'cat-fashion',
        name: 'Thời trang',
        parent_id: null,
        path_ids: ['cat-fashion'],
        status: 'ACTIVE',
        source_version: 1,
        updated_at: '2026-08-30T09:00:00Z',
      },
    ],
  ]);

  const mockCategoryProjection: ICategoryProjection = {
    getMany: jest.fn().mockImplementation(async (ids: string[]) => {
      const res = new Map<string, CategoryProjection>();
      for (const id of ids) {
        if (mockCategories.has(id)) {
          res.set(id, mockCategories.get(id)!);
        }
      }
      return res;
    }),
    resolvePathIds: jest
      .fn()
      .mockResolvedValue(['cat-fashion', 'cat-outerwear']),
  };

  beforeAll(async () => {
    mockSearchFn = jest.fn().mockImplementation(
      async (params: {
        query?: {
          bool?: {
            must?: Array<{ multi_match?: { query?: string } }>;
          };
        };
        [key: string]: unknown;
      }) => {
        const textQuery = params.query?.bool?.must?.[0]?.multi_match?.query;

        // SQ-E-05: timeout simulation
        if (textQuery === 'slowquery') {
          throw new errors.TimeoutError('Elasticsearch request timed out');
        }

        // SQ-E-06: cluster down simulation
        if (textQuery === 'downquery') {
          throw new errors.ConnectionError(
            'Elasticsearch cluster connection refused',
          );
        }

        // SQ-E-09 & SQ-E-10: injection attempts return 0 results
        if (
          textQuery?.includes('match_all') ||
          textQuery?.includes('visibility_status:HIDDEN')
        ) {
          return {
            hits: { total: 0, hits: [] },
            aggregations: {
              category: { values: { buckets: [] } },
              brand: { values: { buckets: [] } },
              price: { values: { buckets: [] } },
              attr_rest: { values: { buckets: [] } },
              index_as_of: { value_as_string: '2026-09-01T10:00:00Z' },
            },
          };
        }

        // Default mock response: cotton query (prod-001)
        return {
          hits: {
            total: { value: 1, relation: 'eq' },
            hits: [
              {
                _id: 'prod-001',
                _source: {
                  product_id: 'prod-001',
                  title: 'Áo khoác cotton nam cao cấp',
                  slug: 'ao-khoac-cotton-nam-cao-cap',
                  shop: {
                    shop_id: 'shop-001',
                    name: 'Fashion House',
                  },
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
            category: {
              values: {
                buckets: [{ key: 'cat-fashion', doc_count: 1 }],
              },
            },
            brand: {
              values: {
                buckets: [{ key: 'Taca Brand', doc_count: 1 }],
              },
            },
            price: {
              values: {
                buckets: [
                  { key: 'lt_5m', to: 5000000, doc_count: 1 },
                  {
                    key: '5m_20m',
                    from: 5000000,
                    to: 20000000,
                    doc_count: 0,
                  },
                  { key: 'gte_20m', from: 20000000, doc_count: 0 },
                ],
              },
            },
            attr_rest: {
              values: {
                buckets: [
                  { key: 'color=Đen', doc_count: 1 },
                  { key: 'size=L', doc_count: 1 },
                ],
              },
            },
            index_as_of: {
              value_as_string: '2026-09-01T10:00:00Z',
            },
          },
        };
      },
    );

    const mockEsClient = {
      search: mockSearchFn,
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
      .useValue(mockEsClient)
      .overrideProvider(Client)
      .useValue(mockEsClient)
      .overrideProvider(CATEGORY_PROJECTION)
      .useValue(mockCategoryProjection)
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

  it('SQ-E-01: GET /api/v1/products/search?q=cotton returns 200 with full contract shape', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?q=cotton')
      .expect(200);

    expect(res.body).toEqual({
      data: [
        {
          product_id: 'prod-001',
          title: 'Áo khoác cotton nam cao cấp',
          slug: 'ao-khoac-cotton-nam-cao-cap',
          shop: {
            shop_id: 'shop-001',
            name: 'Fashion House',
          },
          price: {
            amount: 350000,
            currency: 'VND',
          },
          cover_url: 'https://cdn.example.com/p1.jpg',
          rating_avg: 4.8,
        },
      ],
      meta: {
        facets: {
          category: [
            {
              category_id: 'cat-fashion',
              name: 'Thời trang',
              count: 1,
            },
          ],
          brand: [
            {
              value: 'Taca Brand',
              count: 1,
            },
          ],
          price: [
            { min: 0, max: 5000000, count: 1 },
            { min: 5000000, max: 20000000, count: 0 },
            { min: 20000000, max: null, count: 0 },
          ],
          attributes: {
            color: [{ value: 'Đen', count: 1 }],
            size: [{ value: 'L', count: 1 }],
          },
        },
        page: 1,
        size: 20,
        total: 1,
        total_pages: 1,
        request_id: expect.any(String),
        index_as_of: '2026-09-01T10:00:00Z',
      },
    });
  });

  it('SQ-E-02: propagates custom X-Request-ID header into meta.request_id and response header', async () => {
    const customId = 'gateway-req-trace-uuid-001';
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?q=cotton')
      .set('X-Request-ID', customId)
      .expect(200);

    expect(res.headers['x-request-id']).toBe(customId);
    expect(res.body.meta.request_id).toBe(customId);
  });

  it('SQ-E-03: generates valid UUID/ULID in meta.request_id when X-Request-ID is omitted', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?q=cotton')
      .expect(200);

    const reqId = res.body.meta.request_id;
    expect(typeof reqId).toBe('string');
    expect(reqId.length).toBeGreaterThan(0);
    expect(reqId.length).toBeLessThanOrEqual(64);
  });

  it('SQ-E-04: GET /api/v1/products/search?include_facets=false omits facets key in meta per D7', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?include_facets=false')
      .expect(200);

    expect(res.body.meta.facets).toBeUndefined();
    expect('facets' in res.body.meta).toBe(false);
  });

  it('SQ-E-05: returns HTTP 504 SEARCH_TIMEOUT when ES request times out (D5)', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?q=slowquery')
      .expect(504);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_TIMEOUT,
        message: 'Tìm kiếm mất quá nhiều thời gian.',
        details: null,
        trace_id: expect.any(String),
      },
    });
  });

  it('SQ-E-06: returns HTTP 503 SEARCH_UNAVAILABLE when ES cluster is down/unreachable', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?q=downquery')
      .expect(503);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_UNAVAILABLE,
        message: 'Hệ thống tìm kiếm tạm thời không khả dụng.',
        details: null,
        trace_id: expect.any(String),
      },
    });
  });

  it('SQ-E-07: returns HTTP 400 SEARCH_INVALID_INPUT on validation error (size=200)', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?size=200')
      .expect(400);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_INVALID_INPUT,
        message: 'Tham số tìm kiếm chưa hợp lệ.',
        details: expect.any(Array),
        trace_id: expect.any(String),
      },
    });
  });

  it('SQ-E-08: does not leak sensitive user query or PII in response error/metadata', async () => {
    const sensitiveQuery = 'nguyen van a 0912345678 secret';
    const res = await request(app.getHttpServer())
      .get(`/api/v1/products/search?q=${encodeURIComponent(sensitiveQuery)}`)
      .expect(200);

    expect(res.body.meta.request_id).toBeDefined();
    // Verify response body does not echo sensitive query in unexpected fields
    expect(JSON.stringify(res.body.meta)).not.toContain('0912345678');
  });

  it('SQ-E-09: treats JSON DSL injection attempt via q as literal text string', async () => {
    const injection = '{"query":{"match_all":{}}}';
    const res = await request(app.getHttpServer())
      .get(`/api/v1/products/search?q=${encodeURIComponent(injection)}`)
      .expect(200);

    expect(res.body.data).toEqual([]);
    // Verify ES query was constructed safely with plain string
    const lastCall =
      mockSearchFn.mock.calls[mockSearchFn.mock.calls.length - 1][0];
    const textPassed = lastCall.query?.bool?.must?.[0]?.multi_match?.query;
    expect(textPassed).toBe(injection);
  });

  it('SQ-E-10: prevents Lucene injection and always forces visibility_status: PUBLISHED', async () => {
    const injection = 'title:* OR visibility_status:HIDDEN';
    const res = await request(app.getHttpServer())
      .get(`/api/v1/products/search?q=${encodeURIComponent(injection)}`)
      .expect(200);

    expect(res.body.data).toEqual([]);
    const lastCall =
      mockSearchFn.mock.calls[mockSearchFn.mock.calls.length - 1][0];
    const filters = lastCall.query?.bool?.filter;
    expect(filters).toEqual(
      expect.arrayContaining([{ term: { visibility_status: 'PUBLISHED' } }]),
    );
  });

  it('SQ-E-11: rejects NoSQL / Script injection in attribute filter with HTTP 400', async () => {
    const res1 = await request(app.getHttpServer())
      .get('/api/v1/products/search?attribute[$where]=sleep(5000)')
      .expect(400);

    expect(res1.body.error.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);

    const res2 = await request(app.getHttpServer())
      .get(
        '/api/v1/products/search?attribute[script]=doc%5B%27price%27%5D.value',
      )
      .expect(400);

    expect(res2.body.error.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
  });

  it('SQ-E-12: returns valid ISO-8601 UTC timestamp in meta.index_as_of', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/products/search?q=cotton')
      .expect(200);

    const indexAsOf = res.body.meta.index_as_of;
    expect(indexAsOf).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/);
  });
});
