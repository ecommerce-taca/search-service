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

describe('Suggest Autocomplete (e2e)', () => {
  let app: INestApplication;
  let mockEsClient: {
    search: jest.Mock;
    close: jest.Mock;
  };

  beforeAll(async () => {
    mockEsClient = {
      search: jest.fn().mockResolvedValue({
        suggest: {
          product_suggest: [
            {
              options: [
                {
                  text: 'iPhone 16 Pro Max 256GB',
                  _source: {
                    product_id: 'prod-01',
                    title: 'iPhone 16 Pro Max 256GB',
                    visibility_status: 'PUBLISHED',
                    category_ids: ['cat-phone'],
                  },
                },
              ],
            },
          ],
        },
        hits: {
          hits: [{ _source: { title: 'iphone 16 pro max' } }],
        },
        aggregations: {
          brand_terms: {
            buckets: [{ key: 'Apple', doc_count: 50 }],
          },
        },
      }),
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
    mockEsClient.search.mockReset();
    mockEsClient.search.mockResolvedValue({
      suggest: {
        product_suggest: [
          {
            options: [
              {
                text: 'iPhone 16 Pro Max 256GB',
                _source: {
                  product_id: 'prod-01',
                  title: 'iPhone 16 Pro Max 256GB',
                  visibility_status: 'PUBLISHED',
                  category_ids: ['cat-phone'],
                },
              },
            ],
          },
        ],
      },
      hits: {
        hits: [{ _source: { title: 'iphone 16 pro max' } }],
      },
      aggregations: {
        brand_terms: {
          buckets: [{ key: 'Apple', doc_count: 50 }],
        },
      },
    });
  });

  it('AC-E-01: GET /api/v1/search/suggest without prefix returns 400 SEARCH_INVALID_INPUT', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/search/suggest')
      .expect(400);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_INVALID_INPUT,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        details: "Query parameter 'prefix' is required.",
        trace_id: expect.any(String),
      },
    });
  });

  it('AC-E-02: GET /api/v1/search/suggest with empty prefix returns 400 SEARCH_INVALID_INPUT', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/search/suggest?prefix=')
      .expect(400);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_INVALID_INPUT,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        details: "Query parameter 'prefix' cannot be empty.",
        trace_id: expect.any(String),
      },
    });
  });

  it('AC-E-03: GET /api/v1/search/suggest with 101 chars prefix returns 400 SEARCH_QUERY_TOO_LONG', async () => {
    const long101 = 'a'.repeat(101);
    const res = await request(app.getHttpServer())
      .get(`/api/v1/search/suggest?prefix=${long101}`)
      .expect(400);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_QUERY_TOO_LONG,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_QUERY_TOO_LONG],
        details: "Query parameter 'prefix' exceeds 100 characters.",
        trace_id: expect.any(String),
      },
    });
  });

  it('AC-E-04: GET /api/v1/search/suggest with limit=15 returns 400 SEARCH_INVALID_INPUT', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/search/suggest?prefix=iphone&limit=15')
      .expect(400);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_INVALID_INPUT,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        details: 'Limit cannot exceed 10.',
        trace_id: expect.any(String),
      },
    });
  });

  it('AC-E-09: GET /api/v1/search/suggest with valid query echoes X-Request-ID and returns 200', async () => {
    const customReqId = '01912f92-7a1b-7c12-9c55-8b1c34a6d921';
    const res = await request(app.getHttpServer())
      .get('/api/v1/search/suggest?prefix=iphone')
      .set('X-Request-ID', customReqId)
      .expect(200);

    expect(res.headers['x-request-id']).toBe(customReqId);
    expect(res.body.meta.request_id).toBe(customReqId);
    expect(res.body.data).toBeDefined();
    expect(Array.isArray(res.body.data)).toBe(true);
    expect(res.body.data.length).toBeGreaterThan(0);
  });

  it('handles ES timeout with 504 SEARCH_TIMEOUT', async () => {
    mockEsClient.search.mockRejectedValue(
      new errors.TimeoutError(
        'ES search timed out',
        {} as unknown as ConstructorParameters<typeof errors.TimeoutError>[1],
      ),
    );

    const res = await request(app.getHttpServer())
      .get('/api/v1/search/suggest?prefix=iphone')
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
});
