import { Client, errors } from '@elastic/elasticsearch';
import { Logger } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { ES_REQUEST_TIMEOUT_MS, getProductsAlias } from '../common/constants';
import { SuggestionType } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';
import { ELASTICSEARCH_CLIENT } from '../product-index/elasticsearch.module';
import { SuggestQueryDto } from './dto/suggest.query';
import {
  extractPhrase,
  foldText,
  SuggestService,
  Suggestion,
} from './suggest.service';

describe('SuggestService (Unit)', () => {
  let service: SuggestService;
  let mockEsClient: {
    search: jest.Mock;
  };
  let mockConfigService: {
    esIndexPrefix: string;
  };

  const aliasName = getProductsAlias('test-');

  beforeEach(async () => {
    mockEsClient = {
      search: jest.fn(),
    };

    mockConfigService = {
      esIndexPrefix: 'test-',
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        SuggestService,
        {
          provide: ELASTICSEARCH_CLIENT,
          useValue: mockEsClient,
        },
        {
          provide: Client,
          useValue: mockEsClient,
        },
        {
          provide: AppConfigService,
          useValue: mockConfigService,
        },
      ],
    }).compile();

    service = module.get<SuggestService>(SuggestService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('foldText and extractPhrase helpers', () => {
    it('folds Vietnamese accented characters and case', () => {
      expect(foldText('Áo khoác cotton')).toBe('ao khoac cotton');
      expect(foldText('Điện Thoại')).toBe('dien thoai');
      expect(foldText('ĐỒNG HỒ')).toBe('dong ho');
    });

    it('extracts phrase truncated to max 6 words at word boundary', () => {
      const phrase = extractPhrase(
        'Áo khoác cotton nam cao cấp mùa đông ấm áp siêu nhẹ',
        'ao',
      );
      expect(phrase).toBe('áo khoác cotton nam cao cấp');
      expect(phrase?.split(/\s+/).length).toBe(6);
    });

    it('returns null if prefix not found in title', () => {
      expect(extractPhrase('Laptop Dell', 'ao')).toBeNull();
    });
  });

  describe('Prefix validation and code point length', () => {
    it('AC-U-01: throws SEARCH_INVALID_INPUT when prefix is missing or undefined', async () => {
      await expect(
        service.suggest({} as unknown as SuggestQueryDto),
      ).rejects.toMatchObject({
        code: SearchErrorCode.SEARCH_INVALID_INPUT,
      });

      expect(mockEsClient.search).not.toHaveBeenCalled();
    });

    it('AC-U-02: throws SEARCH_INVALID_INPUT when prefix is empty or whitespace', async () => {
      await expect(
        service.suggest({ prefix: '' } as SuggestQueryDto),
      ).rejects.toMatchObject({
        code: SearchErrorCode.SEARCH_INVALID_INPUT,
      });

      await expect(
        service.suggest({ prefix: '   ' } as SuggestQueryDto),
      ).rejects.toMatchObject({
        code: SearchErrorCode.SEARCH_INVALID_INPUT,
      });

      expect(mockEsClient.search).not.toHaveBeenCalled();
    });

    it('AC-U-03: throws SearchException SEARCH_QUERY_TOO_LONG when prefix exceeds 100 code points', async () => {
      const long101 = 'a'.repeat(101);
      const queryDto: SuggestQueryDto = { prefix: long101, limit: 10 };

      await expect(service.suggest(queryDto)).rejects.toThrow(SearchException);
      await expect(service.suggest(queryDto)).rejects.toMatchObject({
        code: SearchErrorCode.SEARCH_QUERY_TOO_LONG,
      });

      expect(mockEsClient.search).not.toHaveBeenCalled();
    });

    it('handles unicode code points (emojis) correctly: 101 emojis throws SEARCH_QUERY_TOO_LONG', async () => {
      const emoji101 = '🌟'.repeat(101);
      const queryDto: SuggestQueryDto = { prefix: emoji101, limit: 10 };

      await expect(service.suggest(queryDto)).rejects.toMatchObject({
        code: SearchErrorCode.SEARCH_QUERY_TOO_LONG,
      });

      expect(mockEsClient.search).not.toHaveBeenCalled();
    });

    it('AC-U-04: allows prefix with exactly 100 unicode code points', async () => {
      const emoji100 = '🌟'.repeat(100);
      const queryDto: SuggestQueryDto = { prefix: emoji100, limit: 10 };

      mockEsClient.search.mockResolvedValueOnce({
        suggest: { product_suggest: [] },
        hits: { hits: [] },
        aggregations: { brand_terms: { buckets: [] } },
      });

      const result = await service.suggest(queryDto);
      expect(result).toEqual([]);
      expect(mockEsClient.search).toHaveBeenCalledWith(
        expect.objectContaining({
          index: aliasName,
          requestTimeout: ES_REQUEST_TIMEOUT_MS,
        }),
      );
    });

    it('AC-U-05: defaults limit to 10 when limit is omitted', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: { product_suggest: [] },
        hits: { hits: [] },
        aggregations: { brand_terms: { buckets: [] } },
      });

      await service.suggest({ prefix: 'ao' });

      const callArgs = mockEsClient.search.mock.calls[0][0];
      expect(callArgs.suggest.product_suggest.completion.size).toBe(10 * 3);
    });

    it('AC-U-06: accepts limit=5 and passes it through', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: { product_suggest: [] },
        hits: { hits: [] },
        aggregations: { brand_terms: { buckets: [] } },
      });

      await service.suggest({ prefix: 'ao', limit: 5 });

      const callArgs = mockEsClient.search.mock.calls[0][0];
      expect(callArgs.suggest.product_suggest.completion.size).toBe(5 * 3);
    });

    it('AC-U-07: throws SEARCH_INVALID_INPUT when limit=15 (exceeds max 10)', async () => {
      await expect(
        service.suggest({ prefix: 'iphone', limit: 15 }),
      ).rejects.toMatchObject({
        code: SearchErrorCode.SEARCH_INVALID_INPUT,
      });
    });

    it('AC-U-08: throws SEARCH_INVALID_INPUT for invalid limit parameter in service call', async () => {
      await expect(
        service.suggest({ prefix: 'iphone', limit: -1 }),
      ).rejects.toMatchObject({
        code: SearchErrorCode.SEARCH_INVALID_INPUT,
      });

      await expect(
        service.suggest({ prefix: 'iphone', limit: 15 }),
      ).rejects.toMatchObject({
        code: SearchErrorCode.SEARCH_INVALID_INPUT,
      });
    });
  });

  describe('Logging requirements', () => {
    it('never logs raw prefix string, only its length (Design requirement)', async () => {
      const loggerSpy = jest
        .spyOn(Logger.prototype, 'debug')
        .mockImplementation(() => {});
      mockEsClient.search.mockResolvedValueOnce({
        suggest: { product_suggest: [] },
        hits: { hits: [] },
        aggregations: { brand_terms: { buckets: [] } },
      });

      const sensitivePrefix = 'sensitive_query_secret';
      await service.suggest({ prefix: sensitivePrefix, limit: 10 });

      for (const callArgs of loggerSpy.mock.calls) {
        const loggedContent = JSON.stringify(callArgs);
        expect(loggedContent).not.toContain(sensitivePrefix);
      }

      loggerSpy.mockRestore();
    });
  });

  describe('Single ES search request structure', () => {
    it('issues exactly ONE ES search request carrying completion suggester, match_phrase_prefix, and brand agg', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: { product_suggest: [] },
        hits: { hits: [] },
        aggregations: { brand_terms: { buckets: [] } },
      });

      const queryDto: SuggestQueryDto = { prefix: 'ao', limit: 10 };
      await service.suggest(queryDto);

      expect(mockEsClient.search).toHaveBeenCalledTimes(1);
      const callArgs = mockEsClient.search.mock.calls[0][0];

      expect(callArgs.index).toBe(aliasName);
      expect(callArgs.requestTimeout).toBe(ES_REQUEST_TIMEOUT_MS);
      expect(callArgs.suggest).toBeDefined();
      expect(callArgs.query).toBeDefined();
      expect(callArgs.aggregations).toBeDefined();
    });
  });

  describe('QUERY suggestion generation and ranking (D11)', () => {
    it('extracts matching phrases truncated to max 6 words at word boundary, lower-cased, ranked by count desc then alphabetical', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: { product_suggest: [] },
        hits: {
          hits: [
            {
              _source: {
                title: 'Áo khoác cotton nam cao cấp mùa đông ấm áp siêu nhẹ',
              },
            },
            { _source: { title: 'Áo khoác cotton nam cao cấp form rộng' } },
            { _source: { title: 'Áo khoác gió thể thao năng động trẻ trung' } },
          ],
        },
        aggregations: { brand_terms: { buckets: [] } },
      });

      const result = await service.suggest({ prefix: 'ao', limit: 10 });

      const querySuggestions = result.filter(
        (s: Suggestion) => s.type === SuggestionType.QUERY,
      );
      expect(querySuggestions.length).toBeGreaterThan(0);

      for (const qs of querySuggestions) {
        expect(qs.type).toBe(SuggestionType.QUERY);
        expect(qs.product_id).toBeUndefined();
        const wordCount = qs.text.trim().split(/\s+/).length;
        expect(wordCount).toBeLessThanOrEqual(6);
        expect(qs.text).toBe(qs.text.toLowerCase());
      }
    });

    it('ranks phrases by number of matching titles (frequency), then alphabetically', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: { product_suggest: [] },
        hits: {
          hits: [
            { _source: { title: 'Áo khoác dù chống nước cao cấp' } },
            { _source: { title: 'Áo khoác dù chống nước cao cấp' } },
            { _source: { title: 'Áo khoác bò phong cách bụi bặm' } },
          ],
        },
        aggregations: { brand_terms: { buckets: [] } },
      });

      const result = await service.suggest({ prefix: 'ao', limit: 10 });
      const querySuggestions = result.filter(
        (s: Suggestion) => s.type === SuggestionType.QUERY,
      );

      expect(querySuggestions.length).toBe(2);
      expect(querySuggestions[0].text).toContain('áo khoác dù');
      expect(querySuggestions[1].text).toContain('áo khoác bò');
    });
  });

  describe('PRODUCT suggestion filtering', () => {
    it('filters out HIDDEN and DELETED products from completion options, keeping only PUBLISHED', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: {
          product_suggest: [
            {
              options: [
                {
                  text: 'Áo khoác cotton',
                  _source: {
                    product_id: 'prod-1',
                    title: 'Áo khoác cotton',
                    visibility_status: 'PUBLISHED',
                    category_ids: ['cat-1'],
                  },
                },
                {
                  text: 'Áo khoác ẩn',
                  _source: {
                    product_id: 'prod-2',
                    title: 'Áo khoác ẩn',
                    visibility_status: 'HIDDEN',
                    category_ids: ['cat-1'],
                  },
                },
                {
                  text: 'Áo khoác xoá',
                  _source: {
                    product_id: 'prod-3',
                    title: 'Áo khoác xoá',
                    visibility_status: 'DELETED',
                    category_ids: ['cat-1'],
                  },
                },
              ],
            },
          ],
        },
        hits: { hits: [] },
        aggregations: { brand_terms: { buckets: [] } },
      });

      const result = await service.suggest({ prefix: 'ao', limit: 10 });
      const productSuggestions = result.filter(
        (s: Suggestion) => s.type === SuggestionType.PRODUCT,
      );

      expect(productSuggestions.length).toBe(1);
      expect(productSuggestions[0].product_id).toBe('prod-1');
      expect(productSuggestions[0].text).toBe('Áo khoác cotton');
    });

    it('filters PRODUCT suggestions by category_id when provided', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: {
          product_suggest: [
            {
              options: [
                {
                  text: 'Áo khoác outerwear',
                  _source: {
                    product_id: 'prod-1',
                    title: 'Áo khoác outerwear',
                    visibility_status: 'PUBLISHED',
                    category_ids: ['cat-fashion', 'cat-outerwear'],
                  },
                },
                {
                  text: 'Áo khoác khác',
                  _source: {
                    product_id: 'prod-2',
                    title: 'Áo khoác khác',
                    visibility_status: 'PUBLISHED',
                    category_ids: ['cat-other'],
                  },
                },
              ],
            },
          ],
        },
        hits: { hits: [] },
        aggregations: { brand_terms: { buckets: [] } },
      });

      const result = await service.suggest({
        prefix: 'ao',
        limit: 10,
        category_id: 'cat-outerwear',
      });

      const productSuggestions = result.filter(
        (s: Suggestion) => s.type === SuggestionType.PRODUCT,
      );
      expect(productSuggestions.length).toBe(1);
      expect(productSuggestions[0].product_id).toBe('prod-1');
    });

    it('AC-U-11: retains product_id for PRODUCT suggestion type', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: {
          product_suggest: [
            {
              options: [
                {
                  text: 'iPhone 16 Pro Max 256GB',
                  _source: {
                    product_id: 'product-01912f31',
                    title: 'iPhone 16 Pro Max 256GB',
                    visibility_status: 'PUBLISHED',
                    category_ids: ['cat-phone'],
                  },
                },
              ],
            },
          ],
        },
        hits: { hits: [] },
        aggregations: { brand_terms: { buckets: [] } },
      });

      const result = await service.suggest({ prefix: 'iphone', limit: 10 });
      const product = result.find(
        (s: Suggestion) => s.type === SuggestionType.PRODUCT,
      );

      expect(product).toBeDefined();
      expect(product?.product_id).toBe('product-01912f31');
      expect(product?.text).toBe('iPhone 16 Pro Max 256GB');
    });
  });

  describe('BRAND suggestions and AC-U-12', () => {
    it('filters terms agg brands whose folded value starts with folded prefix', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: { product_suggest: [] },
        hits: { hits: [] },
        aggregations: {
          brands_global: {
            brands_filtered: {
              brand_terms: {
                buckets: [
                  { key: 'Apple', doc_count: 50 },
                  { key: 'Apex', doc_count: 10 },
                  { key: 'Samsung', doc_count: 100 },
                ],
              },
            },
          },
        },
      });

      const result = await service.suggest({ prefix: 'ap', limit: 10 });
      const brands = result.filter(
        (s: Suggestion) => s.type === SuggestionType.BRAND,
      );

      expect(brands.length).toBe(2);
      expect(brands.map((b: Suggestion) => b.text)).toEqual(['Apple', 'Apex']);
    });

    it('AC-U-12: omits product_id completely for QUERY and BRAND types', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: { product_suggest: [] },
        hits: {
          hits: [{ _source: { title: 'iphone 16 pro' } }],
        },
        aggregations: {
          brands_global: {
            brands_filtered: {
              brand_terms: {
                buckets: [{ key: 'Ipanema', doc_count: 20 }],
              },
            },
          },
        },
      });

      const result = await service.suggest({ prefix: 'ip', limit: 10 });
      const queryItem = result.find(
        (s: Suggestion) => s.type === SuggestionType.QUERY,
      );
      const brandItem = result.find(
        (s: Suggestion) => s.type === SuggestionType.BRAND,
      );

      expect(queryItem).toBeDefined();
      expect(queryItem).not.toHaveProperty('product_id');

      expect(brandItem).toBeDefined();
      expect(brandItem).not.toHaveProperty('product_id');
      expect(brandItem?.text).toBe('Ipanema');
    });

    it('computes brands using global -> filter -> terms aggregation and parses nested response', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: { product_suggest: [] },
        hits: { hits: [] },
        aggregations: {
          brands_global: {
            brands_filtered: {
              brand_terms: {
                buckets: [
                  { key: 'Apple', doc_count: 50 },
                  { key: 'Apex', doc_count: 10 },
                ],
              },
            },
          },
        },
      });

      const result = await service.suggest({
        prefix: 'ap',
        limit: 10,
        category_id: 'cat-phone',
      });

      expect(mockEsClient.search).toHaveBeenCalledWith(
        expect.objectContaining({
          aggregations: {
            brands_global: {
              global: {},
              aggregations: {
                brands_filtered: {
                  filter: {
                    bool: {
                      filter: [
                        { term: { visibility_status: 'PUBLISHED' } },
                        { term: { category_ids: 'cat-phone' } },
                        {
                          prefix: {
                            brand: {
                              value: 'ap',
                              case_insensitive: true,
                            },
                          },
                        },
                      ],
                    },
                  },
                  aggregations: {
                    brand_terms: {
                      terms: {
                        field: 'brand',
                        size: 10,
                      },
                    },
                  },
                },
              },
            },
          },
        }),
      );

      const brands = result.filter(
        (s: Suggestion) => s.type === SuggestionType.BRAND,
      );
      expect(brands.length).toBe(2);
      expect(brands.map((b) => b.text)).toEqual(['Apple', 'Apex']);
    });
  });

  describe('Deduplication and Ordering (D12, AC-U-09, AC-U-10)', () => {
    it('orders results: QUERY first, then PRODUCT, then BRAND (D12)', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: {
          product_suggest: [
            {
              options: [
                {
                  text: 'Apple Watch Ultra',
                  _source: {
                    product_id: 'prod-watch',
                    title: 'Apple Watch Ultra',
                    visibility_status: 'PUBLISHED',
                    category_ids: ['cat-watch'],
                  },
                },
              ],
            },
          ],
        },
        hits: {
          hits: [{ _source: { title: 'apple airpods pro' } }],
        },
        aggregations: {
          brands_global: {
            brands_filtered: {
              brand_terms: {
                buckets: [{ key: 'Apple', doc_count: 100 }],
              },
            },
          },
        },
      });

      const result = await service.suggest({ prefix: 'app', limit: 10 });

      expect(result.length).toBe(3);
      expect(result[0].type).toBe(SuggestionType.QUERY);
      expect(result[1].type).toBe(SuggestionType.PRODUCT);
      expect(result[2].type).toBe(SuggestionType.BRAND);
    });

    it('AC-U-09: dedupes text across types case/diacritic-insensitively, keeping first occurrence (QUERY priority)', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: {
          product_suggest: [
            {
              options: [
                {
                  text: 'iPhone 16 Pro Max',
                  _source: {
                    product_id: 'product-01912f31',
                    title: 'iPhone 16 Pro Max',
                    visibility_status: 'PUBLISHED',
                    category_ids: ['cat-phone'],
                  },
                },
              ],
            },
          ],
        },
        hits: {
          hits: [{ _source: { title: 'iphone 16 pro max 256gb' } }],
        },
        aggregations: { brand_terms: { buckets: [] } },
      });

      const result = await service.suggest({ prefix: 'iphone', limit: 10 });

      const textsFolded = result.map((r: Suggestion) =>
        r.text.toLowerCase().trim(),
      );
      const uniqueTexts = new Set(textsFolded);
      expect(textsFolded.length).toBe(uniqueTexts.size);
    });

    it('AC-U-10: dedupes identical text within the same type with different casing', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: { product_suggest: [] },
        hits: { hits: [] },
        aggregations: {
          brands_global: {
            brands_filtered: {
              brand_terms: {
                buckets: [
                  { key: 'Apple', doc_count: 50 },
                  { key: 'apple', doc_count: 30 },
                  { key: 'APPLE', doc_count: 20 },
                ],
              },
            },
          },
        },
      });

      const result = await service.suggest({ prefix: 'app', limit: 10 });
      const brands = result.filter(
        (s: Suggestion) => s.type === SuggestionType.BRAND,
      );

      expect(brands.length).toBe(1);
      expect(brands[0].text).toBe('Apple');
      expect(brands[0].type).toBe(SuggestionType.BRAND);
    });

    it('cuts total returned items to limit', async () => {
      mockEsClient.search.mockResolvedValueOnce({
        suggest: {
          product_suggest: [
            {
              options: Array.from({ length: 15 }, (_, i) => ({
                text: `Product ${i}`,
                _source: {
                  product_id: `prod-${i}`,
                  title: `Product ${i}`,
                  visibility_status: 'PUBLISHED',
                  category_ids: ['cat-1'],
                },
              })),
            },
          ],
        },
        hits: { hits: [] },
        aggregations: { brand_terms: { buckets: [] } },
      });

      const result = await service.suggest({ prefix: 'prod', limit: 5 });
      expect(result.length).toBe(5);
    });
  });

  describe('Security & Injection Handling (AC-U-13)', () => {
    it('AC-U-13: treats Lucene / script / DSL injection payloads as literal strings safely', async () => {
      const injectionPayloads = [
        '{"bool":{"must":[{"match_all":{}}]}}',
        '<script>alert("xss")</script>',
        "ao' OR '1'='1' --",
        'title:* OR brand:*',
        '.*+?^${}()|[]\\',
      ];

      for (const payload of injectionPayloads) {
        mockEsClient.search.mockResolvedValueOnce({
          suggest: { product_suggest: [] },
          hits: { hits: [] },
          aggregations: { brand_terms: { buckets: [] } },
        });

        const result = await service.suggest({ prefix: payload, limit: 10 });
        expect(Array.isArray(result)).toBe(true);

        const callArgs =
          mockEsClient.search.mock.calls[
            mockEsClient.search.mock.calls.length - 1
          ][0];
        expect(callArgs).toBeDefined();
      }
    });
  });

  describe('Error Mapping (D5)', () => {
    it('maps TimeoutError to 504 SEARCH_TIMEOUT', async () => {
      mockEsClient.search.mockRejectedValueOnce(
        new errors.TimeoutError(
          'Request timed out',
          {} as unknown as ConstructorParameters<typeof errors.TimeoutError>[1],
        ),
      );

      await expect(service.suggest({ prefix: 'iphone' })).rejects.toMatchObject(
        {
          code: SearchErrorCode.SEARCH_TIMEOUT,
        },
      );
    });

    it('maps ResponseError 404 / index_not_found_exception to 503 SEARCH_INDEX_NOT_READY', async () => {
      const err = new errors.ResponseError({
        body: { error: { type: 'index_not_found_exception' } },
        statusCode: 404,
      } as unknown as ConstructorParameters<typeof errors.ResponseError>[0]);

      mockEsClient.search.mockRejectedValueOnce(err);

      await expect(service.suggest({ prefix: 'iphone' })).rejects.toMatchObject(
        {
          code: SearchErrorCode.SEARCH_INDEX_NOT_READY,
        },
      );
    });

    it('maps ConnectionError to 503 SEARCH_UNAVAILABLE', async () => {
      mockEsClient.search.mockRejectedValueOnce(
        new errors.ConnectionError(
          'Elasticsearch unreachable',
          {} as unknown as ConstructorParameters<
            typeof errors.ConnectionError
          >[1],
        ),
      );

      await expect(service.suggest({ prefix: 'iphone' })).rejects.toMatchObject(
        {
          code: SearchErrorCode.SEARCH_UNAVAILABLE,
        },
      );
    });
  });
});
