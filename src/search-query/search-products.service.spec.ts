import { Client, errors } from '@elastic/elasticsearch';
import { SortOption } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';
import {
  CategoryProjection,
  ICategoryProjection,
} from '../product-index/interfaces';
import { SearchRequestBuilder } from './es/search-request.builder';
import { ParsedSearchQuery, QueryParser } from './query-parser';
import { SearchResultMapper } from './result.mapper';
import { SearchProductsService } from './search-products.service';

describe('SearchProductsService', () => {
  let service: SearchProductsService;
  let mockSearchFn: jest.Mock;
  let mockGetManyFn: jest.Mock;

  const mockCategoriesMap = new Map<string, CategoryProjection>([
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
      'cat-tech',
      {
        category_id: 'cat-tech',
        name: 'Đồ công nghệ',
        parent_id: null,
        path_ids: ['cat-tech'],
        status: 'ACTIVE',
        source_version: 1,
        updated_at: '2026-08-30T09:00:00Z',
      },
    ],
    [
      'cat-phone',
      {
        category_id: 'cat-phone',
        name: 'Điện thoại',
        parent_id: 'cat-tech',
        path_ids: ['cat-tech', 'cat-phone'],
        status: 'ACTIVE',
        source_version: 1,
        updated_at: '2026-08-30T09:00:00Z',
      },
    ],
    [
      'cat-laptop',
      {
        category_id: 'cat-laptop',
        name: 'Laptop',
        parent_id: 'cat-tech',
        path_ids: ['cat-tech', 'cat-laptop'],
        status: 'ACTIVE',
        source_version: 1,
        updated_at: '2026-08-30T09:00:00Z',
      },
    ],
  ]);

  beforeEach(() => {
    mockSearchFn = jest.fn();
    const mockEsClient = {
      search: mockSearchFn,
    } as unknown as Client;

    const mockConfigService = {
      esIndexPrefix: 'test-',
    } as unknown as AppConfigService;

    mockGetManyFn = jest.fn().mockResolvedValue(mockCategoriesMap);
    const mockCategoryProjection = {
      getMany: mockGetManyFn,
      resolvePathIds: jest.fn(),
    } as unknown as ICategoryProjection;

    service = new SearchProductsService(
      mockEsClient,
      mockConfigService,
      mockCategoryProjection,
      new SearchRequestBuilder(),
      new SearchResultMapper(),
    );
  });

  it('handles timeout case: throws 504 SEARCH_TIMEOUT when ES client throws TimeoutError (D5)', async () => {
    mockSearchFn.mockRejectedValue(new errors.TimeoutError('ES Request timeout'));

    const parsed: ParsedSearchQuery = {
      text: 'slowquery',
      filters: { brands: [], attributes: {} },
      sort: SortOption.RELEVANCE,
      page: 1,
      size: 20,
      includeFacets: true,
    };

    await expect(service.search(parsed)).rejects.toThrow(SearchException);

    try {
      await service.search(parsed);
      fail('Expected SearchException');
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_TIMEOUT);
      expect(e.getStatus()).toBe(504);
    }
  });

  it('maps ES ConnectionError to 503 SEARCH_UNAVAILABLE (D5)', async () => {
    mockSearchFn.mockRejectedValue(new errors.ConnectionError('ES Down'));

    const parsed = QueryParser.parse({ q: 'test' } as unknown as any);
    try {
      await service.search(parsed);
      fail('Expected SearchException');
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_UNAVAILABLE);
      expect(e.getStatus()).toBe(503);
    }
  });

  it('maps missing alias/index 404 to 503 SEARCH_INDEX_NOT_READY (D5)', async () => {
    const resErr = new errors.ResponseError({
      statusCode: 404,
      body: { error: { type: 'index_not_found_exception', reason: 'no such index' } },
      warnings: [],
      headers: {},
    } as unknown as ConstructorParameters<typeof errors.ResponseError>[0]);
    mockSearchFn.mockRejectedValue(resErr);

    const parsed = QueryParser.parse({ q: 'test' } as unknown as any);
    try {
      await service.search(parsed);
      fail('Expected SearchException');
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_INDEX_NOT_READY);
      expect(e.getStatus()).toBe(503);
    }
  });

  it('D4: resolves category facet to direct children: root when no filter, direct children when category_id=X', async () => {
    mockSearchFn.mockResolvedValue({
      hits: { total: 8, hits: [] },
      aggregations: {
        category: {
          values: {
            buckets: [
              { key: 'cat-fashion', doc_count: 2 },
              { key: 'cat-outerwear', doc_count: 2 },
              { key: 'cat-tech', doc_count: 6 },
              { key: 'cat-phone', doc_count: 4 },
              { key: 'cat-laptop', doc_count: 2 },
              { key: 'cat-unknown', doc_count: 5 },
            ],
          },
        },
        brand: { values: { buckets: [] } },
        price: { values: { buckets: [] } },
        attr_rest: { values: { buckets: [] } },
        index_as_of: { value_as_string: '2026-09-01T10:00:00Z' },
      },
    });

    // 1. Without category filter -> root categories only (cat-tech, cat-fashion), unknown dropped
    const rootResult = await service.search(QueryParser.parse({} as unknown as any));
    expect(rootResult.meta.facets?.category).toEqual([
      { category_id: 'cat-tech', name: 'Đồ công nghệ', count: 6 },
      { category_id: 'cat-fashion', name: 'Thời trang', count: 2 },
    ]);

    // 2. With category_id=cat-tech -> direct children only (cat-phone, cat-laptop)
    const techResult = await service.search(
      QueryParser.parse({ category_id: 'cat-tech' } as unknown as any),
    );
    expect(techResult.meta.facets?.category).toEqual([
      { category_id: 'cat-phone', name: 'Điện thoại', count: 4 },
      { category_id: 'cat-laptop', name: 'Laptop', count: 2 },
    ]);
  });

  it('D7: skips facet computations and omits meta.facets when include_facets=false', async () => {
    mockSearchFn.mockResolvedValue({
      hits: { total: 1, hits: [] },
    });

    const parsed = QueryParser.parse({ include_facets: false } as unknown as any);
    const result = await service.search(parsed);

    expect(result.meta.facets).toBeUndefined();
    expect(mockGetManyFn).not.toHaveBeenCalled();
  });

  it('D23: aggregates attribute facets from attr_sel_<k> and attr_rest keeping top 10 keys and <=30 values', async () => {
    mockSearchFn.mockResolvedValue({
      hits: { total: 5, hits: [] },
      aggregations: {
        category: { values: { buckets: [] } },
        brand: { values: { buckets: [] } },
        price: { values: { buckets: [] } },
        attr_sel_color: {
          values: {
            buckets: [
              { key: 'color=Đen', doc_count: 5 },
              { key: 'color=Trắng', doc_count: 2 },
            ],
          },
        },
        attr_rest: {
          values: {
            buckets: [
              { key: 'capacity_liter=256GB', doc_count: 4 },
              { key: 'capacity_liter=512GB', doc_count: 3 },
              { key: 'size=L', doc_count: 2 },
            ],
          },
        },
        index_as_of: { value_as_string: '2026-09-01T10:00:00Z' },
      },
    });

    const parsed = QueryParser.parse({
      attribute: { color: 'Đen' },
    } as unknown as any);
    const result = await service.search(parsed);

    const attrFacets = result.meta.facets?.attributes;
    expect(attrFacets).toBeDefined();
    expect(attrFacets?.color).toEqual([
      { value: 'Đen', count: 5 },
      { value: 'Trắng', count: 2 },
    ]);
    expect(attrFacets?.capacity_liter).toEqual([
      { value: '256GB', count: 4 },
      { value: '512GB', count: 3 },
    ]);
    expect(attrFacets?.size).toEqual([{ value: 'L', count: 2 }]);
  });
});
