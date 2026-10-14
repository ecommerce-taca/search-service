import { Client, errors } from '@elastic/elasticsearch';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';
import {
  CategoryProjection,
  ICategoryProjection,
} from '../product-index/interfaces';
import { SearchRequestBuilder } from './es/search-request.builder';
import { QueryParser } from './query-parser';
import { SearchResultMapper } from './result.mapper';
import { SearchProductsService } from './search-products.service';

describe('SearchProductsService', () => {
  let service: SearchProductsService;
  let mockSearchFn: jest.Mock;
  let mockGetManyFn: jest.Mock;

  beforeEach(() => {
    mockSearchFn = jest.fn();
    const mockEsClient = {
      search: mockSearchFn,
    } as unknown as Client;

    const mockConfigService = {
      esIndexPrefix: 'test-',
    } as unknown as AppConfigService;

    mockGetManyFn = jest.fn();
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

  it('maps ES TimeoutError to 504 SEARCH_TIMEOUT (D5)', async () => {
    mockSearchFn.mockRejectedValue(
      new errors.TimeoutError('Request timed out'),
    );

    const parsed = QueryParser.parse({ q: 'slow' });
    await expect(service.search(parsed)).rejects.toThrow(SearchException);

    try {
      await service.search(parsed);
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_TIMEOUT);
      expect(e.getStatus()).toBe(504);
    }
  });

  it('filters category facets by D4 direct-children scope (root vs child)', async () => {
    const categoriesMap = new Map<string, CategoryProjection>([
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
    ]);

    mockGetManyFn.mockResolvedValue(categoriesMap);

    mockSearchFn.mockResolvedValue({
      hits: {
        total: 5,
        hits: [],
      },
      aggregations: {
        category: {
          values: {
            buckets: [
              { key: 'cat-fashion', doc_count: 2 },
              { key: 'cat-outerwear', doc_count: 2 },
              { key: 'cat-tech', doc_count: 3 },
            ],
          },
        },
        brand: { values: { buckets: [] } },
        price: { values: { buckets: [] } },
        attr_rest: { values: { buckets: [] } },
        index_as_of: { value_as_string: '2026-09-01T10:00:00Z' },
      },
    });

    // Case 1: Without category filter -> only root categories (parent_id is null)
    const browseRes = await service.search(QueryParser.parse({}));
    expect(browseRes.meta.facets?.category).toEqual([
      { category_id: 'cat-tech', name: 'Đồ công nghệ', count: 3 },
      { category_id: 'cat-fashion', name: 'Thời trang', count: 2 },
    ]);

    // Case 2: With category filter -> only direct children of cat-fashion
    const childRes = await service.search(
      QueryParser.parse({ category_id: 'cat-fashion' }),
    );
    expect(childRes.meta.facets?.category).toEqual([
      { category_id: 'cat-outerwear', name: 'Áo khoác', count: 2 },
    ]);
  });
});
