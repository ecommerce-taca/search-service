import { RequestContext, RequestContextData } from '../common/context/request-context';
import { SortOption } from '../common/enums';
import { SearchProductsQueryDto } from './dto/search-products.query';
import { ParsedSearchQuery, QueryParser } from './query-parser';
import { SearchController } from './search.controller';
import { SearchProductsService } from './search-products.service';

describe('SearchController', () => {
  let controller: SearchController;
  let mockSearchFn: jest.Mock;
  let mockParseFn: jest.Mock;

  beforeEach(() => {
    mockSearchFn = jest.fn().mockResolvedValue({
      data: [
        {
          product_id: 'prod-001',
          title: 'Áo khoác cotton',
          slug: 'ao-khoac-cotton',
          shop: { shop_id: 'shop-001', name: 'Fashion House' },
          price: { amount: 350000, currency: 'VND' },
          cover_url: 'https://cdn.example.com/p1.jpg',
          rating_avg: 4.8,
        },
      ],
      meta: {
        page: 1,
        size: 20,
        total: 1,
        total_pages: 1,
        index_as_of: '2026-09-01T10:00:00Z',
      },
    });

    const parsedQuery: ParsedSearchQuery = {
      text: 'áo khoác',
      filters: { brands: [], attributes: {} },
      sort: SortOption.RELEVANCE,
      page: 1,
      size: 20,
      includeFacets: true,
    };
    mockParseFn = jest.fn().mockReturnValue(parsedQuery);

    const mockSearchService = {
      search: mockSearchFn,
    } as unknown as SearchProductsService;

    const mockQueryParser = {
      parse: mockParseFn,
    } as unknown as QueryParser;

    controller = new SearchController(mockSearchService, mockQueryParser);
  });

  it('delegates to QueryParser and SearchProductsService and wraps response with createResponse', async () => {
    const queryDto: SearchProductsQueryDto = {
      q: 'áo khoác',
      page: 1,
      size: 20,
      include_facets: true,
    };

    const contextData: RequestContextData = {
      requestId: 'req-test-12345',
      traceId: '12345678901234567890123456789012',
      spanId: '1234567890123456',
      traceparent: '00-12345678901234567890123456789012-1234567890123456-01',
      actor: {
        roles: [],
        permissions: [],
        mfaStepUp: false,
      },
    };

    const result = await RequestContext.run(contextData, () =>
      controller.search(queryDto),
    );

    expect(mockParseFn).toHaveBeenCalledWith(queryDto);
    expect(mockSearchFn).toHaveBeenCalled();
    expect(result).toEqual({
      data: [
        {
          product_id: 'prod-001',
          title: 'Áo khoác cotton',
          slug: 'ao-khoac-cotton',
          shop: { shop_id: 'shop-001', name: 'Fashion House' },
          price: { amount: 350000, currency: 'VND' },
          cover_url: 'https://cdn.example.com/p1.jpg',
          rating_avg: 4.8,
        },
      ],
      meta: {
        request_id: 'req-test-12345',
        page: 1,
        size: 20,
        total: 1,
        total_pages: 1,
        index_as_of: '2026-09-01T10:00:00Z',
      },
    });
  });
});
