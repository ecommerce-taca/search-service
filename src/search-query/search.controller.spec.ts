import { SearchProductsQueryDto } from './dto/search-products.query';
import { QueryParser } from './query-parser';
import { SearchController } from './search.controller';
import { SearchProductsService } from './search-products.service';

describe('SearchController', () => {
  let controller: SearchController;
  let mockSearchFn: jest.Mock;
  let mockParseFn: jest.Mock;

  beforeEach(() => {
    mockSearchFn = jest.fn().mockResolvedValue({
      data: [{ product_id: 'prod-001' }],
      meta: {
        page: 1,
        size: 20,
        total: 1,
        total_pages: 1,
        index_as_of: '2026-09-01T10:00:00Z',
      },
    });
    mockParseFn = jest.fn().mockReturnValue({ page: 1, size: 20 });

    const mockSearchService = {
      search: mockSearchFn,
    } as unknown as SearchProductsService;

    const mockQueryParser = {
      parse: mockParseFn,
    } as unknown as QueryParser;

    controller = new SearchController(mockSearchService, mockQueryParser);
  });

  it('delegates to QueryParser and SearchProductsService and returns createResponse envelope', async () => {
    const res = await controller.search({
      q: 'iPhone',
    } as unknown as SearchProductsQueryDto);

    expect(mockParseFn).toHaveBeenCalledWith({ q: 'iPhone' });
    expect(mockSearchFn).toHaveBeenCalled();
    expect(res).toEqual({
      data: [{ product_id: 'prod-001' }],
      meta: {
        request_id: expect.any(String),
        page: 1,
        size: 20,
        total: 1,
        total_pages: 1,
        index_as_of: '2026-09-01T10:00:00Z',
      },
    });
  });
});
