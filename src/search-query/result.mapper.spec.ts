import { estypes } from '@elastic/elasticsearch';
import { ProductSearchDocument } from '../product-index/interfaces';
import { QueryParser } from './query-parser';
import { SearchResultMapper } from './result.mapper';

describe('SearchResultMapper', () => {
  it('maps raw document to ProductCard', () => {
    const card = SearchResultMapper.mapCard({
      product_id: 'prod-001',
      title: 'Áo khoác cotton',
      slug: 'ao-khoac-cotton',
      shop: {
        shop_id: 'shop-001',
        name: 'Fashion House',
      },
      price: 350000,
      currency: 'VND',
      cover_url: 'https://cdn.example.com/p1.jpg',
      rating_avg: 4.8,
    });

    expect(card).toEqual({
      product_id: 'prod-001',
      title: 'Áo khoác cotton',
      slug: 'ao-khoac-cotton',
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
    });
  });

  it('calculates pagination metadata correctly (SQ-U-29, SQ-U-30)', () => {
    // total = 45, size = 20 -> pages = 3
    const parsed1 = QueryParser.parse({ page: 1, size: 20 });
    const res1 = SearchResultMapper.map(
      {
        hits: { total: 45, hits: [] },
      } as unknown as estypes.SearchResponse<ProductSearchDocument>,
      parsed1,
    );
    expect(res1.meta).toMatchObject({
      page: 1,
      size: 20,
      total: 45,
      total_pages: 3,
    });

    // total = 0, size = 20 -> pages = 0
    const parsed2 = QueryParser.parse({ page: 1, size: 20 });
    const res2 = SearchResultMapper.map(
      {
        hits: { total: 0, hits: [] },
      } as unknown as estypes.SearchResponse<ProductSearchDocument>,
      parsed2,
    );
    expect(res2.meta).toMatchObject({
      page: 1,
      size: 20,
      total: 0,
      total_pages: 0,
    });
  });

  it('omits facets key in meta when include_facets=false (D7)', () => {
    const parsed = QueryParser.parse({ include_facets: false });
    const res = SearchResultMapper.map(
      {
        hits: { total: 10, hits: [] },
      } as unknown as estypes.SearchResponse<ProductSearchDocument>,
      parsed,
      {
        category: [],
        brand: [],
        price: [],
        attributes: {},
      },
    );
    expect(res.meta.facets).toBeUndefined();
    expect('facets' in res.meta).toBe(false);
  });
});
