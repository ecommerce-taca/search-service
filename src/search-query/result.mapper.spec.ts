import { estypes } from '@elastic/elasticsearch';
import { SortOption } from '../common/enums';
import { ProductSearchDocument } from '../product-index/interfaces';
import { ParsedSearchQuery } from './query-parser';
import { ResultMapper, SearchFacets, SearchResultMapper } from './result.mapper';

describe('SearchResultMapper / ResultMapper', () => {
  let mapper: SearchResultMapper;

  beforeEach(() => {
    mapper = new SearchResultMapper();
  });

  const baseParsed: ParsedSearchQuery = {
    filters: {
      brands: [],
      attributes: {},
    },
    sort: SortOption.RELEVANCE,
    page: 1,
    size: 20,
    includeFacets: true,
  };

  it('maps raw document to ProductCard including shop snapshot and cover_url (D3)', () => {
    const card = mapper.mapCard({
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
    });

    expect(card).toEqual({
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
    });
  });

  it('SQ-U-28: maps category facet items with resolved projection names', () => {
    // When SearchResultMapper receives mapped facets, it includes them in meta.facets
    const sampleFacets: SearchFacets = {
      category: [
        { category_id: 'cat-phone', name: 'Điện thoại', count: 4 },
        { category_id: 'cat-tech', name: 'Đồ công nghệ', count: 6 },
      ],
      brand: [{ value: 'Apple', count: 3 }],
      price: [
        { min: 0, max: 5000000, count: 3 },
        { min: 5000000, max: 20000000, count: 2 },
        { min: 20000000, max: null, count: 3 },
      ],
      attributes: {
        capacity_liter: [{ value: '256GB', count: 3 }],
      },
    };

    const res = mapper.map(
      {
        hits: { total: { value: 6, relation: 'eq' }, hits: [] },
      } as unknown as estypes.SearchResponse<ProductSearchDocument>,
      baseParsed,
      sampleFacets,
      '2026-09-01T10:00:00Z',
    );

    expect(res.meta.facets?.category).toEqual([
      { category_id: 'cat-phone', name: 'Điện thoại', count: 4 },
      { category_id: 'cat-tech', name: 'Đồ công nghệ', count: 6 },
    ]);
  });

  it('SQ-U-29: calculates pagination metadata correctly when total=45 and size=20', () => {
    const res = mapper.map(
      {
        hits: { total: 45, hits: [] },
      } as unknown as estypes.SearchResponse<ProductSearchDocument>,
      { ...baseParsed, page: 1, size: 20 },
    );

    expect(res.meta).toMatchObject({
      page: 1,
      size: 20,
      total: 45,
      total_pages: 3,
    });
  });

  it('SQ-U-30: calculates pagination metadata correctly when total=0 and size=20', () => {
    const res = mapper.map(
      {
        hits: { total: 0, hits: [] },
      } as unknown as estypes.SearchResponse<ProductSearchDocument>,
      { ...baseParsed, page: 1, size: 20 },
    );

    expect(res.meta).toMatchObject({
      page: 1,
      size: 20,
      total: 0,
      total_pages: 0,
    });
  });

  it('omits meta.facets key when include_facets=false (D7)', () => {
    const res = mapper.map(
      {
        hits: { total: 10, hits: [] },
      } as unknown as estypes.SearchResponse<ProductSearchDocument>,
      { ...baseParsed, includeFacets: false },
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

  it('is accessible via ResultMapper alias', () => {
    const altMapper = new ResultMapper();
    const card = altMapper.mapCard({
      product_id: 'prod-002',
      title: 'Test',
    });
    expect(card.product_id).toBe('prod-002');
  });
});
