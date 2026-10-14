import { SortOption } from '../../common/enums';
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { ParsedSearchQuery } from '../query-parser';
import { ElasticsearchQueryBuilder, SearchRequestBuilder } from './search-request.builder';

describe('SearchRequestBuilder / ElasticsearchQueryBuilder', () => {
  let builder: SearchRequestBuilder;

  beforeEach(() => {
    builder = new SearchRequestBuilder();
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

  it('SQ-U-21: always appends visibility_status=PUBLISHED inside top-level bool.filter', () => {
    const req = builder.build(baseParsed);

    const boolQuery = req.query?.bool;
    expect(boolQuery).toBeDefined();
    expect(boolQuery?.filter).toBeDefined();

    const filters = (Array.isArray(boolQuery?.filter)
      ? boolQuery?.filter
      : [boolQuery?.filter]) as any[];

    const hasPublishedFilter = filters.some((f) => {
      return (f.term as any)?.visibility_status === 'PUBLISHED';
    });

    expect(hasPublishedFilter).toBe(true);
  });

  it('SQ-U-22: constructs multi_match on title^3, brand^2, description and title_exact^5 when text is present', () => {
    const req = builder.build({
      ...baseParsed,
      text: 'áo khoác',
    });

    const boolQuery = req.query?.bool;
    expect(boolQuery?.must).toBeDefined();

    const mustClause = (boolQuery?.must as Array<Record<string, unknown>>)[0];
    const multiMatch = mustClause.multi_match as {
      query: string;
      fields: string[];
      operator: string;
    };

    expect(multiMatch.query).toBe('áo khoác');
    expect(multiMatch.fields).toEqual(['title^3', 'brand^2', 'description']);
    expect(multiMatch.operator).toBe('and');

    const shouldClause = (boolQuery?.should as Array<Record<string, unknown>>)[0];
    const termExact = shouldClause.term as {
      title_exact: { value: string; boost: number };
    };
    expect(termExact.title_exact.value).toBe('áo khoác');
    expect(termExact.title_exact.boost).toBe(5);

    // Visibility filter is preserved
    const filters = boolQuery?.filter as Array<Record<string, any>>;
    expect(filters.some((f) => (f.term as any)?.visibility_status === 'PUBLISHED')).toBe(
      true,
    );
  });

  it('SQ-U-23: sort="price_asc" produces price asc and product_id asc tie-break', () => {
    const req = builder.build({
      ...baseParsed,
      sort: SortOption.PRICE_ASC,
    });

    expect(req.sort).toEqual([
      { price: { order: 'asc' } },
      { product_id: { order: 'asc' } },
    ]);
  });

  it('SQ-U-24: sort="relevance" produces _score desc and product_id asc tie-break', () => {
    const req = builder.build({
      ...baseParsed,
      text: 'iPhone',
      sort: SortOption.RELEVANCE,
    });

    expect(req.sort).toEqual([
      { _score: { order: 'desc' } },
      { product_id: { order: 'asc' } },
    ]);
  });

  it('SQ-U-25: sort="newest" produces created_at desc and product_id asc tie-break per D6', () => {
    const req = builder.build({
      ...baseParsed,
      sort: SortOption.NEWEST,
    });

    expect(req.sort).toEqual([
      { created_at: { order: 'desc' } },
      { product_id: { order: 'asc' } },
    ]);
  });

  it('handles sort="price_desc" and sort="rating_desc" with deterministic tie-break', () => {
    const priceDescReq = builder.build({
      ...baseParsed,
      sort: SortOption.PRICE_DESC,
    });
    expect(priceDescReq.sort).toEqual([
      { price: { order: 'desc' } },
      { product_id: { order: 'asc' } },
    ]);

    const ratingDescReq = builder.build({
      ...baseParsed,
      sort: SortOption.RATING_DESC,
    });
    expect(ratingDescReq.sort).toEqual([
      { rating_avg: { order: 'desc' } },
      { rating_count: { order: 'desc' } },
      { product_id: { order: 'asc' } },
    ]);
  });

  it('SQ-U-26: include_facets=false omits aggs property from ES search request', () => {
    const req = builder.build({
      ...baseParsed,
      includeFacets: false,
    });

    expect(req.aggs).toBeUndefined();
  });

  it('SQ-U-27: include_facets=true creates fixed price range aggregation with D9 buckets', () => {
    const req = builder.build({
      ...baseParsed,
      includeFacets: true,
    });

    expect(req.aggs).toBeDefined();
    const priceAgg = req.aggs?.price as {
      filter?: Record<string, unknown>;
      aggs?: {
        values?: {
          range?: {
            field: string;
            ranges: Array<{ from?: number; to?: number }>;
          };
        };
      };
    };

    expect(priceAgg).toBeDefined();
    expect(priceAgg.aggs?.values?.range?.field).toBe('price');
    expect(priceAgg.aggs?.values?.range?.ranges).toEqual([
      { to: 5000000 },
      { from: 5000000, to: 20000000 },
      { from: 20000000 },
    ]);
  });

  it('builds post_filter containing all selected filters (category, brand, price, attributes)', () => {
    const req = builder.build({
      ...baseParsed,
      filters: {
        categoryId: 'cat-phone',
        shopId: 'shop-001',
        brands: ['Apple', 'Samsung'],
        minPrice: 5000000,
        maxPrice: 20000000,
        attributes: {
          color: ['Đen'],
          capacity_liter: ['256GB', '512GB'],
        },
      },
    });

    expect(req.post_filter).toBeDefined();
    const postBool = (req.post_filter as { bool?: { filter?: unknown[] } })?.bool;
    expect(postBool?.filter).toBeDefined();
    const filters = postBool?.filter as any[];

    expect(filters.some((f) => (f.term as any)?.category_ids === 'cat-phone')).toBe(true);
    expect(filters.some((f) => (f.term as any)?.shop_id === 'shop-001')).toBe(true);
    expect(filters.some((f) => (f.terms as any)?.brand?.includes('Apple'))).toBe(true);
    expect(filters.some((f) => (f.range as any)?.price?.gte === 5000000)).toBe(true);
    expect(filters.some((f) => (f.term as any)?.attribute_pairs === 'color=Đen')).toBe(true);
    expect(
      filters.some((f) =>
        (f.terms as any)?.attribute_pairs?.includes('capacity_liter=256GB'),
      ),
    ).toBe(true);
  });

  it('builds multi-select facet aggregations excluding own filter per D10', () => {
    const req = builder.build({
      ...baseParsed,
      filters: {
        categoryId: 'cat-phone',
        brands: ['Apple'],
        minPrice: 10000000,
        attributes: {
          capacity_liter: ['256GB'],
        },
      },
      includeFacets: true,
    });

    const aggs = req.aggs!;

    // Category facet should NOT contain category filter
    const catAggFilter = (aggs.category as { filter?: { bool?: { filter?: any[] } } })
      .filter?.bool?.filter as any[];
    expect(catAggFilter.some((f) => (f.term as any)?.category_ids)).toBe(false);
    expect(catAggFilter.some((f) => (f.term as any)?.brand === 'Apple')).toBe(true);

    // Brand facet should NOT contain brand filter
    const brandAggFilter = (aggs.brand as { filter?: { bool?: { filter?: any[] } } })
      .filter?.bool?.filter as any[];
    expect(
      brandAggFilter.some((f) => (f.term as any)?.brand || (f.terms as any)?.brand),
    ).toBe(false);
    expect(brandAggFilter.some((f) => (f.term as any)?.category_ids === 'cat-phone')).toBe(
      true,
    );

    // Price facet should NOT contain price filter
    const priceAggFilter = (aggs.price as { filter?: { bool?: { filter?: any[] } } })
      .filter?.bool?.filter as any[];
    expect(priceAggFilter.some((f) => (f.range as any)?.price)).toBe(false);

    // Attribute facets (D23): selected key capacity_liter
    expect(aggs['attr_sel_capacity_liter']).toBeDefined();
    expect(aggs['attr_rest']).toBeDefined();
  });

  it('adds index_as_of max aggregation on indexed_at field', () => {
    const req = builder.build({
      ...baseParsed,
      includeFacets: true,
    });

    const maxAgg = req.aggs?.index_as_of as { max?: { field: string } };
    expect(maxAgg).toBeDefined();
    expect(maxAgg.max?.field).toBe('indexed_at');
  });

  it('restricts _source to product card fields', () => {
    const req = builder.build(baseParsed);
    expect(req._source).toEqual(
      expect.arrayContaining([
        'product_id',
        'title',
        'slug',
        'shop',
        'price',
        'currency',
        'cover_url',
        'rating_avg',
      ]),
    );
  });

  it('rejects page * size > 10000 with SEARCH_INVALID_INPUT', () => {
    expect(() =>
      builder.build({
        ...baseParsed,
        page: 101,
        size: 100,
      }),
    ).toThrow(SearchException);

    try {
      builder.build({
        ...baseParsed,
        page: 501,
        size: 20,
      });
      fail('Expected SearchException');
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
    }
  });

  it('is accessible via alias ElasticsearchQueryBuilder', () => {
    const esBuilder = new ElasticsearchQueryBuilder();
    const req = esBuilder.build(baseParsed);
    expect(req).toBeDefined();
  });
});
