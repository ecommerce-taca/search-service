import { SortOption } from '../../common/enums';
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { ParsedSearchQuery } from '../query-parser';
import { SearchRequestBuilder } from './search-request.builder';

function getTermValue(clause: unknown, field: string): unknown {
  const c = clause as { term?: Record<string, unknown> } | undefined;
  return c?.term?.[field];
}

function getTermsValues(clause: unknown, field: string): unknown[] | undefined {
  const c = clause as { terms?: Record<string, unknown[]> } | undefined;
  return c?.terms?.[field];
}

function getRangeField(
  clause: unknown,
  field: string,
): { gte?: number; lte?: number } | undefined {
  const c = clause as
    { range?: Record<string, { gte?: number; lte?: number }> } | undefined;
  return c?.range?.[field];
}

describe('SearchRequestBuilder', () => {
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

    const filters = (
      Array.isArray(boolQuery?.filter) ? boolQuery?.filter : [boolQuery?.filter]
    ) as unknown[];

    const hasPublishedFilter = filters.some((f) => {
      return getTermValue(f, 'visibility_status') === 'PUBLISHED';
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

    const shouldClause = (
      boolQuery?.should as Array<Record<string, unknown>>
    )[0];
    const termExact = shouldClause.term as {
      title_exact: { value: string; boost: number };
    };
    expect(termExact.title_exact.value).toBe('áo khoác');
    expect(termExact.title_exact.boost).toBe(5);

    // Visibility filter is preserved
    const filters = (
      Array.isArray(boolQuery?.filter) ? boolQuery?.filter : [boolQuery?.filter]
    ) as unknown[];
    expect(
      filters.some((f) => getTermValue(f, 'visibility_status') === 'PUBLISHED'),
    ).toBe(true);
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

  it('SQ-U-26: include_facets=false includes index_as_of agg but omits facet aggs', () => {
    const req = builder.build({
      ...baseParsed,
      includeFacets: false,
    });

    expect(req.aggs).toBeDefined();
    expect(req.aggs?.index_as_of).toEqual({
      max: { field: 'indexed_at' },
    });
    expect(req.aggs?.category).toBeUndefined();
    expect(req.aggs?.brand).toBeUndefined();
    expect(req.aggs?.price).toBeUndefined();
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

  it('builds post_filter containing all selected facet filters (category, brand, price, attributes)', () => {
    const req = builder.build({
      ...baseParsed,
      filters: {
        categoryId: 'cat-phone',
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
    const postBool = (req.post_filter as { bool?: { filter?: unknown[] } })
      ?.bool;
    expect(postBool?.filter).toBeDefined();
    const filters = (postBool?.filter || []) as unknown[];

    expect(
      filters.some((f) => getTermValue(f, 'category_ids') === 'cat-phone'),
    ).toBe(true);
    expect(
      filters.some((f) =>
        (getTermsValues(f, 'brand') as string[] | undefined)?.includes('Apple'),
      ),
    ).toBe(true);
    expect(
      filters.some((f) => getRangeField(f, 'price')?.gte === 5000000),
    ).toBe(true);
    expect(
      filters.some((f) => getTermValue(f, 'attribute_pairs') === 'color=Đen'),
    ).toBe(true);
    expect(
      filters.some((f) =>
        (
          getTermsValues(f, 'attribute_pairs') as string[] | undefined
        )?.includes('capacity_liter=256GB'),
      ),
    ).toBe(true);
  });

  it('places shop_id in main query bool.filter, NOT in post_filter or facet aggs', () => {
    const req = builder.build({
      ...baseParsed,
      filters: {
        ...baseParsed.filters,
        shopId: 'shop-001',
      },
    });

    const boolFilters = (
      Array.isArray(req.query?.bool?.filter)
        ? req.query?.bool?.filter
        : [req.query?.bool?.filter]
    ) as unknown[];
    expect(
      boolFilters.some((f) => getTermValue(f, 'shop_id') === 'shop-001'),
    ).toBe(true);
    expect(req.post_filter).toBeUndefined();
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
    const catAggFilter = ((
      aggs.category as { filter?: { bool?: { filter?: unknown[] } } }
    ).filter?.bool?.filter || []) as unknown[];
    expect(catAggFilter.some((f) => getTermValue(f, 'category_ids'))).toBe(
      false,
    );
    expect(catAggFilter.some((f) => getTermValue(f, 'brand') === 'Apple')).toBe(
      true,
    );

    // Brand facet should NOT contain brand filter
    const brandAggFilter = ((
      aggs.brand as { filter?: { bool?: { filter?: unknown[] } } }
    ).filter?.bool?.filter || []) as unknown[];
    expect(
      brandAggFilter.some(
        (f) => getTermValue(f, 'brand') || getTermsValues(f, 'brand'),
      ),
    ).toBe(false);
    expect(
      brandAggFilter.some(
        (f) => getTermValue(f, 'category_ids') === 'cat-phone',
      ),
    ).toBe(true);

    // Price facet should NOT contain price filter
    const priceAggFilter = ((
      aggs.price as { filter?: { bool?: { filter?: unknown[] } } }
    ).filter?.bool?.filter || []) as unknown[];
    expect(priceAggFilter.some((f) => getRangeField(f, 'price'))).toBe(false);

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

  it('restricts _source to product card fields and does not include shop_id or shop_name', () => {
    const req = builder.build(baseParsed);
    expect(req._source).toEqual([
      'product_id',
      'title',
      'slug',
      'shop',
      'price',
      'currency',
      'cover_url',
      'rating_avg',
    ]);
    expect((req._source as string[]).includes('shop_id')).toBe(false);
    expect((req._source as string[]).includes('shop_name')).toBe(false);
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

  it('builds request via instance method build()', () => {
    const req = builder.build(baseParsed);
    expect(req).toBeDefined();
  });
});
