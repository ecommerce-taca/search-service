import { SortOption } from '../../common/enums';
import { QueryParser } from '../query-parser';
import { SearchRequestBuilder } from './search-request.builder';

describe('SearchRequestBuilder', () => {
  it('appends term visibility_status=PUBLISHED in bool.filter (SQ-U-21)', () => {
    const parsed = QueryParser.parse({});
    const req = SearchRequestBuilder.build(parsed);

    const filter =
      (req.query?.bool?.filter as Array<Record<string, unknown>>) || [];
    expect(filter).toContainEqual({
      term: { visibility_status: 'PUBLISHED' },
    });
  });

  it('builds multi_match and title_exact boost when text is present (SQ-U-22)', () => {
    const parsed = QueryParser.parse({ q: 'áo khoác' });
    const req = SearchRequestBuilder.build(parsed);

    const must =
      (req.query?.bool?.must as Array<Record<string, unknown>>) || [];
    const should =
      (req.query?.bool?.should as Array<Record<string, unknown>>) || [];

    expect(must).toContainEqual({
      multi_match: {
        query: 'áo khoác',
        fields: ['title^3', 'brand^2', 'description'],
        operator: 'and',
      },
    });

    expect(should).toContainEqual({
      term: {
        title_exact: {
          value: 'áo khoác',
          boost: 5,
        },
      },
    });
  });

  it('builds sort definitions with deterministic product_id asc tie-break (SQ-U-23, SQ-U-24, SQ-U-25)', () => {
    // price_asc
    const priceAscReq = SearchRequestBuilder.build(
      QueryParser.parse({ sort: SortOption.PRICE_ASC }),
    );
    expect(priceAscReq.sort).toEqual([
      { price: { order: 'asc' } },
      { product_id: { order: 'asc' } },
    ]);

    // relevance
    const relReq = SearchRequestBuilder.build(
      QueryParser.parse({ q: 'iPhone', sort: SortOption.RELEVANCE }),
    );
    expect(relReq.sort).toEqual([
      { _score: { order: 'desc' } },
      { product_id: { order: 'asc' } },
    ]);

    // newest (created_at desc per D6)
    const newestReq = SearchRequestBuilder.build(
      QueryParser.parse({ sort: SortOption.NEWEST }),
    );
    expect(newestReq.sort).toEqual([
      { created_at: { order: 'desc' } },
      { product_id: { order: 'asc' } },
    ]);
  });

  it('omits aggs when include_facets=false (SQ-U-26)', () => {
    const parsed = QueryParser.parse({ include_facets: false });
    const req = SearchRequestBuilder.build(parsed);

    expect(req.aggs).toBeUndefined();
  });

  it('contains fixed price range buckets when include_facets=true (SQ-U-27)', () => {
    const parsed = QueryParser.parse({ include_facets: true });
    const req = SearchRequestBuilder.build(parsed);

    expect(req.aggs).toBeDefined();
    const priceAgg = req.aggs?.price as
      { aggs?: { values?: { range?: { ranges?: unknown } } } } | undefined;
    expect(priceAgg?.aggs?.values?.range?.ranges).toEqual([
      { to: 5000000 },
      { from: 5000000, to: 20000000 },
      { from: 20000000 },
    ]);
  });

  it('applies post_filter and multi-select attribute filters', () => {
    const parsed = QueryParser.parse({
      brand: ['Apple'],
      attribute: { capacity_liter: '256GB' },
    });
    const req = SearchRequestBuilder.build(parsed);

    expect(req.post_filter).toBeDefined();
    const selAgg = req.aggs?.['attr_sel_capacity_liter'] as
      { aggs?: { values?: { terms?: { include?: string } } } } | undefined;
    expect(selAgg).toBeDefined();
    expect(selAgg?.aggs?.values?.terms?.include).toBe('^capacity_liter=.*');

    const restAgg = req.aggs?.['attr_rest'] as
      { aggs?: { values?: { terms?: { exclude?: string } } } } | undefined;
    expect(restAgg).toBeDefined();
    expect(restAgg?.aggs?.values?.terms?.exclude).toBe('^(capacity_liter)=.*');
  });
});
