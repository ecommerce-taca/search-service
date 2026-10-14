import { SortOption } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { SearchProductsQueryDto } from './dto/search-products.query';
import { QueryParser } from './query-parser';

describe('QueryParser', () => {
  it('rejects unknown root parameters (SQ-U-01, SQ-U-02)', () => {
    expect(() =>
      QueryParser.parse({ foo: 'bar' } as Record<string, unknown>),
    ).toThrow(SearchException);
    try {
      QueryParser.parse({ foo: 'bar' } as Record<string, unknown>);
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
      expect(e.details).toEqual(["Unexpected parameter 'foo'"]);
    }

    try {
      QueryParser.parse({ inject: 'true', order_by: 'desc' } as Record<
        string,
        unknown
      >);
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
      expect(e.details).toContain("Unexpected parameter 'inject'");
      expect(e.details).toContain("Unexpected parameter 'order_by'");
    }
  });

  it('validates sort option allowlist and applies D6 default sort (SQ-U-03, SQ-U-04)', () => {
    // Invalid sort options
    for (const invalidSort of ['popular', 'price_descending', 'id_asc']) {
      expect(() =>
        QueryParser.parse({
          sort: invalidSort,
        } as unknown as SearchProductsQueryDto),
      ).toThrow(SearchException);
    }

    // Valid sort options
    for (const validSort of Object.values(SortOption)) {
      const parsed = QueryParser.parse({ sort: validSort });
      expect(parsed.sort).toBe(validSort);
    }

    // D6 default sort: relevance with q, newest without q
    expect(QueryParser.parse({ q: 'iPhone' }).sort).toBe(SortOption.RELEVANCE);
    expect(QueryParser.parse({}).sort).toBe(SortOption.NEWEST);
    expect(QueryParser.parse({ q: '   ' }).sort).toBe(SortOption.NEWEST);
  });

  it('validates attribute keys, prototype pollution, and sanitizes values (SQ-U-05 - SQ-U-08)', () => {
    // Valid attributes
    const parsed = QueryParser.parse({
      attribute: {
        color: 'Đen',
        capacity_liter: ['256GB'],
      },
    });
    expect(parsed.filters.attributes).toEqual({
      color: ['Đen'],
      capacity_liter: ['256GB'],
    });

    // Invalid attribute keys
    expect(() =>
      QueryParser.parse({
        attribute: { 'color.keyword': 'Đen' },
      }),
    ).toThrow(SearchException);

    expect(() =>
      QueryParser.parse({
        attribute: { 'nested.field': 'val' },
      }),
    ).toThrow(SearchException);

    // Prototype pollution
    expect(() =>
      QueryParser.parse({
        attribute: { ['__proto__']: 'polluted' },
      }),
    ).toThrow(SearchException);

    expect(() =>
      QueryParser.parse({
        attribute: { constructor: 'danger' },
      }),
    ).toThrow(SearchException);

    expect(() =>
      QueryParser.parse({
        attribute: { prototype: 'danger' },
      }),
    ).toThrow(SearchException);

    // Script injection
    expect(() =>
      QueryParser.parse({
        attribute: { '<script>alert(1)</script>': 'x' },
      }),
    ).toThrow(SearchException);

    expect(() =>
      QueryParser.parse({
        attribute: { $where: 'x' },
      }),
    ).toThrow(SearchException);
  });

  it('enforces Unicode character bounds on q (SQ-U-09, SQ-U-10)', () => {
    const q200 = 'á'.repeat(200);
    const parsed = QueryParser.parse({ q: q200 });
    expect(parsed.text).toBe(q200);

    const q201 = 'á'.repeat(201);
    expect(() => QueryParser.parse({ q: q201 })).toThrow(SearchException);
    try {
      QueryParser.parse({ q: q201 });
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_QUERY_TOO_LONG);
    }
  });

  it('enforces page and size bounds (SQ-U-11 - SQ-U-14)', () => {
    expect(() => QueryParser.parse({ size: 101 })).toThrow(SearchException);
    expect(() => QueryParser.parse({ size: 0 })).toThrow(SearchException);
    expect(() => QueryParser.parse({ size: -5 })).toThrow(SearchException);
    expect(() =>
      QueryParser.parse({ size: 10.5 } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);

    expect(QueryParser.parse({ size: 100 }).size).toBe(100);
    expect(QueryParser.parse({ size: 1 }).size).toBe(1);

    expect(() => QueryParser.parse({ page: 0 })).toThrow(SearchException);
    expect(() => QueryParser.parse({ page: -1 })).toThrow(SearchException);
    expect(() =>
      QueryParser.parse({ page: 1.5 } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);
    expect(QueryParser.parse({ page: 5 }).page).toBe(5);

    // Max window
    expect(() => QueryParser.parse({ page: 101, size: 100 })).toThrow(
      SearchException,
    );
  });

  it('validates price ranges and consistency (SQ-U-15 - SQ-U-18)', () => {
    expect(() =>
      QueryParser.parse({
        min_price: 10.5,
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);
    expect(() => QueryParser.parse({ min_price: -1000 })).toThrow(
      SearchException,
    );
    expect(() => QueryParser.parse({ max_price: -50000 })).toThrow(
      SearchException,
    );

    // min > max
    expect(() =>
      QueryParser.parse({ min_price: 10000000, max_price: 5000000 }),
    ).toThrow(SearchException);
    try {
      QueryParser.parse({ min_price: 10000000, max_price: 5000000 });
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
      expect(e.details).toContain('min_price cannot exceed max_price');
    }

    const validRange = QueryParser.parse({
      min_price: 0,
      max_price: 5000000,
    });
    expect(validRange.filters.minPrice).toBe(0);
    expect(validRange.filters.maxPrice).toBe(5000000);
  });

  it('handles include_facets flag (SQ-U-19, SQ-U-20)', () => {
    expect(() =>
      QueryParser.parse({
        include_facets: 'yes',
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);
    expect(() =>
      QueryParser.parse({
        include_facets: '1',
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);

    expect(QueryParser.parse({ include_facets: false }).includeFacets).toBe(
      false,
    );
    expect(
      QueryParser.parse({
        include_facets: 'false',
      } as unknown as SearchProductsQueryDto).includeFacets,
    ).toBe(false);
    expect(QueryParser.parse({ include_facets: true }).includeFacets).toBe(
      true,
    );
    expect(QueryParser.parse({}).includeFacets).toBe(true);
  });
});
