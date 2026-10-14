import { plainToInstance } from 'class-transformer';
import { SortOption } from '../common/enums';
import { ERROR_MESSAGES_MAP, SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { SearchProductsQueryDto } from './dto/search-products.query';
import { QueryParser } from './query-parser';

describe('QueryParser', () => {
  let parser: QueryParser;

  beforeEach(() => {
    parser = new QueryParser();
  });

  it('SQ-U-01: throws SEARCH_INVALID_INPUT when receiving unknown parameter foo=bar', () => {
    expect(() =>
      parser.parse({ foo: 'bar' } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);

    try {
      parser.parse({ foo: 'bar' } as unknown as SearchProductsQueryDto);
      fail('Expected SearchException');
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
      expect(e.message).toBe(ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT]);
      expect(e.getStatus()).toBe(400);
      expect(e.details).toEqual(["Unexpected parameter 'foo'"]);
    }
  });

  it('SQ-U-02: throws SEARCH_INVALID_INPUT rejecting multiple disallowed parameters', () => {
    try {
      parser.parse({
        inject: 'true',
        order_by: 'desc',
      } as unknown as SearchProductsQueryDto);
      fail('Expected SearchException');
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
      expect(e.getStatus()).toBe(400);
      expect(e.details).toContain("Unexpected parameter 'inject'");
      expect(e.details).toContain("Unexpected parameter 'order_by'");
    }
  });

  it('SQ-U-03: throws SEARCH_INVALID_INPUT with detail for unsupported sort option', () => {
    for (const invalidSort of ['popular', 'price_descending', 'id_asc']) {
      try {
        parser.parse({ sort: invalidSort } as unknown as SearchProductsQueryDto);
        fail(`Expected SearchException for sort=${invalidSort}`);
      } catch (err) {
        const e = err as SearchException;
        expect(e.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
        expect(e.getStatus()).toBe(400);
        expect(e.details).toContain(`Unsupported sort option '${invalidSort}'`);
      }
    }
  });

  it('SQ-U-04: passes validation cleanly for each valid sort option and applies D6 defaults', () => {
    for (const validSort of Object.values(SortOption)) {
      const parsed = parser.parse({ sort: validSort } as unknown as SearchProductsQueryDto);
      expect(parsed.sort).toBe(validSort);
    }

    // D6 default sort: relevance when q present, newest when q absent
    const withQ = parser.parse({ q: 'iPhone' } as unknown as SearchProductsQueryDto);
    expect(withQ.sort).toBe(SortOption.RELEVANCE);

    const withoutQ = parser.parse({} as unknown as SearchProductsQueryDto);
    expect(withoutQ.sort).toBe(SortOption.NEWEST);

    const withWhitespaceQ = parser.parse({ q: '   ' } as unknown as SearchProductsQueryDto);
    expect(withWhitespaceQ.sort).toBe(SortOption.NEWEST);
  });

  it('SQ-U-05: parses valid attribute filters into normalized record', () => {
    const parsed = parser.parse({
      attribute: {
        color: 'Đen',
        capacity_liter: ['256GB'],
      },
    } as unknown as SearchProductsQueryDto);

    expect(parsed.filters.attributes).toEqual({
      color: ['Đen'],
      capacity_liter: ['256GB'],
    });
  });

  it('SQ-U-06: rejects attribute keys containing dot, path traversal, or Lucene syntax', () => {
    expect(() =>
      parser.parse({
        attribute: { 'color.keyword': 'Đen' },
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);

    expect(() =>
      parser.parse({
        attribute: { 'nested.field': 'val' },
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);

    try {
      parser.parse({
        attribute: { 'color.keyword': 'Đen' },
      } as unknown as SearchProductsQueryDto);
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
      expect(e.getStatus()).toBe(400);
    }
  });

  it('SQ-U-07: rejects attribute key with prototype pollution keys', () => {
    const protoObj = JSON.parse('{"__proto__": "polluted"}');
    const dangerousCases: Array<Record<string, unknown>> = [
      protoObj,
      { constructor: 'danger' },
      { prototype: 'x' },
    ];

    for (const badAttr of dangerousCases) {
      expect(() =>
        parser.parse({
          attribute: badAttr,
        } as unknown as SearchProductsQueryDto),
      ).toThrow(SearchException);
    }
  });

  it('SQ-U-08: rejects attribute key with script tags or injection', () => {
    expect(() =>
      parser.parse({
        attribute: { '<script>alert(1)</script>': 'x' },
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);

    expect(() =>
      parser.parse({
        attribute: { $where: 'x' },
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);
  });

  it('SQ-U-09: throws SEARCH_QUERY_TOO_LONG when q contains >200 Unicode code points', () => {
    const q201 = 'a'.repeat(201);
    const q201Vi = 'á'.repeat(201);

    for (const q of [q201, q201Vi]) {
      try {
        parser.parse({ q } as unknown as SearchProductsQueryDto);
        fail('Expected SEARCH_QUERY_TOO_LONG');
      } catch (err) {
        const e = err as SearchException;
        expect(e.code).toBe(SearchErrorCode.SEARCH_QUERY_TOO_LONG);
        expect(e.message).toBe(ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_QUERY_TOO_LONG]);
        expect(e.getStatus()).toBe(400);
      }
    }
  });

  it('SQ-U-10: passes validation cleanly when q contains exactly 200 Unicode characters', () => {
    const q200 = 'á'.repeat(200);
    const parsed = parser.parse({ q: q200 } as unknown as SearchProductsQueryDto);
    expect(parsed.text).toBe(q200);
    expect(Array.from(parsed.text || '').length).toBe(200);
  });

  it('SQ-U-11: throws SEARCH_INVALID_INPUT when size exceeds 100', () => {
    try {
      parser.parse({ size: 101 } as unknown as SearchProductsQueryDto);
      fail('Expected SearchException');
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
      expect(e.getStatus()).toBe(400);
    }
  });

  it('SQ-U-12: passes validation cleanly with size boundaries 1 and 100', () => {
    expect(parser.parse({ size: 100 } as unknown as SearchProductsQueryDto).size).toBe(100);
    expect(parser.parse({ size: 1 } as unknown as SearchProductsQueryDto).size).toBe(1);
  });

  it('SQ-U-13: throws SEARCH_INVALID_INPUT for size=0, size=-5, or non-integer size', () => {
    for (const invalidSize of [0, -5, 'twenty', 10.5]) {
      expect(() =>
        parser.parse({ size: invalidSize } as unknown as SearchProductsQueryDto),
      ).toThrow(SearchException);
    }
  });

  it('SQ-U-14: throws SEARCH_INVALID_INPUT for page=0, page=-1, page=abc, or page=1.5', () => {
    for (const invalidPage of [0, -1, 'abc', 1.5]) {
      expect(() =>
        parser.parse({ page: invalidPage } as unknown as SearchProductsQueryDto),
      ).toThrow(SearchException);
    }
  });

  it('SQ-U-15: throws SEARCH_INVALID_INPUT for floating point or scientific notation price', () => {
    for (const invalidPrice of [10.5, '29.99', NaN]) {
      expect(() =>
        parser.parse({ min_price: invalidPrice } as unknown as SearchProductsQueryDto),
      ).toThrow(SearchException);
      expect(() =>
        parser.parse({ max_price: invalidPrice } as unknown as SearchProductsQueryDto),
      ).toThrow(SearchException);
    }

    const scientificDto = plainToInstance(SearchProductsQueryDto, { min_price: '1e5' });
    expect(() => parser.parse(scientificDto)).toThrow(SearchException);
  });

  it('SQ-U-16: throws SEARCH_INVALID_INPUT for negative price', () => {
    expect(() =>
      parser.parse({ min_price: -1000 } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);
    expect(() =>
      parser.parse({ max_price: -50000 } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);
  });

  it('SQ-U-17: throws SEARCH_INVALID_INPUT when min_price > max_price', () => {
    try {
      parser.parse({
        min_price: 10000000,
        max_price: 5000000,
      } as unknown as SearchProductsQueryDto);
      fail('Expected SearchException');
    } catch (err) {
      const e = err as SearchException;
      expect(e.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
      expect(e.details).toContain('min_price cannot exceed max_price');
    }
  });

  it('SQ-U-18: passes validation cleanly for valid price boundaries', () => {
    const range1 = parser.parse({
      min_price: 0,
      max_price: 5000000,
    } as unknown as SearchProductsQueryDto);
    expect(range1.filters.minPrice).toBe(0);
    expect(range1.filters.maxPrice).toBe(5000000);

    const range2 = parser.parse({
      min_price: 5000000,
      max_price: 5000000,
    } as unknown as SearchProductsQueryDto);
    expect(range2.filters.minPrice).toBe(5000000);
    expect(range2.filters.maxPrice).toBe(5000000);
  });

  it('SQ-U-19: throws SEARCH_INVALID_INPUT for non-boolean include_facets', () => {
    for (const invalidValue of ['yes', '1', 'undefined', 'random']) {
      expect(() =>
        parser.parse({
          include_facets: invalidValue,
        } as unknown as SearchProductsQueryDto),
      ).toThrow(SearchException);
    }
  });

  it('SQ-U-20: correctly parses include_facets boolean values and defaults to true', () => {
    expect(
      parser.parse({ include_facets: false } as unknown as SearchProductsQueryDto)
        .includeFacets,
    ).toBe(false);
    expect(
      parser.parse({
        include_facets: 'false',
      } as unknown as SearchProductsQueryDto).includeFacets,
    ).toBe(false);

    expect(
      parser.parse({ include_facets: true } as unknown as SearchProductsQueryDto)
        .includeFacets,
    ).toBe(true);
    expect(
      parser.parse({
        include_facets: 'true',
      } as unknown as SearchProductsQueryDto).includeFacets,
    ).toBe(true);

    expect(parser.parse({} as unknown as SearchProductsQueryDto).includeFacets).toBe(true);
  });

  it('rejects page * size > 10000 (ES max_result_window)', () => {
    expect(() =>
      parser.parse({ page: 101, size: 100 } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);
    expect(() =>
      parser.parse({ page: 200, size: 60 } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);
  });

  it('enforces attribute key limits and value bounds', () => {
    // > 10 keys
    const elevenKeys: Record<string, string> = {};
    for (let i = 0; i < 11; i++) {
      elevenKeys[`attr_${i}`] = 'val';
    }
    expect(() =>
      parser.parse({ attribute: elevenKeys } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);

    // value length > 100
    expect(() =>
      parser.parse({
        attribute: { color: 'a'.repeat(101) },
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);

    // values count > 20
    const twentyOneVals = Array.from({ length: 21 }, (_, i) => `v${i}`);
    expect(() =>
      parser.parse({
        attribute: { color: twentyOneVals },
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);
  });
});
