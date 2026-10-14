import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { SortOption } from '../../common/enums';
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { QueryParser } from '../query-parser';
import { SearchProductsQueryDto } from './search-products.query';

describe('SearchProductsQueryDto', () => {
  let parser: QueryParser;

  beforeEach(() => {
    parser = new QueryParser();
  });

  it('SQ-U-04: transforms and validates valid query dto with default values', async () => {
    const plain = {};
    const dto = plainToInstance(SearchProductsQueryDto, plain);
    const errors = await validate(dto);

    expect(errors.length).toBe(0);
    expect(dto.page).toBe(1);
    expect(dto.size).toBe(20);
    expect(dto.include_facets).toBe(true);

    const parsed = parser.parse(dto);
    expect(parsed.page).toBe(1);
    expect(parsed.size).toBe(20);
    expect(parsed.includeFacets).toBe(true);
  });

  it('SQ-U-04: validates valid sort options', async () => {
    for (const sort of Object.values(SortOption)) {
      const dto = plainToInstance(SearchProductsQueryDto, { sort });
      const errors = await validate(dto);
      expect(errors.length).toBe(0);
      expect(dto.sort).toBe(sort);
      const parsed = parser.parse(dto);
      expect(parsed.sort).toBe(sort);
    }
  });

  it('SQ-U-03: rejects invalid sort options', async () => {
    for (const invalidSort of [
      'popular',
      'price_descending',
      'id_asc',
      'unknown',
    ]) {
      const dto = plainToInstance(SearchProductsQueryDto, {
        sort: invalidSort,
      });
      expect(() => parser.parse(dto)).toThrow(SearchException);
      try {
        parser.parse(dto);
      } catch (err) {
        expect((err as SearchException).code).toBe(
          SearchErrorCode.SEARCH_INVALID_INPUT,
        );
      }
    }
  });

  it('SQ-U-05: parses and validates valid attribute record', async () => {
    const dto = plainToInstance(SearchProductsQueryDto, {
      attribute: {
        color: 'Đen',
        capacity_liter: ['256GB', '512GB'],
      },
    });
    const errors = await validate(dto);
    expect(errors.length).toBe(0);
    const parsed = parser.parse(dto);
    expect(parsed.filters.attributes).toEqual({
      color: ['Đen'],
      capacity_liter: ['256GB', '512GB'],
    });
  });

  it('SQ-U-06: rejects attribute keys with illegal characters, dots, or Lucene syntax', async () => {
    const dto1 = plainToInstance(SearchProductsQueryDto, {
      attribute: { 'color.keyword': 'Đen' },
    });
    expect(() => parser.parse(dto1)).toThrow(SearchException);

    const dto2 = plainToInstance(SearchProductsQueryDto, {
      attribute: { 'nested.field': 'val' },
    });
    expect(() => parser.parse(dto2)).toThrow(SearchException);
  });

  it('SQ-U-07: rejects attribute with prototype pollution keys', async () => {
    const protoObj = JSON.parse('{"__proto__": "polluted"}');
    expect(() =>
      parser.parse({
        attribute: protoObj,
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);

    expect(() =>
      parser.parse({
        attribute: { constructor: 'danger' },
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);

    expect(() =>
      parser.parse({
        attribute: { prototype: 'danger' },
      } as unknown as SearchProductsQueryDto),
    ).toThrow(SearchException);
  });

  it('SQ-U-08: rejects attribute keys containing script or injection syntax', async () => {
    const dto1 = plainToInstance(SearchProductsQueryDto, {
      attribute: { '<script>alert(1)</script>': 'x' },
    });
    expect(() => parser.parse(dto1)).toThrow(SearchException);

    const dto2 = plainToInstance(SearchProductsQueryDto, {
      attribute: { $where: 'sleep(1000)' },
    });
    expect(() => parser.parse(dto2)).toThrow(SearchException);

    const dto3 = plainToInstance(SearchProductsQueryDto, {
      attribute: { "key' OR 1=1--": 'x' },
    });
    expect(() => parser.parse(dto3)).toThrow(SearchException);
  });

  it('SQ-U-11 & SQ-U-12: validates size boundaries [1, 100]', async () => {
    const valid1 = plainToInstance(SearchProductsQueryDto, { size: '1' });
    expect(await validate(valid1)).toHaveLength(0);
    expect(valid1.size).toBe(1);
    expect(parser.parse(valid1).size).toBe(1);

    const valid100 = plainToInstance(SearchProductsQueryDto, { size: '100' });
    expect(await validate(valid100)).toHaveLength(0);
    expect(valid100.size).toBe(100);
    expect(parser.parse(valid100).size).toBe(100);

    const invalid101 = plainToInstance(SearchProductsQueryDto, { size: '101' });
    expect(() => parser.parse(invalid101)).toThrow(SearchException);
  });

  it('SQ-U-13: rejects size=0, negative size, or non-integer size', async () => {
    for (const invalidSize of ['0', '-5', 'twenty', '10.5']) {
      const dto = plainToInstance(SearchProductsQueryDto, {
        size: invalidSize,
      });
      expect(() => parser.parse(dto)).toThrow(SearchException);
    }
  });

  it('SQ-U-14: rejects page=0, negative page, or non-integer page', async () => {
    for (const invalidPage of ['0', '-1', 'abc', '1.5']) {
      const dto = plainToInstance(SearchProductsQueryDto, {
        page: invalidPage,
      });
      expect(() => parser.parse(dto)).toThrow(SearchException);
    }
  });

  it('SQ-U-15 & SQ-U-16: validates integer non-negative price', async () => {
    for (const invalidPrice of ['10.5', '29.99', '1e5', '-1000']) {
      const dto = plainToInstance(SearchProductsQueryDto, {
        min_price: invalidPrice,
      });
      expect(() => parser.parse(dto)).toThrow(SearchException);
    }
  });

  it('SQ-U-18: accepts valid price ranges and transforms strings', async () => {
    const dto = plainToInstance(SearchProductsQueryDto, {
      min_price: '5000000',
      max_price: '20000000',
    });
    const errors = await validate(dto);
    expect(errors.length).toBe(0);
    expect(dto.min_price).toBe(5000000);
    expect(dto.max_price).toBe(20000000);
    const parsed = parser.parse(dto);
    expect(parsed.filters.minPrice).toBe(5000000);
    expect(parsed.filters.maxPrice).toBe(20000000);
  });

  it('SQ-U-19 & SQ-U-20: validates include_facets boolean transformation', async () => {
    const trueDto = plainToInstance(SearchProductsQueryDto, {
      include_facets: 'true',
    });
    expect(await validate(trueDto)).toHaveLength(0);
    expect(trueDto.include_facets).toBe(true);
    expect(parser.parse(trueDto).includeFacets).toBe(true);

    const falseDto = plainToInstance(SearchProductsQueryDto, {
      include_facets: 'false',
    });
    expect(await validate(falseDto)).toHaveLength(0);
    expect(falseDto.include_facets).toBe(false);
    expect(parser.parse(falseDto).includeFacets).toBe(false);

    for (const invalidBool of ['yes', '1', 'invalid']) {
      const dto = plainToInstance(SearchProductsQueryDto, {
        include_facets: invalidBool,
      });
      expect(() => parser.parse(dto)).toThrow(SearchException);
    }
  });

  it('transforms single string brand or repeated brand into array of strings', async () => {
    const single = plainToInstance(SearchProductsQueryDto, { brand: 'Apple' });
    expect(await validate(single)).toHaveLength(0);
    expect(single.brand).toEqual(['Apple']);
    expect(parser.parse(single).filters.brands).toEqual(['Apple']);

    const multi = plainToInstance(SearchProductsQueryDto, {
      brand: ['Apple', 'Samsung'],
    });
    expect(await validate(multi)).toHaveLength(0);
    expect(multi.brand).toEqual(['Apple', 'Samsung']);
    expect(parser.parse(multi).filters.brands).toEqual(['Apple', 'Samsung']);
  });
});
