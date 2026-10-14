import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { SortOption } from '../../common/enums';
import {
  IsAttributeRecordConstraint,
  SearchProductsQueryDto,
} from './search-products.query';

describe('SearchProductsQueryDto', () => {
  it('SQ-U-04: transforms and validates valid query dto with default values', async () => {
    const plain = {};
    const dto = plainToInstance(SearchProductsQueryDto, plain);
    const errors = await validate(dto);

    expect(errors.length).toBe(0);
    expect(dto.page).toBe(1);
    expect(dto.size).toBe(20);
    expect(dto.include_facets).toBe(true);
  });

  it('SQ-U-04: validates valid sort options', async () => {
    for (const sort of Object.values(SortOption)) {
      const dto = plainToInstance(SearchProductsQueryDto, { sort });
      const errors = await validate(dto);
      expect(errors.length).toBe(0);
      expect(dto.sort).toBe(sort);
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
      const errors = await validate(dto);
      expect(errors.length).toBeGreaterThan(0);
      expect(errors[0].property).toBe('sort');
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
  });

  it('SQ-U-06: rejects attribute keys with illegal characters, dots, or Lucene syntax', async () => {
    const dto1 = plainToInstance(SearchProductsQueryDto, {
      attribute: { 'color.keyword': 'Đen' },
    });
    const errors1 = await validate(dto1);
    expect(errors1.length).toBeGreaterThan(0);
    expect(errors1[0].property).toBe('attribute');

    const dto2 = plainToInstance(SearchProductsQueryDto, {
      attribute: { 'nested.field': 'val' },
    });
    const errors2 = await validate(dto2);
    expect(errors2.length).toBeGreaterThan(0);
  });

  it('SQ-U-07: rejects attribute with prototype pollution keys', async () => {
    const constraint = new IsAttributeRecordConstraint();

    // JSON.parse creates own property __proto__
    const protoObj = JSON.parse('{"__proto__": "polluted"}');
    expect(constraint.validate(protoObj)).toBe(false);

    expect(constraint.validate({ constructor: 'danger' })).toBe(false);
    expect(constraint.validate({ prototype: 'danger' })).toBe(false);

    const pollutedObj = Object.create(null);
    Object.defineProperty(pollutedObj, '__proto__', {
      value: 'evil',
      enumerable: true,
    });
    expect(constraint.validate(pollutedObj)).toBe(false);
  });

  it('SQ-U-08: rejects attribute keys containing script or injection syntax', async () => {
    const constraint = new IsAttributeRecordConstraint();

    expect(constraint.validate({ '<script>alert(1)</script>': 'x' })).toBe(
      false,
    );
    expect(constraint.validate({ $where: 'sleep(1000)' })).toBe(false);
    expect(constraint.validate({ "key' OR 1=1--": 'x' })).toBe(false);
  });

  it('SQ-U-11 & SQ-U-12: validates size boundaries [1, 100]', async () => {
    const valid1 = plainToInstance(SearchProductsQueryDto, { size: '1' });
    expect(await validate(valid1)).toHaveLength(0);
    expect(valid1.size).toBe(1);

    const valid100 = plainToInstance(SearchProductsQueryDto, { size: '100' });
    expect(await validate(valid100)).toHaveLength(0);
    expect(valid100.size).toBe(100);

    const invalid101 = plainToInstance(SearchProductsQueryDto, { size: '101' });
    const errors101 = await validate(invalid101);
    expect(errors101.length).toBeGreaterThan(0);
    expect(errors101[0].property).toBe('size');
  });

  it('SQ-U-13: rejects size=0, negative size, or non-integer size', async () => {
    for (const invalidSize of ['0', '-5', 'twenty', '10.5']) {
      const dto = plainToInstance(SearchProductsQueryDto, {
        size: invalidSize,
      });
      const errors = await validate(dto);
      expect(errors.length).toBeGreaterThan(0);
      expect(errors[0].property).toBe('size');
    }
  });

  it('SQ-U-14: rejects page=0, negative page, or non-integer page', async () => {
    for (const invalidPage of ['0', '-1', 'abc', '1.5']) {
      const dto = plainToInstance(SearchProductsQueryDto, {
        page: invalidPage,
      });
      const errors = await validate(dto);
      expect(errors.length).toBeGreaterThan(0);
      expect(errors[0].property).toBe('page');
    }
  });

  it('SQ-U-15 & SQ-U-16: validates integer non-negative price', async () => {
    for (const invalidPrice of ['10.5', '29.99', '1e5', '-1000']) {
      const dto = plainToInstance(SearchProductsQueryDto, {
        min_price: invalidPrice,
      });
      const errors = await validate(dto);
      expect(errors.length).toBeGreaterThan(0);
      expect(errors[0].property).toBe('min_price');
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
  });

  it('SQ-U-19 & SQ-U-20: validates include_facets boolean transformation', async () => {
    const trueDto = plainToInstance(SearchProductsQueryDto, {
      include_facets: 'true',
    });
    expect(await validate(trueDto)).toHaveLength(0);
    expect(trueDto.include_facets).toBe(true);

    const falseDto = plainToInstance(SearchProductsQueryDto, {
      include_facets: 'false',
    });
    expect(await validate(falseDto)).toHaveLength(0);
    expect(falseDto.include_facets).toBe(false);

    for (const invalidBool of ['yes', '1', 'undefined', 'invalid']) {
      const dto = plainToInstance(SearchProductsQueryDto, {
        include_facets: invalidBool,
      });
      const errors = await validate(dto);
      expect(errors.length).toBeGreaterThan(0);
      expect(errors[0].property).toBe('include_facets');
    }
  });

  it('transforms single string brand or repeated brand into array of strings', async () => {
    const single = plainToInstance(SearchProductsQueryDto, { brand: 'Apple' });
    expect(await validate(single)).toHaveLength(0);
    expect(single.brand).toEqual(['Apple']);

    const multi = plainToInstance(SearchProductsQueryDto, {
      brand: ['Apple', 'Samsung'],
    });
    expect(await validate(multi)).toHaveLength(0);
    expect(multi.brand).toEqual(['Apple', 'Samsung']);
  });
});
