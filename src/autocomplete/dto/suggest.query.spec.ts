import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { ValidationPipe } from '@nestjs/common';
import { SuggestQueryDto } from './suggest.query';

describe('SuggestQueryDto (Unit)', () => {
  let pipe: ValidationPipe;

  beforeEach(() => {
    pipe = new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
    });
  });

  it('AC-U-01: fails validation when prefix is missing', async () => {
    const raw = {};
    const dto = plainToInstance(SuggestQueryDto, raw);
    const errors = await validate(dto);

    expect(errors.length).toBeGreaterThan(0);
    const prefixError = errors.find((e) => e.property === 'prefix');
    expect(prefixError).toBeDefined();

    await expect(
      pipe.transform(raw, { type: 'query', metatype: SuggestQueryDto }),
    ).rejects.toThrow();
  });

  it('AC-U-02: fails validation when prefix is empty or whitespace', async () => {
    const emptyRaw = { prefix: '' };
    const emptyDto = plainToInstance(SuggestQueryDto, emptyRaw);
    const emptyErrors = await validate(emptyDto);
    expect(emptyErrors.some((e) => e.property === 'prefix')).toBe(true);

    const spaceRaw = { prefix: '   ' };
    const spaceDto = plainToInstance(SuggestQueryDto, spaceRaw);
    const spaceErrors = await validate(spaceDto);
    expect(spaceErrors.some((e) => e.property === 'prefix')).toBe(true);
  });

  it('AC-U-03: fails validation when prefix exceeds 100 characters', async () => {
    const long101 = 'a'.repeat(101);
    const raw = { prefix: long101 };
    const dto = plainToInstance(SuggestQueryDto, raw);
    const errors = await validate(dto);

    expect(errors.some((e) => e.property === 'prefix')).toBe(true);
  });

  it('AC-U-04: passes validation when prefix has exactly 100 characters', async () => {
    const exact100 = 'a'.repeat(100);
    const raw = { prefix: exact100 };
    const dto = plainToInstance(SuggestQueryDto, raw);
    const errors = await validate(dto);

    const prefixError = errors.find((e) => e.property === 'prefix');
    expect(prefixError).toBeUndefined();
  });

  it('AC-U-05: defaults limit to 10 when parameter is omitted', async () => {
    const raw = { prefix: 'iphone' };
    const transformed = (await pipe.transform(raw, {
      type: 'query',
      metatype: SuggestQueryDto,
    })) as SuggestQueryDto;

    expect(transformed.limit).toBe(10);
  });

  it('AC-U-06: accepts valid limit parameter and transforms to number', async () => {
    const raw = { prefix: 'iphone', limit: '5' };
    const transformed = (await pipe.transform(raw, {
      type: 'query',
      metatype: SuggestQueryDto,
    })) as SuggestQueryDto;

    expect(transformed.limit).toBe(5);
  });

  it('AC-U-07: fails validation when limit exceeds 10 (D8)', async () => {
    const raw = { prefix: 'iphone', limit: 15 };
    const dto = plainToInstance(SuggestQueryDto, raw);
    const errors = await validate(dto);

    expect(errors.some((e) => e.property === 'limit')).toBe(true);

    await expect(
      pipe.transform(raw, { type: 'query', metatype: SuggestQueryDto }),
    ).rejects.toThrow();
  });

  it('AC-U-08: fails validation when limit is 0, negative, or non-integer', async () => {
    const invalidValues = [0, -1, 'abc', 3.14];

    for (const val of invalidValues) {
      const raw = { prefix: 'iphone', limit: val };
      const dto = plainToInstance(SuggestQueryDto, raw);
      const errors = await validate(dto);

      expect(errors.some((e) => e.property === 'limit')).toBe(true);
    }
  });

  it('accepts optional category_id as valid string', async () => {
    const raw = { prefix: 'iphone', category_id: 'category-phone' };
    const dto = plainToInstance(SuggestQueryDto, raw);
    const errors = await validate(dto);

    expect(errors.length).toBe(0);
    expect(dto.category_id).toBe('category-phone');
  });

  it('trims leading and trailing whitespace on prefix if provided', async () => {
    const raw = { prefix: '  iphone 16  ' };
    const transformed = (await pipe.transform(raw, {
      type: 'query',
      metatype: SuggestQueryDto,
    })) as SuggestQueryDto;

    expect(transformed.prefix).toBe('iphone 16');
  });
});
