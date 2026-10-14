import { ValidationPipe } from '@nestjs/common';
import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
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

  describe('Type-only validation (Techlead review requirement 3)', () => {
    it('accepts empty object when prefix and limit are omitted (semantic validation handled by service)', async () => {
      const raw = {};
      const dto = plainToInstance(SuggestQueryDto, raw);
      const errors = await validate(dto);

      expect(errors.length).toBe(0);
      expect(dto.prefix).toBeUndefined();
      expect(dto.limit).toBeUndefined();
    });

    it('accepts valid string prefix including whitespace or empty string (semantic validation handled by service)', async () => {
      const validCases = ['iphone', '', '   ', 'a'.repeat(101)];
      for (const val of validCases) {
        const raw = { prefix: val };
        const dto = plainToInstance(SuggestQueryDto, raw);
        const errors = await validate(dto);
        expect(errors.length).toBe(0);
      }
    });

    it('rejects non-string prefix', async () => {
      const raw = { prefix: 12345 };
      const dto = plainToInstance(SuggestQueryDto, raw);
      const errors = await validate(dto);

      expect(errors.some((e) => e.property === 'prefix')).toBe(true);
      await expect(
        pipe.transform(raw, { type: 'query', metatype: SuggestQueryDto }),
      ).rejects.toThrow();
    });

    it('AC-U-06: transforms and accepts string integer limit', async () => {
      const raw = { prefix: 'iphone', limit: '5' };
      const transformed = (await pipe.transform(raw, {
        type: 'query',
        metatype: SuggestQueryDto,
      })) as SuggestQueryDto;

      expect(transformed.limit).toBe(5);
    });

    it('AC-U-05: accepts integer limit outside 1..10 in DTO (semantic validation handled by service)', async () => {
      const values = [0, -1, 15];
      for (const val of values) {
        const raw = { prefix: 'iphone', limit: val };
        const dto = plainToInstance(SuggestQueryDto, raw);
        const errors = await validate(dto);
        expect(errors.some((e) => e.property === 'limit')).toBe(false);
      }
    });

    it('AC-U-07: rejects non-integer limit (e.g. strings that are not ints, floats)', async () => {
      const invalidValues = ['abc', 3.14];
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

    it('rejects non-string category_id', async () => {
      const raw = { prefix: 'iphone', category_id: 123 };
      const dto = plainToInstance(SuggestQueryDto, raw);
      const errors = await validate(dto);

      expect(errors.some((e) => e.property === 'category_id')).toBe(true);
    });
  });
});
