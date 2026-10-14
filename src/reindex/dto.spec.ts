import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { AdminSearchStatusQueryDto, ReindexRequestDto } from './dto';
import { ReindexSource } from './enums';

describe('Reindex DTOs', () => {
  describe('ReindexRequestDto', () => {
    it('validates a correct payload', async () => {
      const dto = plainToInstance(ReindexRequestDto, {
        source: 'INDEX_COPY',
        mapping_version: 1,
        reason: 'Regular mapping update',
      });
      const errors = await validate(dto);
      expect(errors).toHaveLength(0);
    });

    it('defaults source to INDEX_COPY when missing (D16)', async () => {
      const dto = plainToInstance(ReindexRequestDto, {
        mapping_version: 1,
        reason: 'Regular mapping update',
      });
      const errors = await validate(dto);
      expect(errors).toHaveLength(0);
      expect(dto.source).toBe(ReindexSource.INDEX_COPY);
    });

    it('fails when source is invalid', async () => {
      const dto = plainToInstance(ReindexRequestDto, {
        source: 'DIRECT_DB',
        mapping_version: 1,
        reason: 'Regular mapping update',
      });
      const errors = await validate(dto);
      expect(errors.length).toBeGreaterThan(0);
      expect(errors.some((e) => e.property === 'source')).toBe(true);
    });

    it('fails when mapping_version is missing or invalid', async () => {
      const dtoMissing = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        reason: 'Regular mapping update',
      });
      const errorsMissing = await validate(dtoMissing);
      expect(errorsMissing.some((e) => e.property === 'mapping_version')).toBe(
        true,
      );

      const dtoFloat = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        mapping_version: 1.5,
        reason: 'Regular mapping update',
      });
      const errorsFloat = await validate(dtoFloat);
      expect(errorsFloat.some((e) => e.property === 'mapping_version')).toBe(
        true,
      );

      const dtoNegative = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        mapping_version: -1,
        reason: 'Regular mapping update',
      });
      const errorsNegative = await validate(dtoNegative);
      expect(errorsNegative.some((e) => e.property === 'mapping_version')).toBe(
        true,
      );
    });

    it('fails when reason is missing or empty or whitespace-only', async () => {
      const dtoMissing = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        mapping_version: 1,
      });
      const errorsMissing = await validate(dtoMissing);
      expect(errorsMissing.some((e) => e.property === 'reason')).toBe(true);

      const dtoEmpty = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        mapping_version: 1,
        reason: '',
      });
      const errorsEmpty = await validate(dtoEmpty);
      expect(errorsEmpty.some((e) => e.property === 'reason')).toBe(true);

      const dtoWhitespace = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        mapping_version: 1,
        reason: '   ',
      });
      const errorsWhitespace = await validate(dtoWhitespace);
      expect(errorsWhitespace.some((e) => e.property === 'reason')).toBe(true);
    });

    it('fails when reason exceeds 200 characters', async () => {
      const dto = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        mapping_version: 1,
        reason: 'a'.repeat(256),
      });
      const errors = await validate(dto);
      expect(errors.some((e) => e.property === 'reason')).toBe(true);
    });
  });

  describe('AdminSearchStatusQueryDto', () => {
    it('accepts valid query characters', async () => {
      const dto = plainToInstance(AdminSearchStatusQueryDto, {
        index: 'products-v1-20261014',
        consumer_group: 'search-service-group',
      });
      const errors = await validate(dto);
      expect(errors).toHaveLength(0);
    });

    it('rejects forbidden characters in index or consumer_group', async () => {
      const dto = plainToInstance(AdminSearchStatusQueryDto, {
        index: 'products;rm -rf /',
      });
      const errors = await validate(dto);
      expect(errors.some((e) => e.property === 'index')).toBe(true);
    });
  });
});
