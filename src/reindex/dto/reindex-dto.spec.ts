import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { MAPPING_VERSION } from '../../common/constants';
import {
  ReindexRequestDto,
  ReindexSource,
  StatusQueryDto,
} from './reindex-request.dto';

describe('Reindex DTOs Validation (Unit)', () => {
  describe('ReindexRequestDto', () => {
    it('[RX-U-05] Payload validation: missing source field defaults to INDEX_COPY (D16)', async () => {
      const dto = plainToInstance(ReindexRequestDto, {
        mapping_version: MAPPING_VERSION,
        reason: 'Valid migration reason',
      });

      const errors = await validate(dto);
      expect(errors.length).toBe(0);
      expect(dto.source).toBe(ReindexSource.INDEX_COPY);
    });

    it('[RX-U-06] Payload validation: source is invalid string "DIRECT_DB" fails validation', async () => {
      const dto = plainToInstance(ReindexRequestDto, {
        source: 'DIRECT_DB',
        mapping_version: MAPPING_VERSION,
        reason: 'Upgrade mappings',
      });

      const errors = await validate(dto);
      expect(errors.length).toBeGreaterThan(0);
      const sourceError = errors.find((e) => e.property === 'source');
      expect(sourceError).toBeDefined();
    });

    it('[RX-U-07] Payload validation: missing mapping_version field fails validation', async () => {
      const dto = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        reason: 'Upgrade mappings',
      });

      const errors = await validate(dto);
      expect(errors.length).toBeGreaterThan(0);
      const mappingError = errors.find((e) => e.property === 'mapping_version');
      expect(mappingError).toBeDefined();
    });

    it('[RX-U-08] Payload validation: mapping_version is float (1.5), negative (-1), or string ("v1") fails validation', async () => {
      const invalidVersions = [1.5, -1, 'v1', 0];

      for (const val of invalidVersions) {
        const dto = plainToInstance(ReindexRequestDto, {
          source: ReindexSource.INDEX_COPY,
          mapping_version: val,
          reason: 'Upgrade mappings',
        });

        const errors = await validate(dto);
        expect(errors.length).toBeGreaterThan(0);
        const mappingError = errors.find(
          (e) => e.property === 'mapping_version',
        );
        expect(mappingError).toBeDefined();
      }
    });

    it('[RX-U-09] Payload validation: mapping_version is integer 2 when application mapping version is 1 fails validation', async () => {
      const dto = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        mapping_version: 2,
        reason: 'Upgrade mappings',
      });

      const errors = await validate(dto);
      expect(errors.length).toBeGreaterThan(0);
      const mappingError = errors.find((e) => e.property === 'mapping_version');
      expect(mappingError).toBeDefined();
    });

    it('[RX-U-10] Payload validation: missing reason field fails validation', async () => {
      const dto = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        mapping_version: MAPPING_VERSION,
      });

      const errors = await validate(dto);
      expect(errors.length).toBeGreaterThan(0);
      const reasonError = errors.find((e) => e.property === 'reason');
      expect(reasonError).toBeDefined();
    });

    it('[RX-U-11] Payload validation: reason is empty string "" or whitespace-only "   " fails validation', async () => {
      const emptyReasons = ['', '   ', '\t\n'];

      for (const empty of emptyReasons) {
        const dto = plainToInstance(ReindexRequestDto, {
          source: ReindexSource.INDEX_COPY,
          mapping_version: MAPPING_VERSION,
          reason: empty,
        });

        const errors = await validate(dto);
        expect(errors.length).toBeGreaterThan(0);
        const reasonError = errors.find((e) => e.property === 'reason');
        expect(reasonError).toBeDefined();
      }
    });

    it('[RX-U-12] Payload validation: reason length constraints (3..200 characters per design) fails when out of bounds', async () => {
      const tooShortDto = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        mapping_version: MAPPING_VERSION,
        reason: 'ab', // < 3
      });
      const shortErrors = await validate(tooShortDto);
      expect(shortErrors.length).toBeGreaterThan(0);
      expect(shortErrors.some((e) => e.property === 'reason')).toBe(true);

      const tooLongDto = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        mapping_version: MAPPING_VERSION,
        reason: 'a'.repeat(201), // > 200
      });
      const longErrors = await validate(tooLongDto);
      expect(longErrors.length).toBeGreaterThan(0);
      expect(longErrors.some((e) => e.property === 'reason')).toBe(true);

      const validDto = plainToInstance(ReindexRequestDto, {
        source: ReindexSource.INDEX_COPY,
        mapping_version: MAPPING_VERSION,
        reason: 'Valid reason with 3 chars minimum and within 200 chars.',
      });
      const validErrors = await validate(validDto);
      expect(validErrors.length).toBe(0);
    });

    it('[RX-U-13] Payload validation: client submits unexpected extra fields (target_index, state) fails with forbidNonWhitelisted or strips them', async () => {
      const rawPayload = {
        source: 'INDEX_COPY',
        mapping_version: MAPPING_VERSION,
        reason: 'Valid migration reason',
        target_index: 'hacked-index',
        state: 'SWAPPED',
      };

      const dto = plainToInstance(ReindexRequestDto, rawPayload);
      const errors = await validate(dto, {
        whitelist: true,
        forbidNonWhitelisted: true,
      });

      expect(errors.length).toBeGreaterThan(0);
      const extraProperties = errors.map((e) => e.property);
      expect(
        extraProperties.includes('target_index') ||
          extraProperties.includes('state'),
      ).toBe(true);
    });
  });

  describe('StatusQueryDto', () => {
    it('accepts valid index and consumer_group conforming to allowlist chars', async () => {
      const dto = plainToInstance(StatusQueryDto, {
        index: 'products-v1-20261014120000',
        consumer_group: 'search-service-group',
      });

      const errors = await validate(dto);
      expect(errors.length).toBe(0);
    });

    it('rejects suspicious special characters in index and consumer_group', async () => {
      const invalidInputs = [
        { index: '../products', consumer_group: 'group' },
        { index: 'products*', consumer_group: 'group' },
        { index: 'products', consumer_group: 'group; rm -rf' },
        { index: '<script>alert(1)</script>', consumer_group: 'group' },
      ];

      for (const input of invalidInputs) {
        const dto = plainToInstance(StatusQueryDto, input);
        const errors = await validate(dto);
        expect(errors.length).toBeGreaterThan(0);
      }
    });
  });
});
