import { Client, errors } from '@elastic/elasticsearch';
import { ES_REQUEST_TIMEOUT_MS } from '../common/constants';
import { AppConfigService } from '../config/config.service';
import { EsCategoryProjection } from './category-projection.service';
import { CategoryProjection } from './interfaces';

describe('EsCategoryProjection', () => {
  let service: EsCategoryProjection;
  let mockClient: {
    mget: jest.Mock;
    get: jest.Mock;
  };
  let mockConfigService: {
    esIndexPrefix: string;
  };

  beforeEach(() => {
    mockClient = {
      mget: jest.fn(),
      get: jest.fn(),
    };
    mockConfigService = {
      esIndexPrefix: 'test-',
    };

    service = new EsCategoryProjection(
      mockClient as unknown as Client,
      mockConfigService as unknown as AppConfigService,
    );
  });

  describe('getMany', () => {
    it('should return empty map without calling ES when ids array is empty', async () => {
      const result = await service.getMany([]);

      expect(result.size).toBe(0);
      expect(mockClient.mget).not.toHaveBeenCalled();
    });

    it('should dedupe ids and return only found docs in the result map', async () => {
      const cat1: CategoryProjection = {
        category_id: 'cat-1',
        name: 'Electronics',
        parent_id: null,
        path_ids: ['cat-1'],
        status: 'ACTIVE',
        source_version: 1,
        updated_at: '2026-10-14T10:00:00Z',
      };

      mockClient.mget.mockResolvedValue({
        docs: [
          {
            _id: 'cat-1',
            found: true,
            _source: cat1,
          },
          {
            _id: 'cat-2',
            found: false,
          },
        ],
      });

      const result = await service.getMany(['cat-1', 'cat-2', 'cat-1']);

      expect(mockClient.mget).toHaveBeenCalledTimes(1);
      expect(mockClient.mget).toHaveBeenCalledWith(
        {
          index: 'test-search-categories',
          ids: ['cat-1', 'cat-2'],
        },
        {
          requestTimeout: ES_REQUEST_TIMEOUT_MS,
        },
      );

      expect(result.size).toBe(1);
      expect(result.get('cat-1')).toEqual(cat1);
      expect(result.has('cat-2')).toBe(false);
    });

    it('should handle response with undefined docs', async () => {
      mockClient.mget.mockResolvedValue({});

      const result = await service.getMany(['cat-1']);

      expect(result.size).toBe(0);
    });
  });

  describe('resolvePathIds', () => {
    it('should return empty array without calling ES for empty or blank id', async () => {
      const result1 = await service.resolvePathIds('');
      const result2 = await service.resolvePathIds('   ');

      expect(result1).toEqual([]);
      expect(result2).toEqual([]);
      expect(mockClient.get).not.toHaveBeenCalled();
    });

    it('should return path_ids when doc is found', async () => {
      mockClient.get.mockResolvedValue({
        found: true,
        _source: {
          path_ids: ['root', 'child'],
        },
      });

      const result = await service.resolvePathIds('child');

      expect(mockClient.get).toHaveBeenCalledWith(
        {
          index: 'test-search-categories',
          id: 'child',
        },
        {
          requestTimeout: ES_REQUEST_TIMEOUT_MS,
        },
      );
      expect(result).toEqual(['root', 'child']);
    });

    it('should return empty array when doc found is true but path_ids is missing', async () => {
      mockClient.get.mockResolvedValue({
        found: true,
        _source: {},
      });

      const result = await service.resolvePathIds('child');
      expect(result).toEqual([]);
    });

    it('should return empty array when doc is not found (found: false)', async () => {
      mockClient.get.mockResolvedValue({
        found: false,
      });

      const result = await service.resolvePathIds('missing');
      expect(result).toEqual([]);
    });

    it('should return empty array without throwing when ES returns 404 ResponseError', async () => {
      const notFoundError = new errors.ResponseError({
        statusCode: 404,
        body: { error: { type: 'index_not_found_exception' } },
        warnings: [],
        headers: {},
      } as unknown as ConstructorParameters<typeof errors.ResponseError>[0]);

      mockClient.get.mockRejectedValue(notFoundError);

      const result = await service.resolvePathIds('missing-category');
      expect(result).toEqual([]);
    });

    it('should rethrow non-404 errors', async () => {
      const timeoutError = new errors.TimeoutError(
        'Request timed out',
        {} as unknown as ConstructorParameters<typeof errors.TimeoutError>[1],
      );

      mockClient.get.mockRejectedValue(timeoutError);

      await expect(service.resolvePathIds('cat-1')).rejects.toThrow(
        errors.TimeoutError,
      );
    });
  });
});
