import { Client } from '@elastic/elasticsearch';
import { AppConfigService } from '../config/config.service';
import { IndexBootstrapService } from './index-bootstrap.service';
import { PRODUCTS_INDEX_MAPPING_V1 } from './mappings';

describe('IndexBootstrapService', () => {
  let service: IndexBootstrapService;
  let mockClient: jest.Mocked<Partial<Client>>;
  let mockIndices: {
    existsAlias: jest.Mock;
    exists: jest.Mock;
    create: jest.Mock;
    updateAliases: jest.Mock;
    getAlias: jest.Mock;
    delete: jest.Mock;
    putMapping: jest.Mock;
  };
  let mockConfigService: Partial<AppConfigService>;

  beforeEach(() => {
    mockIndices = {
      existsAlias: jest.fn(),
      exists: jest.fn(),
      create: jest.fn(),
      updateAliases: jest.fn(),
      getAlias: jest.fn(),
      delete: jest.fn(),
      putMapping: jest.fn().mockResolvedValue({ acknowledged: true }),
    };

    mockClient = {
      indices: mockIndices as unknown as Client['indices'],
    };

    mockConfigService = {
      esIndexPrefix: 'test-',
    };

    service = new IndexBootstrapService(
      mockClient as Client,
      mockConfigService as AppConfigService,
    );
  });

  it('should initialize all indices and alias when none exist', async () => {
    mockIndices.existsAlias.mockResolvedValue(false);
    mockIndices.exists.mockResolvedValue(false);
    mockIndices.create.mockResolvedValue({ acknowledged: true });
    mockIndices.updateAliases.mockResolvedValue({ acknowledged: true });
    mockIndices.getAlias.mockImplementation(async () => ({
      'test-products-v1-20261014100000': {
        aliases: { 'test-products-read': {} },
      },
    }));

    const result = await service.bootstrap();

    expect(result.aliasCreated).toBe(true);
    expect(result.consumerStateCreated).toBe(true);
    expect(result.reindexJobsCreated).toBe(true);
    expect(result.searchCategoriesCreated).toBe(true);
    expect(mockIndices.create).toHaveBeenCalledTimes(4); // products + consumer_state + reindex_jobs + search_categories
    expect(mockIndices.updateAliases).toHaveBeenCalledWith({
      actions: [
        {
          add: {
            index: expect.stringContaining('test-products-v1-'),
            alias: 'test-products-read',
          },
        },
      ],
    });
  });

  it('should handle multi-replica race where alias points to 2 indices, keeping lexicographically smallest', async () => {
    mockIndices.existsAlias.mockResolvedValue(false);
    mockIndices.exists.mockResolvedValue(false);
    mockIndices.create.mockResolvedValue({ acknowledged: true });
    mockIndices.updateAliases.mockResolvedValue({ acknowledged: true });

    // Simulate another replica having already created and aliased index 20261014100000
    // while this replica created 20261014100001
    let createdIndex = '';
    mockIndices.create.mockImplementation(async (params: { index: string }) => {
      if (params.index.includes('products-v1')) {
        createdIndex = params.index;
      }
      return { acknowledged: true };
    });

    const keeperIndex = 'test-products-v1-20261014100000';
    mockIndices.getAlias.mockImplementation(async () => {
      return {
        [createdIndex]: { aliases: { 'test-products-read': {} } },
        [keeperIndex]: { aliases: { 'test-products-read': {} } },
      };
    });

    const result = await service.bootstrap();

    expect(mockIndices.updateAliases).toHaveBeenCalledTimes(2);
    // Second updateAliases call removes the alias from the larger index
    const largerIndex = [createdIndex, keeperIndex].sort()[1];
    expect(mockIndices.updateAliases).toHaveBeenLastCalledWith({
      actions: [
        {
          remove: {
            index: largerIndex,
            alias: 'test-products-read',
          },
        },
      ],
    });

    // If this instance created the larger index, it must delete it
    if (createdIndex === largerIndex) {
      expect(mockIndices.delete).toHaveBeenCalledWith({
        index: createdIndex,
      });
    }
    expect(result.activeTargetIndex).toBe('test-products-v1-20261014100000');
  });

  it('should treat resource_already_exists_exception on create as success', async () => {
    mockIndices.existsAlias.mockResolvedValue(false);
    mockIndices.exists.mockResolvedValue(false);
    const existsError = new Error('resource_already_exists_exception');
    (
      existsError as unknown as { meta: { body: object; statusCode: number } }
    ).meta = {
      statusCode: 400,
      body: { error: { type: 'resource_already_exists_exception' } },
    };
    mockIndices.create.mockRejectedValue(existsError);
    mockIndices.updateAliases.mockResolvedValue({ acknowledged: true });
    mockIndices.getAlias.mockResolvedValue({
      'test-products-v1-20261014100000': {
        aliases: { 'test-products-read': {} },
      },
    });

    const result = await service.bootstrap();
    expect(result.aliasCreated).toBe(true);
    expect(result.consumerStateCreated).toBe(false);
  });

  it('adds new fields to internal indices and the existing products index', async () => {
    mockIndices.existsAlias.mockResolvedValue(true);
    mockIndices.exists.mockResolvedValue(true);
    mockIndices.getAlias.mockResolvedValue({
      'test-products-v1-20261014100000': {
        aliases: { 'test-products-read': {} },
      },
    });

    await service.bootstrap();

    const syncedIndices = mockIndices.putMapping.mock.calls.map(
      ([req]: [{ index: string }]) => req.index,
    );
    expect(syncedIndices).toEqual(
      expect.arrayContaining([
        'test-products-v1-20261014100000',
        'test-search-consumer-state',
        'test-search-reindex-jobs',
        'test-search-categories',
      ]),
    );
    expect(mockIndices.putMapping).toHaveBeenCalledWith({
      index: 'test-products-v1-20261014100000',
      properties: PRODUCTS_INDEX_MAPPING_V1.properties,
    });
    const reindexJobsCall = mockIndices.putMapping.mock.calls.find(
      ([req]: [{ index: string }]) => req.index === 'test-search-reindex-jobs',
    ) as [{ properties: Record<string, unknown> }];
    expect(reindexJobsCall[0].properties).toHaveProperty('total_estimate', {
      type: 'long',
    });
  });

  it('logs a warning and continues when putMapping fails on existing products index', async () => {
    mockIndices.existsAlias.mockResolvedValue(true);
    mockIndices.exists.mockResolvedValue(true);
    mockIndices.getAlias.mockResolvedValue({
      'test-products-v1-20261014100000': {
        aliases: { 'test-products-read': {} },
      },
    });
    mockIndices.putMapping.mockImplementation(
      async (params: { index: string }) => {
        if (params.index === 'test-products-v1-20261014100000') {
          throw new Error('Mapping sync conflict');
        }
        return { acknowledged: true };
      },
    );

    const result = await service.bootstrap();

    expect(result.activeTargetIndex).toBe('test-products-v1-20261014100000');
    expect(result.consumerStateCreated).toBe(false);
  });
});
