import { Client } from '@elastic/elasticsearch';
import { Test, TestingModule } from '@nestjs/testing';
import {
  ES_REQUEST_TIMEOUT_MS,
  getSearchCategoriesIndex,
} from '../src/common/constants';
import { AppConfigService } from '../src/config/config.service';
import { EsCategoryProjection } from '../src/product-index/category-projection.service';
import { ELASTICSEARCH_CLIENT } from '../src/product-index/elasticsearch.module';
import {
  CATEGORY_PROJECTION,
  CategoryProjection,
  ICategoryProjection,
} from '../src/product-index/interfaces';
import { SEARCH_CATEGORIES_MAPPING_V1 } from '../src/product-index/mappings';

describe('EsCategoryProjection (Integration on real ES cluster)', () => {
  let service: ICategoryProjection;
  let client: Client;
  let moduleRef: TestingModule;

  const rand = Math.random().toString(36).substring(2, 8);
  const testPrefix = process.env.ES_INDEX_PREFIX
    ? `${process.env.ES_INDEX_PREFIX}${rand}-`
    : `it-shared-${rand}-`;

  const categoriesIndex = getSearchCategoriesIndex(testPrefix);

  const mockConfigService = {
    esNode: process.env.ES_NODE || 'http://192.168.100.23:30920',
    esIndexPrefix: testPrefix,
    serviceName: 'search-service',
    appEnv: 'test',
    appVersion: '1.0.0',
    kafkaBrokers: process.env.KAFKA_BROKERS
      ? process.env.KAFKA_BROKERS.split(',')
      : ['192.168.100.23:31714'],
    kafkaClientId: 'search-service',
    kafkaGroupId: 'search-service-group',
    kafkaTopicPrefix: '',
    kafkaConsumerEnabled: false,
  };

  const sampleRoot: CategoryProjection = {
    category_id: 'cat-root-1',
    name: 'Electronics',
    parent_id: null,
    path_ids: ['cat-root-1'],
    status: 'ACTIVE',
    source_version: 1,
    updated_at: '2026-10-14T10:00:00.000Z',
  };

  const sampleChild: CategoryProjection = {
    category_id: 'cat-child-1',
    name: 'Phones',
    parent_id: 'cat-root-1',
    path_ids: ['cat-root-1', 'cat-child-1'],
    status: 'ACTIVE',
    source_version: 1,
    updated_at: '2026-10-14T10:00:00.000Z',
  };

  const sampleLeaf: CategoryProjection = {
    category_id: 'cat-leaf-1',
    name: 'Smartphones',
    parent_id: 'cat-child-1',
    path_ids: ['cat-root-1', 'cat-child-1', 'cat-leaf-1'],
    status: 'ACTIVE',
    source_version: 1,
    updated_at: '2026-10-14T10:00:00.000Z',
  };

  beforeAll(async () => {
    moduleRef = await Test.createTestingModule({
      providers: [
        {
          provide: AppConfigService,
          useValue: mockConfigService,
        },
        {
          provide: ELASTICSEARCH_CLIENT,
          useFactory: () => {
            return new Client({
              node: mockConfigService.esNode,
              requestTimeout: ES_REQUEST_TIMEOUT_MS,
            });
          },
        },
        {
          provide: Client,
          useExisting: ELASTICSEARCH_CLIENT,
        },
        {
          provide: CATEGORY_PROJECTION,
          useClass: EsCategoryProjection,
        },
        EsCategoryProjection,
      ],
    }).compile();

    service = moduleRef.get<ICategoryProjection>(CATEGORY_PROJECTION);
    client = moduleRef.get(Client);

    // Create search-categories index with mapping
    await client.indices.create({
      index: categoriesIndex,
      mappings: SEARCH_CATEGORIES_MAPPING_V1,
    });

    // Seed test categories
    await client.index({
      index: categoriesIndex,
      id: sampleRoot.category_id,
      document: sampleRoot,
    });
    await client.index({
      index: categoriesIndex,
      id: sampleChild.category_id,
      document: sampleChild,
    });
    await client.index({
      index: categoriesIndex,
      id: sampleLeaf.category_id,
      document: sampleLeaf,
    });

    await client.indices.refresh({ index: categoriesIndex });
  });

  afterAll(async () => {
    try {
      if (await client.indices.exists({ index: categoriesIndex })) {
        await client.indices.delete({ index: categoriesIndex });
      }
    } catch (cleanupErr) {
      console.warn('Error during ES cleanup:', cleanupErr);
    }

    await client.close();
    await moduleRef.close();
  });

  describe('getMany', () => {
    it('should return an empty map when ids array is empty', async () => {
      const result = await service.getMany([]);
      expect(result.size).toBe(0);
    });

    it('should retrieve existing categories, dedupe ids, and skip missing ones', async () => {
      const result = await service.getMany([
        'cat-root-1',
        'cat-child-1',
        'cat-root-1',
        'cat-non-existent',
      ]);

      expect(result.size).toBe(2);
      expect(result.has('cat-root-1')).toBe(true);
      expect(result.has('cat-child-1')).toBe(true);
      expect(result.has('cat-non-existent')).toBe(false);

      const root = result.get('cat-root-1');
      expect(root?.name).toBe('Electronics');
      expect(root?.path_ids).toEqual(['cat-root-1']);

      const child = result.get('cat-child-1');
      expect(child?.name).toBe('Phones');
      expect(child?.parent_id).toBe('cat-root-1');
      expect(child?.path_ids).toEqual(['cat-root-1', 'cat-child-1']);
    });
  });

  describe('resolvePathIds', () => {
    it('should resolve full path_ids hierarchy for a category', async () => {
      const pathIds = await service.resolvePathIds('cat-leaf-1');
      expect(pathIds).toEqual(['cat-root-1', 'cat-child-1', 'cat-leaf-1']);
    });

    it('should return root path_ids for root category', async () => {
      const pathIds = await service.resolvePathIds('cat-root-1');
      expect(pathIds).toEqual(['cat-root-1']);
    });

    it('should return empty array for missing category without throwing', async () => {
      const pathIds = await service.resolvePathIds('cat-missing');
      expect(pathIds).toEqual([]);
    });

    it('should return empty array for empty string without throwing', async () => {
      const pathIds = await service.resolvePathIds('');
      expect(pathIds).toEqual([]);
    });
  });
});
