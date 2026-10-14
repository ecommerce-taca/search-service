import { Client } from '@elastic/elasticsearch';
import { Test, TestingModule } from '@nestjs/testing';
import {
  ES_REQUEST_TIMEOUT_MS,
  getConsumerStateIndex,
  getProductsAlias,
  getReindexJobsIndex,
  getSearchCategoriesIndex,
} from '../src/common/constants';
import { AppConfigService } from '../src/config/config.service';
import { ELASTICSEARCH_CLIENT } from '../src/product-index/elasticsearch.module';
import { IndexBootstrapService } from '../src/product-index/index-bootstrap.service';

describe('IndexBootstrapService (Integration on real ES cluster)', () => {
  let service: IndexBootstrapService;
  let client: Client;
  let moduleRef: TestingModule;

  const testPrefix = process.env.ES_INDEX_PREFIX
    ? `${process.env.ES_INDEX_PREFIX}${Date.now()}-`
    : `it-bootstrap-${Date.now()}-`;

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
        IndexBootstrapService,
      ],
    }).compile();

    service = moduleRef.get(IndexBootstrapService);
    client = moduleRef.get(Client);
  });

  afterAll(async () => {
    try {
      const alias = getProductsAlias(testPrefix);
      const aliasExists = await client.indices.existsAlias({ name: alias });
      if (aliasExists) {
        const aliasInfo = await client.indices.getAlias({ name: alias });
        const indices = Object.keys(aliasInfo);
        if (indices.length > 0) {
          await client.indices.delete({ index: indices });
        }
      }

      const consumerIndex = getConsumerStateIndex(testPrefix);
      if (await client.indices.exists({ index: consumerIndex })) {
        await client.indices.delete({ index: consumerIndex });
      }

      const reindexIndex = getReindexJobsIndex(testPrefix);
      if (await client.indices.exists({ index: reindexIndex })) {
        await client.indices.delete({ index: reindexIndex });
      }

      const categoriesIndex = getSearchCategoriesIndex(testPrefix);
      if (await client.indices.exists({ index: categoriesIndex })) {
        await client.indices.delete({ index: categoriesIndex });
      }
    } catch (cleanupErr) {
      console.warn('Error during ES cleanup:', cleanupErr);
    }

    await client.close();
    await moduleRef.close();
  });

  it('should initialize products index with alias and internal indices on real ES', async () => {
    const result = await service.bootstrap();

    expect(result.aliasCreated).toBe(true);
    expect(result.consumerStateCreated).toBe(true);
    expect(result.reindexJobsCreated).toBe(true);
    expect(result.searchCategoriesCreated).toBe(true);

    const aliasName = getProductsAlias(testPrefix);
    const aliasExists = await client.indices.existsAlias({ name: aliasName });
    expect(aliasExists).toBe(true);

    const consumerStateIndex = getConsumerStateIndex(testPrefix);
    const consumerExists = await client.indices.exists({
      index: consumerStateIndex,
    });
    expect(consumerExists).toBe(true);

    const reindexJobsIndex = getReindexJobsIndex(testPrefix);
    const reindexExists = await client.indices.exists({
      index: reindexJobsIndex,
    });
    expect(reindexExists).toBe(true);

    const searchCategoriesIndex = getSearchCategoriesIndex(testPrefix);
    const categoriesExists = await client.indices.exists({
      index: searchCategoriesIndex,
    });
    expect(categoriesExists).toBe(true);
  });

  it('should be idempotent when called a second time', async () => {
    const secondRun = await service.bootstrap();

    expect(secondRun.aliasCreated).toBe(false);
    expect(secondRun.consumerStateCreated).toBe(false);
    expect(secondRun.reindexJobsCreated).toBe(false);
    expect(secondRun.searchCategoriesCreated).toBe(false);
  });
});
