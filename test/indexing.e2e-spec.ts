import { Client } from '@elastic/elasticsearch';
import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Consumer, Kafka, Producer } from 'kafkajs';
import { AppModule } from '../src/app.module';
import {
  CONSUMER_STATUS_PROVIDER,
  IConsumerStatusProvider,
} from '../src/event-consumer/interfaces';
import { KAFKA_CLIENT } from '../src/event-consumer/kafka.module';
import { ELASTICSEARCH_CLIENT } from '../src/product-index/elasticsearch.module';
import { IndexBootstrapService } from '../src/product-index/index-bootstrap.service';
import {
  INDEX_WRITER,
  IIndexWriter,
  PRODUCT_DOCUMENT_BUILDER,
} from '../src/product-index/interfaces';

describe('Indexing Module (e2e - no cluster required)', () => {
  let app: INestApplication;
  let mockEsClient: jest.Mocked<Partial<Client>>;
  let mockKafka: jest.Mocked<Partial<Kafka>>;
  let mockConsumer: jest.Mocked<Partial<Consumer>>;
  let mockProducer: jest.Mocked<Partial<Producer>>;
  let eachBatchHandler: ((payload: unknown) => Promise<void>) | undefined;

  beforeAll(async () => {
    mockEsClient = {
      bulk: jest.fn().mockResolvedValue({ errors: false, items: [] }),
      index: jest.fn().mockResolvedValue({ result: 'created' }),
      update: jest.fn().mockResolvedValue({ result: 'updated' }),
      get: jest.fn().mockResolvedValue({ found: false }),
      mget: jest.fn().mockResolvedValue({ docs: [] }),
      search: jest.fn().mockResolvedValue({ hits: { hits: [], total: { value: 0 } } }),
      count: jest.fn().mockResolvedValue({ count: 0 }),
      close: jest.fn().mockResolvedValue(undefined),
      indices: {
        exists: jest.fn().mockResolvedValue(true),
        create: jest.fn().mockResolvedValue({ acknowledged: true }),
        putAlias: jest.fn().mockResolvedValue({ acknowledged: true }),
      } as unknown as Client['indices'],
    };

    mockConsumer = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      subscribe: jest.fn().mockResolvedValue(undefined),
      run: jest.fn().mockImplementation(async (options) => {
        eachBatchHandler = options?.eachBatch;
      }),
      stop: jest.fn().mockResolvedValue(undefined),
    };

    mockProducer = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      send: jest.fn().mockResolvedValue([]),
    };

    mockKafka = {
      consumer: jest.fn().mockReturnValue(mockConsumer as Consumer),
      producer: jest.fn().mockReturnValue(mockProducer as Producer),
      admin: jest.fn().mockReturnValue({
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        fetchOffsets: jest.fn().mockResolvedValue([]),
        fetchTopicOffsets: jest.fn().mockResolvedValue([]),
      }),
    };

    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    })
      .overrideProvider(ELASTICSEARCH_CLIENT)
      .useValue(mockEsClient)
      .overrideProvider(Client)
      .useValue(mockEsClient)
      .overrideProvider(KAFKA_CLIENT)
      .useValue(mockKafka)
      .overrideProvider(Kafka)
      .useValue(mockKafka)
      .overrideProvider(IndexBootstrapService)
      .useValue({
        onApplicationBootstrap: jest.fn().mockResolvedValue(undefined),
        bootstrap: jest.fn().mockResolvedValue({
          alias: 'products-read',
          aliasCreated: false,
          consumerStateIndex: 'search-consumer-state',
          consumerStateCreated: false,
          reindexJobsIndex: 'search-reindex-jobs',
          reindexJobsCreated: false,
          searchCategoriesIndex: 'search-categories',
          searchCategoriesCreated: false,
        }),
      })
      .compile();

    app = moduleFixture.createNestApplication();
    await app.init();
  });

  afterAll(async () => {
    if (app) {
      await app.close();
    }
  });

  describe('[IDX-E2E-01] Module Bootstrap without Cluster', () => {
    it('[IDX-E2E-01] should successfully start Nest application context without contacting real ES or Kafka', () => {
      expect(app).toBeDefined();
    });

    it('[IDX-E2E-01] should export PRODUCT_DOCUMENT_BUILDER and INDEX_WRITER globally', () => {
      const builder = app.get(PRODUCT_DOCUMENT_BUILDER, { strict: false });
      const writer = app.get(INDEX_WRITER, { strict: false });

      expect(builder).toBeDefined();
      expect(writer).toBeDefined();
    });
  });

  describe('[IDX-E2E-02] Consumer Status Provider DI', () => {
    it('[IDX-E2E-02] should provide CONSUMER_STATUS_PROVIDER in application context', async () => {
      const statusProvider = app.get<IConsumerStatusProvider>(
        CONSUMER_STATUS_PROVIDER,
        { strict: false },
      );

      expect(statusProvider).toBeDefined();
      const status = await statusProvider.getStatus();
      expect(status).toHaveProperty('state');
      expect(status).toHaveProperty('lagByTopic');
      expect(status).toHaveProperty('maxLagSeconds');
      expect(status).toHaveProperty('dlqCount');
    });
  });

  describe('[IDX-E2E-03] End-to-end event flow to mocked ES', () => {
    it('[IDX-E2E-03] should accept incoming product events and call bulk write on mocked ES client', async () => {
      const writer = app.get<IIndexWriter>(INDEX_WRITER, { strict: false });

      const sampleDoc = {
        product_id: 'e2e-prod-1',
        shop_id: 'shop-1',
        slug: 'e2e-prod',
        title: 'E2E Product',
        title_exact: 'E2E Product',
        description: 'E2E Desc',
        category_ids: ['cat-1'],
        category_path: ['root'],
        brand: 'E2E Brand',
        price: 99000,
        currency: 'VND',
        rating_avg: 5,
        rating_count: 10,
        attributes: {},
        spec_attributes: {},
        attribute_pairs: [],
        visibility_status: 'PUBLISHED' as const,
        source_version: 1,
        source_event_id: 'evt-e2e-1',
        indexed_at: '2026-10-14T10:00:00Z',
      };

      await writer.bulkUpsert('products-v1', [sampleDoc]);

      expect(mockEsClient.bulk).toHaveBeenCalled();
    });
  });
});
