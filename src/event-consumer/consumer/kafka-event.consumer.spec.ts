import { Client } from '@elastic/elasticsearch';
import { Test, TestingModule } from '@nestjs/testing';
import { Consumer, Kafka, Producer } from 'kafkajs';
import { Registry } from 'prom-client';
import {
  EVENT_RETRY_BACKOFF_MS,
  EVENT_RETRY_COUNT,
} from '../../common/constants';
import { RequestContext } from '../../common/context/request-context';
import { AppConfigService } from '../../config/config.service';
import { MetricsService } from '../../observability/metrics.service';
import { ELASTICSEARCH_CLIENT } from '../../product-index/elasticsearch.module';
import {
  CatalogHandler,
  CategoryHandler,
  ProductHandler,
  RatingHandler,
  SkuHandler,
} from '../handlers';
import { KAFKA_CLIENT } from '../kafka.module';
import { ConsumerStatusService } from '../status/consumer-status.service';
import { DlqPublisher } from './dlq-publisher';
import { EventProcessor } from './event-processor';
import { KafkaEventConsumer } from './kafka-event.consumer';

describe('KafkaEventConsumer', () => {
  let consumerService: KafkaEventConsumer;
  let mockKafka: jest.Mocked<Partial<Kafka>>;
  let mockConsumer: jest.Mocked<Partial<Consumer>>;
  let mockProducer: jest.Mocked<Partial<Producer>>;
  let mockClient: jest.Mocked<Partial<Client>>;
  let testRegistry: Registry;
  let mockConfigService: jest.Mocked<Partial<AppConfigService>> & {
    kafkaConsumerEnabled?: boolean;
    kafkaGroupId?: string;
    kafkaTopicPrefix?: string;
  };
  let mockProductHandler: jest.Mocked<Partial<ProductHandler>>;
  let mockSkuHandler: jest.Mocked<Partial<SkuHandler>>;
  let mockCategoryHandler: jest.Mocked<Partial<CategoryHandler>>;
  let mockCatalogHandler: jest.Mocked<Partial<CatalogHandler>>;
  let mockRatingHandler: jest.Mocked<Partial<RatingHandler>>;
  let mockStatusService: jest.Mocked<Partial<ConsumerStatusService>>;

  async function createTestModule(overrides?: {
    config?: Partial<AppConfigService>;
    productHandler?: Partial<ProductHandler>;
  }): Promise<TestingModule> {
    const config = { ...mockConfigService, ...(overrides?.config || {}) };
    testRegistry = new Registry();
    return Test.createTestingModule({
      providers: [
        KafkaEventConsumer,
        EventProcessor,
        DlqPublisher,
        {
          provide: AppConfigService,
          useValue: config,
        },
        {
          provide: KAFKA_CLIENT,
          useValue: mockKafka,
        },
        {
          provide: Kafka,
          useValue: mockKafka,
        },
        {
          provide: ELASTICSEARCH_CLIENT,
          useValue: mockClient,
        },
        {
          provide: MetricsService,
          useValue: {
            getRegistry: jest.fn().mockReturnValue(testRegistry),
          },
        },
        {
          provide: ProductHandler,
          useValue: overrides?.productHandler || mockProductHandler,
        },
        {
          provide: SkuHandler,
          useValue: mockSkuHandler,
        },
        {
          provide: CategoryHandler,
          useValue: mockCategoryHandler,
        },
        {
          provide: CatalogHandler,
          useValue: mockCatalogHandler,
        },
        {
          provide: RatingHandler,
          useValue: mockRatingHandler,
        },
        {
          provide: ConsumerStatusService,
          useValue: mockStatusService,
        },
      ],
    }).compile();
  }

  beforeEach(async () => {
    mockConsumer = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      subscribe: jest.fn().mockResolvedValue(undefined),
      run: jest.fn().mockResolvedValue(undefined),
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
    };

    mockClient = {
      index: jest.fn().mockResolvedValue({ result: 'created' }),
      create: jest.fn().mockResolvedValue({ result: 'created' }),
      update: jest.fn().mockResolvedValue({ result: 'updated' }),
      get: jest.fn().mockResolvedValue({ found: false }),
    };

    mockConfigService = {
      kafkaConsumerEnabled: true,
      kafkaGroupId: 'search-service-group',
      kafkaTopicPrefix: 'test-',
    };

    mockProductHandler = {
      handle: jest.fn().mockResolvedValue(undefined),
    };
    mockSkuHandler = {
      handle: jest.fn().mockResolvedValue(undefined),
    };
    mockCategoryHandler = {
      handle: jest.fn().mockResolvedValue(undefined),
    };
    mockCatalogHandler = {
      handle: jest.fn().mockResolvedValue(undefined),
    };
    mockRatingHandler = {
      handle: jest.fn().mockResolvedValue(undefined),
    };
    mockStatusService = {
      setConsumerState: jest.fn(),
      recordEventProcessed: jest.fn(),
    };

    const module = await createTestModule();
    consumerService = module.get<KafkaEventConsumer>(KafkaEventConsumer);
  });

  describe('[IDX-CSM-01] topic subscriptions', () => {
    it('[IDX-CSM-01] should subscribe to all 5 topics with prefix on startup', async () => {
      await consumerService.onApplicationBootstrap();

      expect(mockConsumer.connect).toHaveBeenCalled();
      expect(mockConsumer.subscribe).toHaveBeenCalledWith(
        expect.objectContaining({
          topics: expect.arrayContaining([
            'test-product.events.v1',
            'test-sku.events.v1',
            'test-category.events.v1',
            'test-catalog.events.v1',
            'test-rating.events.v1',
          ]),
        }),
      );
    });

    it('[IDX-CSM-01] should not start consumer when kafkaConsumerEnabled is false', async () => {
      const module = await createTestModule({
        config: { kafkaConsumerEnabled: false },
      });
      const disabledConsumer =
        module.get<KafkaEventConsumer>(KafkaEventConsumer);

      await disabledConsumer.onApplicationBootstrap();

      expect(mockConsumer.connect).not.toHaveBeenCalled();
      expect(mockConsumer.subscribe).not.toHaveBeenCalled();
    });
  });

  describe('[IDX-CSM-02] manual offset commit after bulk processing', () => {
    it('[IDX-CSM-02] should call eachBatch with manual commit after successful batch processing', async () => {
      await consumerService.onApplicationBootstrap();

      expect(mockConsumer.run).toHaveBeenCalledWith(
        expect.objectContaining({
          eachBatchAutoResolve: false,
          eachBatch: expect.any(Function),
        }),
      );
    });

    it('should not resolve offset and should propagate error when DLQ send rejects', async () => {
      const dlqError = new Error('Kafka broker unavailable for DLQ');
      mockProducer.send = jest.fn().mockRejectedValue(dlqError);

      const failingProductHandler = {
        handle: jest
          .fn()
          .mockRejectedValue(new Error('Handler permanent failure')),
      };

      const module = await createTestModule({
        productHandler: failingProductHandler,
      });
      const consumer = module.get<KafkaEventConsumer>(KafkaEventConsumer);

      await consumer.onApplicationBootstrap();

      const runCall = (mockConsumer.run as jest.Mock).mock.calls[0][0];
      const eachBatch = runCall.eachBatch;

      const resolveOffset = jest.fn();
      const heartbeat = jest.fn().mockResolvedValue(undefined);
      const commitOffsetsIfNecessary = jest.fn().mockResolvedValue(undefined);

      const batch = {
        topic: 'test-product.events.v1',
        partition: 0,
        highWatermark: '10',
        messages: [
          {
            offset: '101',
            key: Buffer.from('prod-1'),
            value: Buffer.from(
              JSON.stringify({
                event_id: 'evt-dlq-fail',
                event_type: 'product.created',
                aggregate_id: 'prod-1',
                version: 1,
                payload: { product_id: 'prod-1' },
              }),
            ),
          },
        ],
      };

      await expect(
        eachBatch({
          batch,
          resolveOffset,
          heartbeat,
          commitOffsetsIfNecessary,
          isRunning: () => true,
          isStale: () => false,
        }),
      ).rejects.toThrow('Kafka broker unavailable for DLQ');

      expect(resolveOffset).not.toHaveBeenCalled();
      expect(commitOffsetsIfNecessary).not.toHaveBeenCalled();
    });
  });

  describe('[IDX-CSM-03] deduplication via search-consumer-state', () => {
    it('[IDX-CSM-03] should check or write consumer-state doc with op_type create and skip on 409 conflict', async () => {
      mockClient.create = jest.fn().mockRejectedValueOnce({
        statusCode: 409,
        message: 'version conflict, document already exists',
      });
      mockClient.index = jest.fn().mockRejectedValueOnce({
        statusCode: 409,
        message: 'version conflict, document already exists',
      });

      const event = {
        event_id: 'evt-duplicate-1',
        schema_version: 1,
        event_type: 'product.updated',
        occurred_at: '2026-10-14T10:00:00Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-1',
        version: 5,
        payload: { product_id: 'prod-1' },
      };

      const result = await consumerService.processEvent(event, {});

      expect(result).toEqual(
        expect.objectContaining({
          status: 'SKIPPED',
          reason: expect.stringMatching(/DUPLICATE|ALREADY_PROCESSED/),
        }),
      );
    });
  });

  describe('[IDX-CSM-04] retry mechanism', () => {
    it('[IDX-CSM-04] should retry failed processing 3 times with 2s backoff', async () => {
      expect(EVENT_RETRY_COUNT).toBe(3);
      expect(EVENT_RETRY_BACKOFF_MS).toBe(2000);

      const failingHandler = jest
        .fn()
        .mockRejectedValueOnce(new Error('Transient error 1'))
        .mockRejectedValueOnce(new Error('Transient error 2'))
        .mockResolvedValueOnce({ status: 'PROCESSED' });

      const event = {
        event_id: 'evt-retry-1',
        schema_version: 1,
        event_type: 'product.created',
        occurred_at: '2026-10-14T10:00:00Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-1',
        version: 1,
        payload: { product_id: 'prod-1' },
      };

      const outcome = await consumerService.processWithRetry(
        event,
        {},
        failingHandler,
      );

      expect(failingHandler).toHaveBeenCalledTimes(3);
      expect(outcome.status).toBe('PROCESSED');
    });
  });

  describe('[IDX-CSM-05] DLQ routing with redacted payload', () => {
    it('[IDX-CSM-05] should route to DLQ topic after 3 failed attempts with payload redacted', async () => {
      const permanentError = new Error('Permanent processing error');
      const failingHandler = jest.fn().mockRejectedValue(permanentError);

      const event = {
        event_id: 'evt-poison-1',
        schema_version: 1,
        event_type: 'product.created',
        occurred_at: '2026-10-14T10:00:00Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-poison',
        version: 1,
        payload: {
          product_id: 'prod-poison',
          sensitive_secret: 'do-not-leak-into-dlq',
        },
      };

      const headers = {
        traceparent: Buffer.from(
          '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
        ),
        'x-request-id': Buffer.from('req-12345'),
      };

      await consumerService.processWithRetry(event, headers, failingHandler);

      expect(mockProducer.send).toHaveBeenCalled();
      const sendArgs = (mockProducer.send as jest.Mock).mock.calls[0][0];

      expect(sendArgs.topic).toBe('test-search.events.dlq.v1');
      expect(sendArgs.messages.length).toBe(1);

      const dlqMessage = sendArgs.messages[0];
      const parsedValue = JSON.parse(dlqMessage.value.toString());

      expect(parsedValue.event_id).toBe('evt-poison-1');
      expect(parsedValue.event_type).toBe('product.created');
      expect(parsedValue.aggregate_id).toBe('prod-poison');
      expect(parsedValue.version).toBe(1);
      expect(parsedValue.payload).toBeUndefined();
      expect(parsedValue.sensitive_secret).toBeUndefined();

      expect(dlqMessage.headers.traceparent).toBeDefined();
      expect(dlqMessage.headers['x-request-id']).toBeDefined();
    });
  });

  describe('[IDX-CSM-06] trace context propagation', () => {
    it('[IDX-CSM-06] should propagate traceparent header into RequestContext during processing', async () => {
      const event = {
        event_id: 'evt-trace-1',
        schema_version: 1,
        event_type: 'product.updated',
        occurred_at: '2026-10-14T10:00:00Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-1',
        version: 2,
        payload: { product_id: 'prod-1' },
      };

      const headers = {
        traceparent: Buffer.from(
          '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
        ),
      };

      let capturedTraceId: string | undefined;
      const handler = jest.fn().mockImplementation(() => {
        capturedTraceId = RequestContext.getTraceId();
        return Promise.resolve({ status: 'PROCESSED' });
      });

      await consumerService.processWithRetry(event, headers, handler);

      expect(handler).toHaveBeenCalled();
      expect(capturedTraceId).toBe('4bf92f3577b34da6a3ce929d0e0e4736');
    });
  });
});
