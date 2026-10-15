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

describe('KafkaEventConsumer & EventProcessor', () => {
  let consumerService: KafkaEventConsumer;
  let eventProcessor: EventProcessor;
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
      commitOffsets: jest.fn().mockResolvedValue(undefined),
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
    eventProcessor = module.get<EventProcessor>(EventProcessor);
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

    it('should resolve and commit offset after each successfully processed message', async () => {
      await consumerService.onApplicationBootstrap();

      const runCall = (mockConsumer.run as jest.Mock).mock.calls[0][0];
      const eachBatch = runCall.eachBatch;

      const resolveOffset = jest.fn();
      const heartbeat = jest.fn().mockResolvedValue(undefined);
      const commitOffsetsIfNecessary = jest.fn().mockResolvedValue(undefined);

      const batch = {
        topic: 'test-product.events.v1',
        partition: 2,
        highWatermark: '10',
        messages: [
          {
            offset: '100',
            key: Buffer.from('prod-1'),
            value: Buffer.from(
              JSON.stringify({
                event_id: 'evt-1',
                event_type: 'product.created',
                aggregate_id: 'prod-1',
                version: 1,
                payload: { product_id: 'prod-1' },
              }),
            ),
          },
          {
            offset: '101',
            key: Buffer.from('prod-2'),
            value: Buffer.from(
              JSON.stringify({
                event_id: 'evt-2',
                event_type: 'product.created',
                aggregate_id: 'prod-2',
                version: 1,
                payload: { product_id: 'prod-2' },
              }),
            ),
          },
        ],
      };

      await eachBatch({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
        isRunning: () => true,
        isStale: () => false,
      });

      expect(resolveOffset).toHaveBeenCalledTimes(2);
      expect(resolveOffset).toHaveBeenNthCalledWith(1, '100');
      expect(resolveOffset).toHaveBeenNthCalledWith(2, '101');

      expect(mockConsumer.commitOffsets).toHaveBeenCalledTimes(2);
      expect(mockConsumer.commitOffsets).toHaveBeenNthCalledWith(1, [
        {
          topic: 'test-product.events.v1',
          partition: 2,
          offset: '101',
        },
      ]);
      expect(mockConsumer.commitOffsets).toHaveBeenNthCalledWith(2, [
        {
          topic: 'test-product.events.v1',
          partition: 2,
          offset: '102',
        },
      ]);
      expect(commitOffsetsIfNecessary).toHaveBeenCalledTimes(1);
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
      expect(mockConsumer.commitOffsets).not.toHaveBeenCalled();
      expect(commitOffsetsIfNecessary).not.toHaveBeenCalled();
    });

    it('should propagate error and not commit offset for failed message in batch', async () => {
      mockProductHandler.handle = jest
        .fn()
        .mockResolvedValueOnce(undefined)
        .mockRejectedValue(new Error('Handler failure on msg 2'));
      mockProducer.send = jest
        .fn()
        .mockRejectedValue(new Error('DLQ publish failed'));

      await consumerService.onApplicationBootstrap();

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
            offset: '200',
            key: Buffer.from('prod-1'),
            value: Buffer.from(
              JSON.stringify({
                event_id: 'evt-succ',
                event_type: 'product.created',
                aggregate_id: 'prod-1',
                version: 1,
                payload: { product_id: 'prod-1' },
              }),
            ),
          },
          {
            offset: '201',
            key: Buffer.from('prod-2'),
            value: Buffer.from(
              JSON.stringify({
                event_id: 'evt-fail',
                event_type: 'product.created',
                aggregate_id: 'prod-2',
                version: 1,
                payload: { product_id: 'prod-2' },
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
      ).rejects.toThrow('DLQ publish failed');

      expect(resolveOffset).toHaveBeenCalledTimes(1);
      expect(resolveOffset).toHaveBeenCalledWith('200');
      expect(mockConsumer.commitOffsets).toHaveBeenCalledTimes(1);
      expect(mockConsumer.commitOffsets).toHaveBeenCalledWith([
        {
          topic: 'test-product.events.v1',
          partition: 0,
          offset: '201',
        },
      ]);
      expect(commitOffsetsIfNecessary).not.toHaveBeenCalled();
    });
  });

  describe('BLOCKER 1: dedupe marker post-handler execution and redelivery', () => {
    it('handler throws + DLQ throws -> no consumer-state doc written, error propagates; redelivery processes again', async () => {
      let attempts = 0;
      mockProductHandler.handle = jest.fn().mockImplementation(async () => {
        attempts++;
        throw new Error(`Handler fail attempt ${attempts}`);
      });
      mockProducer.send = jest
        .fn()
        .mockRejectedValue(new Error('Kafka DLQ down'));

      mockClient.get = jest.fn().mockResolvedValue({ found: false });
      mockClient.index = jest.fn().mockResolvedValue({ result: 'created' });

      const message = {
        offset: '201',
        value: Buffer.from(
          JSON.stringify({
            event_id: 'evt-blocker1-fail',
            event_type: 'product.created',
            aggregate_id: 'prod-b1',
            version: 1,
            occurred_at: '2026-10-14T10:00:00Z',
            payload: { product_id: 'prod-b1' },
          }),
        ),
      };

      // 1. Process fails on both handler and DLQ: error propagates
      await expect(
        eventProcessor.processMessage('test-product.events.v1', message, 0),
      ).rejects.toThrow('Kafka DLQ down');

      // 2. CRUCIAL: No consumer-state document written!
      expect(mockClient.index).not.toHaveBeenCalled();

      // 3. Simulating redelivery: handler now recovers
      mockProductHandler.handle = jest.fn().mockResolvedValue(undefined);
      const res = await eventProcessor.processMessage(
        'test-product.events.v1',
        message,
        0,
      );

      expect(res).toBe('2026-10-14T10:00:00.000Z');
      expect(mockProductHandler.handle).toHaveBeenCalled();
      expect(mockClient.index).toHaveBeenCalledWith(
        expect.objectContaining({
          id: 'evt-blocker1-fail',
          document: expect.objectContaining({
            event_id: 'evt-blocker1-fail',
            status: 'PROCESSED',
          }),
        }),
      );
    });
  });

  describe('BLOCKER 2: unparseable JSON / missing event_id DLQ routing', () => {
    it('should route unparseable JSON to DLQ with redacted envelope and count as failed', async () => {
      const message = {
        offset: '55',
        value: Buffer.from('invalid-json{{{'),
      };

      const result = await eventProcessor.processMessage(
        'test-product.events.v1',
        message,
        3,
      );

      expect(result).toBeNull();
      expect(mockProducer.send).toHaveBeenCalledTimes(1);

      const sendArgs = (mockProducer.send as jest.Mock).mock.calls[0][0];
      expect(sendArgs.topic).toBe('test-search.events.dlq.v1');
      expect(sendArgs.messages[0].key).toBe('test-product.events.v1-3-55');

      const payload = JSON.parse(sendArgs.messages[0].value.toString());
      expect(payload).toEqual({
        topic: 'test-product.events.v1',
        partition: 3,
        offset: '55',
        error_code: 'INVALID_EVENT',
      });
      expect(payload.payload).toBeUndefined();
    });

    it('should route event missing event_id to DLQ with redacted envelope and count as failed', async () => {
      const message = {
        offset: '56',
        value: Buffer.from(
          JSON.stringify({
            event_type: 'product.created',
            aggregate_id: 'prod-no-id',
            payload: { secret: 'redact-me' },
          }),
        ),
      };

      const result = await eventProcessor.processMessage(
        'test-product.events.v1',
        message,
        1,
      );

      expect(result).toBeNull();
      expect(mockProducer.send).toHaveBeenCalledTimes(1);

      const sendArgs = (mockProducer.send as jest.Mock).mock.calls[0][0];
      const payload = JSON.parse(sendArgs.messages[0].value.toString());
      expect(payload).toEqual({
        topic: 'test-product.events.v1',
        partition: 1,
        offset: '56',
        error_code: 'INVALID_EVENT',
      });
      expect(payload.payload).toBeUndefined();
      expect(payload.secret).toBeUndefined();
    });

    it('should propagate error if DLQ produce fails on unparseable JSON', async () => {
      mockProducer.send = jest
        .fn()
        .mockRejectedValue(new Error('DLQ unreachable'));
      const message = {
        offset: '57',
        value: Buffer.from('invalid-json{{{'),
      };

      await expect(
        eventProcessor.processMessage('test-product.events.v1', message, 0),
      ).rejects.toThrow('DLQ unreachable');
    });
  });

  describe('MAJOR 2: normalization of occurred_at (toIsoTimestamp)', () => {
    it('should normalize epoch millis numbers and string timestamps accurately', () => {
      const epochMillis = 1791979200000;
      const expectedIso = new Date(epochMillis).toISOString();

      expect(eventProcessor.toIsoTimestamp(epochMillis)).toBe(expectedIso);
      expect(eventProcessor.toIsoTimestamp(String(epochMillis))).toBe(
        expectedIso,
      );

      const isoStr = '2026-10-14T10:00:00.000Z';
      expect(eventProcessor.toIsoTimestamp(isoStr)).toBe(isoStr);
      expect(eventProcessor.toIsoTimestamp('2026-10-14T10:00:00Z')).toBe(
        '2026-10-14T10:00:00.000Z',
      );
    });

    it('should accept epoch millis in DomainEvent and pass normalized ISO string to handlers and return value', async () => {
      const epochMillis = 1791979200000;
      const expectedIso = new Date(epochMillis).toISOString();

      const message = {
        offset: '301',
        value: Buffer.from(
          JSON.stringify({
            event_id: 'evt-epoch-1',
            event_type: 'product.created',
            aggregate_id: 'prod-epoch',
            version: '2',
            occurred_at: epochMillis,
            payload: { product_id: 'prod-epoch' },
          }),
        ),
      };

      const result = await eventProcessor.processMessage(
        'test-product.events.v1',
        message,
        0,
      );

      expect(result).toBe(expectedIso);
      expect(mockProductHandler.handle).toHaveBeenCalledWith(
        expect.objectContaining({
          occurred_at: expectedIso,
          version: 2,
        }),
      );
    });
  });

  describe('Debezium outbox router contract', () => {
    const buildMessage = (event: Record<string, unknown>, offset = '400') => ({
      offset,
      value: Buffer.from(JSON.stringify(event)),
    });

    it('parses a payload delivered as a JSON string before dispatching', async () => {
      const message = buildMessage({
        event_id: 'evt-string-payload',
        event_type: 'product.created',
        aggregate_id: 'prod-str',
        version: 1,
        occurred_at: 1791979200000,
        payload: JSON.stringify({ product_id: 'prod-str', title: 'Tên' }),
      });

      await eventProcessor.processMessage('test-product.events.v1', message, 0);

      expect(mockProductHandler.handle).toHaveBeenCalledWith(
        expect.objectContaining({
          payload: { product_id: 'prod-str', title: 'Tên' },
        }),
      );
    });

    it('routes a payload that is not valid JSON to the DLQ as INVALID_EVENT', async () => {
      const message = buildMessage({
        event_id: 'evt-bad-payload',
        event_type: 'product.created',
        aggregate_id: 'prod-bad',
        version: 1,
        occurred_at: 1791979200000,
        payload: '{not json',
      });

      const result = await eventProcessor.processMessage(
        'test-product.events.v1',
        message,
        0,
      );

      expect(result).toBeNull();
      expect(mockProductHandler.handle).not.toHaveBeenCalled();
      expect(mockProducer.send).toHaveBeenCalledWith(
        expect.objectContaining({
          messages: [
            expect.objectContaining({
              value: expect.stringContaining('INVALID_EVENT'),
            }),
          ],
        }),
      );
    });

    it('sends product.skus_updated from the product topic to the SKU handler', async () => {
      mockSkuHandler.handleSkusUpdated = jest.fn().mockResolvedValue(undefined);
      const message = buildMessage({
        event_id: 'evt-skus-updated',
        event_type: 'product.skus_updated',
        aggregate_id: 'prod-1',
        version: 3,
        occurred_at: 1791979200000,
        payload: { product_id: 'prod-1', skus: [] },
      });

      await eventProcessor.processMessage('test-product.events.v1', message, 0);

      expect(mockSkuHandler.handleSkusUpdated).toHaveBeenCalledTimes(1);
      expect(mockProductHandler.handle).not.toHaveBeenCalled();
    });
  });

  describe('[IDX-CSM-03] deduplication via search-consumer-state', () => {
    it('[IDX-CSM-03] should check consumer-state doc via GET and skip duplicates', async () => {
      mockClient.get = jest.fn().mockResolvedValueOnce({
        found: true,
        _id: 'evt-duplicate-1',
      });

      const message = {
        offset: '10',
        value: Buffer.from(
          JSON.stringify({
            event_id: 'evt-duplicate-1',
            schema_version: 1,
            event_type: 'product.updated',
            occurred_at: '2026-10-14T10:00:00Z',
            aggregate_type: 'PRODUCT',
            aggregate_id: 'prod-1',
            version: 5,
            payload: { product_id: 'prod-1' },
          }),
        ),
      };

      const result = await eventProcessor.processMessage(
        'test-product.events.v1',
        message,
        0,
      );

      expect(mockClient.get).toHaveBeenCalledWith(
        expect.objectContaining({
          id: 'evt-duplicate-1',
        }),
      );
      expect(mockProductHandler.handle).not.toHaveBeenCalled();
      expect(result).toBe('2026-10-14T10:00:00.000Z');
    });
  });

  describe('[IDX-CSM-04] retry mechanism', () => {
    it('[IDX-CSM-04] should retry failed processing 3 times with backoff', async () => {
      expect(EVENT_RETRY_COUNT).toBe(3);
      expect(EVENT_RETRY_BACKOFF_MS).toBe(2000);

      mockProductHandler.handle = jest
        .fn()
        .mockRejectedValueOnce(new Error('Transient error 1'))
        .mockRejectedValueOnce(new Error('Transient error 2'))
        .mockResolvedValueOnce(undefined);

      const message = {
        offset: '20',
        value: Buffer.from(
          JSON.stringify({
            event_id: 'evt-retry-1',
            schema_version: 1,
            event_type: 'product.created',
            occurred_at: '2026-10-14T10:00:00Z',
            aggregate_type: 'PRODUCT',
            aggregate_id: 'prod-1',
            version: 1,
            payload: { product_id: 'prod-1' },
          }),
        ),
      };

      const outcome = await eventProcessor.processMessage(
        'test-product.events.v1',
        message,
        0,
      );

      expect(mockProductHandler.handle).toHaveBeenCalledTimes(3);
      expect(outcome).toBe('2026-10-14T10:00:00.000Z');
      expect(mockClient.index).toHaveBeenCalledWith(
        expect.objectContaining({
          id: 'evt-retry-1',
          document: expect.objectContaining({
            event_id: 'evt-retry-1',
            status: 'PROCESSED',
          }),
        }),
      );
    });
  });

  describe('[IDX-CSM-05] DLQ routing with redacted payload', () => {
    it('[IDX-CSM-05] should route to DLQ topic after 3 failed attempts with payload redacted', async () => {
      const permanentError = new Error('Permanent processing error');
      mockProductHandler.handle = jest.fn().mockRejectedValue(permanentError);

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

      const message = {
        offset: '30',
        headers,
        value: Buffer.from(JSON.stringify(event)),
      };

      await eventProcessor.processMessage('test-product.events.v1', message, 0);

      expect(mockProductHandler.handle).toHaveBeenCalledTimes(3);
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
      mockProductHandler.handle = jest.fn().mockImplementation(() => {
        capturedTraceId = RequestContext.getTraceId();
        return Promise.resolve(undefined);
      });

      const message = {
        offset: '40',
        headers,
        value: Buffer.from(JSON.stringify(event)),
      };

      await eventProcessor.processMessage('test-product.events.v1', message, 0);

      expect(mockProductHandler.handle).toHaveBeenCalled();
      expect(capturedTraceId).toBe('4bf92f3577b34da6a3ce929d0e0e4736');
    });
  });
});
