import { Client, errors } from '@elastic/elasticsearch';
import { Kafka, Producer } from 'kafkajs';
import { RequestContext } from '../common/context/request-context';
import { ConsumerState } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';
import { MetricsService } from '../observability/metrics.service';
import { KafkaEventConsumer } from './consumer/kafka-event.consumer';
import {
  CatalogHandler,
  CategoryHandler,
  ProductHandler,
  RatingHandler,
  SkuHandler,
} from './handlers';
import { ConsumerStatusService } from './status/consumer-status.service';

describe('EventConsumerModule Unit Tests', () => {
  describe('ConsumerStatusService', () => {
    let service: ConsumerStatusService;
    let clientMock: jest.Mocked<Partial<Client>>;
    let configServiceMock: jest.Mocked<Partial<AppConfigService>>;

    beforeEach(() => {
      clientMock = {
        count: jest.fn().mockResolvedValue({ count: 0 }),
      };
      configServiceMock = {
        esIndexPrefix: 'test-',
        kafkaTopicPrefix: '',
        kafkaConsumerEnabled: true,
        kafkaGroupId: 'test-group',
      };

      service = new ConsumerStatusService(
        clientMock as unknown as Client,
        configServiceMock as unknown as AppConfigService,
      );
    });

    it('returns RUNNING status when running, no DLQ docs, and no lag', async () => {
      const status = await service.getStatus();
      expect(status.state).toBe(ConsumerState.RUNNING);
      expect(status.dlqCount).toBe(0);
      expect(status.maxLagSeconds).toBe(0);
    });

    it('returns DLQ status when DLQ count > 0', async () => {
      (clientMock.count as jest.Mock).mockResolvedValue({ count: 3 });

      const status = await service.getStatus();
      expect(status.state).toBe(ConsumerState.DLQ);
      expect(status.dlqCount).toBe(3);
    });

    it('returns DEGRADED status when lag > 0', async () => {
      service.setTopicLag('product.events.v1', 50);
      service.recordEventProcessed(
        'product.events.v1',
        new Date(Date.now() - 70000).toISOString(),
        50,
      );

      const status = await service.getStatus();
      expect(status.state).toBe(ConsumerState.DEGRADED);
      expect(status.maxLagSeconds).toBeGreaterThanOrEqual(60);
    });

    it('caches status for 5s', async () => {
      const firstStatus = await service.getStatus();
      (clientMock.count as jest.Mock).mockResolvedValue({ count: 99 });

      const secondStatus = await service.getStatus();
      expect(secondStatus.dlqCount).toBe(firstStatus.dlqCount);
      expect(clientMock.count).toHaveBeenCalledTimes(1);
    });
  });

  describe('KafkaEventConsumer', () => {
    let consumer: KafkaEventConsumer;
    let esClientMock: jest.Mocked<Partial<Client>>;
    let configServiceMock: jest.Mocked<Partial<AppConfigService>>;
    let metricsServiceMock: MetricsService;
    let productHandlerMock: jest.Mocked<Partial<ProductHandler>>;
    let skuHandlerMock: jest.Mocked<Partial<SkuHandler>>;
    let categoryHandlerMock: jest.Mocked<Partial<CategoryHandler>>;
    let catalogHandlerMock: jest.Mocked<Partial<CatalogHandler>>;
    let ratingHandlerMock: jest.Mocked<Partial<RatingHandler>>;
    let consumerStatusServiceMock: jest.Mocked<Partial<ConsumerStatusService>>;
    let kafkaMock: jest.Mocked<Partial<Kafka>>;
    let producerMock: jest.Mocked<Partial<Producer>>;

    beforeEach(() => {
      esClientMock = {
        create: jest.fn().mockResolvedValue({ result: 'created' }),
        index: jest.fn().mockResolvedValue({ result: 'created' }),
      };

      configServiceMock = {
        esIndexPrefix: 'test-',
        kafkaTopicPrefix: 'test-',
        kafkaConsumerEnabled: true,
        kafkaGroupId: 'test-group',
      };

      metricsServiceMock = new MetricsService();

      productHandlerMock = {
        handle: jest.fn().mockResolvedValue(undefined),
      };
      skuHandlerMock = {
        handle: jest.fn().mockResolvedValue(undefined),
      };
      categoryHandlerMock = {
        handle: jest.fn().mockResolvedValue(undefined),
      };
      catalogHandlerMock = {
        handle: jest.fn().mockResolvedValue(undefined),
      };
      ratingHandlerMock = {
        handle: jest.fn().mockResolvedValue(undefined),
      };

      consumerStatusServiceMock = {
        setConsumerState: jest.fn(),
        recordEventProcessed: jest.fn(),
        setTopicLag: jest.fn(),
      };

      producerMock = {
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        send: jest.fn().mockResolvedValue([]),
      };

      kafkaMock = {
        producer: jest
          .fn()
          .mockReturnValue(producerMock as unknown as Producer),
        consumer: jest.fn(),
      };

      consumer = new KafkaEventConsumer(
        configServiceMock as unknown as AppConfigService,
        esClientMock as unknown as Client,
        metricsServiceMock,
        productHandlerMock as unknown as ProductHandler,
        skuHandlerMock as unknown as SkuHandler,
        categoryHandlerMock as unknown as CategoryHandler,
        catalogHandlerMock as unknown as CatalogHandler,
        ratingHandlerMock as unknown as RatingHandler,
        consumerStatusServiceMock as unknown as ConsumerStatusService,
        kafkaMock as unknown as Kafka,
      );
    });

    it('successfully processes product event and registers in consumer-state', async () => {
      const rawEvent = {
        event_id: 'evt-1',
        schema_version: 1,
        event_type: 'product.created',
        occurred_at: '2026-10-14T10:00:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-1',
        version: 1,
        payload: { product_id: 'prod-1' },
      };

      const message = {
        headers: {
          traceparent: Buffer.from(
            '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
          ),
          'x-request-id': Buffer.from('req-123'),
        },
        value: Buffer.from(JSON.stringify(rawEvent)),
        offset: '10',
      };

      await consumer.processMessage('test-product.events.v1', message);

      expect(esClientMock.create).toHaveBeenCalledWith({
        index: 'test-search-consumer-state',
        id: 'evt-1',
        document: expect.objectContaining({
          event_id: 'evt-1',
          aggregate_id: 'prod-1',
          status: 'PROCESSED',
        }),
      });
      expect(productHandlerMock.handle).toHaveBeenCalledWith(rawEvent);
    });

    it('skips duplicate event when consumer-state create returns 409 conflict', async () => {
      const rawEvent = {
        event_id: 'evt-dup',
        event_type: 'product.created',
        aggregate_id: 'prod-1',
        version: 1,
      };

      const conflictError = new errors.ResponseError({
        body: {},
        statusCode: 409,
        headers: {},
        warnings: [],
      } as unknown as ConstructorParameters<typeof errors.ResponseError>[0]);
      (esClientMock.create as jest.Mock).mockRejectedValue(conflictError);

      const message = {
        value: Buffer.from(JSON.stringify(rawEvent)),
        offset: '11',
      };

      await consumer.processMessage('test-product.events.v1', message);

      expect(productHandlerMock.handle).not.toHaveBeenCalled();
    });

    it('retries 3 times then produces redacted envelope to DLQ on failure', async () => {
      Object.defineProperty(consumer, 'producer', {
        value: producerMock,
        writable: true,
      });

      const rawEvent = {
        event_id: 'evt-poison',
        schema_version: 1,
        event_type: 'product.created',
        occurred_at: '2026-10-14T10:00:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-poison',
        version: 1,
        payload: { sensitive_token: 'secret', credit_card: '1234' },
      };

      const failure = new SearchException(
        SearchErrorCode.SEARCH_EVENT_INVALID,
        'Invalid format',
      );
      (productHandlerMock.handle as jest.Mock).mockRejectedValue(failure);

      const message = {
        headers: {
          traceparent: Buffer.from(
            '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
          ),
          'x-request-id': Buffer.from('req-poison-123'),
        },
        value: Buffer.from(JSON.stringify(rawEvent)),
        offset: '12',
      };

      jest
        .spyOn(
          consumer as unknown as { delay: (ms: number) => Promise<void> },
          'delay',
        )
        .mockImplementation(() => Promise.resolve());

      await consumer.processMessage('test-product.events.v1', message);

      expect(productHandlerMock.handle).toHaveBeenCalledTimes(3);

      expect(esClientMock.index).toHaveBeenCalledWith({
        index: 'test-search-consumer-state',
        id: 'evt-poison',
        document: expect.objectContaining({
          event_id: 'evt-poison',
          status: 'FAILED',
          error_code: SearchErrorCode.SEARCH_EVENT_INVALID,
        }),
      });

      expect(producerMock.send).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: 'test-search.events.dlq.v1',
          messages: [
            expect.objectContaining({
              value: JSON.stringify({
                event_id: 'evt-poison',
                event_type: 'product.created',
                aggregate_id: 'prod-poison',
                version: 1,
                error_code: SearchErrorCode.SEARCH_EVENT_INVALID,
              }),
            }),
          ],
        }),
      );
    });

    it('runs within RequestContext preserving trace and request_id', async () => {
      let capturedTraceId: string | undefined;
      let capturedRequestId: string | undefined;

      (productHandlerMock.handle as jest.Mock).mockImplementation(async () => {
        capturedTraceId = RequestContext.getTraceId();
        capturedRequestId = RequestContext.getRequestId();
      });

      const rawEvent = {
        event_id: 'evt-trace',
        event_type: 'product.created',
        aggregate_id: 'prod-1',
        version: 1,
      };

      const message = {
        headers: {
          traceparent: Buffer.from(
            '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
          ),
          'x-request-id': Buffer.from('my-custom-req-id'),
        },
        value: Buffer.from(JSON.stringify(rawEvent)),
        offset: '15',
      };

      await consumer.processMessage('test-product.events.v1', message);

      expect(capturedTraceId).toBe('4bf92f3577b34da6a3ce929d0e0e4736');
      expect(capturedRequestId).toBe('my-custom-req-id');
    });
  });
});
