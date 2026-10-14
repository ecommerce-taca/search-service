import { Client, errors } from '@elastic/elasticsearch';
import { Kafka, Producer } from 'kafkajs';
import { RequestContext } from '../../common/context/request-context';
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { AppConfigService } from '../../config/config.service';
import { MetricsService } from '../../observability/metrics.service';
import {
  CatalogHandler,
  CategoryHandler,
  ProductHandler,
  RatingHandler,
  SkuHandler,
} from '../handlers';
import { ConsumerStatusService } from '../status/consumer-status.service';
import { KafkaEventConsumer } from './kafka-event.consumer';

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
      producer: jest.fn().mockReturnValue(producerMock as unknown as Producer),
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
        traceparent: Buffer.from('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'),
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
    // initialize producer
    (consumer as any).producer = producerMock;

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
        traceparent: Buffer.from('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'),
        'x-request-id': Buffer.from('req-poison-123'),
      },
      value: Buffer.from(JSON.stringify(rawEvent)),
      offset: '12',
    };

    // Override delay so test doesn't wait 4s
    jest.spyOn(consumer as any, 'delay').mockImplementation(() => Promise.resolve());

    await consumer.processMessage('test-product.events.v1', message);

    expect(productHandlerMock.handle).toHaveBeenCalledTimes(3);

    // Verify state updated to FAILED
    expect(esClientMock.index).toHaveBeenCalledWith({
      index: 'test-search-consumer-state',
      id: 'evt-poison',
      document: expect.objectContaining({
        event_id: 'evt-poison',
        status: 'FAILED',
        error_code: SearchErrorCode.SEARCH_EVENT_INVALID,
      }),
    });

    // Verify DLQ message produced with redacted payload (no sensitive data)
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
            headers: expect.objectContaining({
              'x-request-id': 'req-poison-123',
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
        traceparent: Buffer.from('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'),
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
