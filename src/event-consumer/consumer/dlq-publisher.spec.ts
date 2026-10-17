import { RequestContextData } from '../../common/context/request-context';
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { DomainEvent } from '../interfaces';
import { DlqPublisher } from './dlq-publisher';

describe('DlqPublisher', () => {
  let publisher: DlqPublisher;
  let mockProducer: {
    connect: jest.Mock;
    disconnect: jest.Mock;
    send: jest.Mock;
  };
  let mockKafka: {
    producer: jest.Mock;
  };
  let mockEsClient: {
    index: jest.Mock;
  };
  let mockConfigService: {
    esIndexPrefix: string;
    kafkaTopicPrefix: string;
  };

  const sampleEvent: DomainEvent<{ secret: string; foo: string }> = {
    event_id: 'evt-test-123',
    schema_version: 1,
    event_type: 'product.created',
    occurred_at: '2026-10-17T12:00:00Z',
    aggregate_type: 'PRODUCT',
    aggregate_id: 'prod-456',
    version: 3,
    payload: {
      secret: 'super-sensitive-data',
      foo: 'bar',
    },
  };

  const ctx: RequestContextData = {
    traceId: '4bf92f3577b34da6a3ce929d0e0e4736',
    spanId: '00f067aa0ba902b7',
    traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
    requestId: 'req-abc-789',
    actor: { roles: [], permissions: [], mfaStepUp: false },
  };

  beforeEach(() => {
    mockProducer = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      send: jest.fn().mockResolvedValue([]),
    };
    mockKafka = {
      producer: jest.fn().mockReturnValue(mockProducer),
    };
    mockEsClient = {
      index: jest.fn().mockResolvedValue({ result: 'created' }),
    };
    mockConfigService = {
      esIndexPrefix: 'test-',
      kafkaTopicPrefix: 'test-',
    };

    publisher = new DlqPublisher(
      mockConfigService as never,
      mockEsClient as never,
      mockKafka as never,
    );
  });

  describe('publish (failed event DLQ routing)', () => {
    it('carries replay metadata and existing envelope fields in DLQ value (AC1)', async () => {
      const error = new SearchException(
        SearchErrorCode.SEARCH_INTERNAL_ERROR,
        'Something broke',
      );

      await publisher.publish('product.events.v1', sampleEvent, error, ctx, {
        partition: 2,
        offset: '105',
        attempts: 3,
        failed_at: '2026-10-17T15:00:00.000Z',
      });

      expect(mockProducer.send).toHaveBeenCalledTimes(1);
      const sendArgs = mockProducer.send.mock.calls[0][0];
      expect(sendArgs.topic).toBe('test-search.events.dlq.v1');
      expect(sendArgs.messages).toHaveLength(1);

      const message = sendArgs.messages[0];
      const envelope = JSON.parse(message.value.toString());

      expect(envelope).toEqual({
        event_id: 'evt-test-123',
        event_type: 'product.created',
        aggregate_id: 'prod-456',
        version: 3,
        error_code: SearchErrorCode.SEARCH_INTERNAL_ERROR,
        source_topic: 'product.events.v1',
        source_partition: 2,
        source_offset: '105',
        attempts: 3,
        failed_at: '2026-10-17T15:00:00.000Z',
      });
    });

    it('generates an ISO-8601 UTC timestamp for failed_at when omitted', async () => {
      const error = new Error('Generic error');
      const before = Date.now();

      await publisher.publish('sku.events.v1', sampleEvent, error, ctx, {
        partition: 1,
        offset: '50',
        attempts: 2,
      });

      const message = mockProducer.send.mock.calls[0][0].messages[0];
      const envelope = JSON.parse(message.value.toString());
      const after = Date.now();

      expect(envelope.failed_at).toBeDefined();
      expect(new Date(envelope.failed_at).toISOString()).toBe(
        envelope.failed_at,
      );
      const failedAtMs = new Date(envelope.failed_at).getTime();
      expect(failedAtMs).toBeGreaterThanOrEqual(before);
      expect(failedAtMs).toBeLessThanOrEqual(after);
    });

    it('redacts payload and never contains any payload fields, asserting exact key set (AC2)', async () => {
      const error = new Error('Processing failed');

      await publisher.publish('product.events.v1', sampleEvent, error, ctx, {
        partition: 0,
        offset: '42',
        attempts: 3,
      });

      const message = mockProducer.send.mock.calls[0][0].messages[0];
      const envelope = JSON.parse(message.value.toString());

      // Asserts that neither payload nor its nested keys leak
      expect(envelope.payload).toBeUndefined();
      expect(envelope.secret).toBeUndefined();
      expect(envelope.foo).toBeUndefined();

      // Asserts exact key set
      const expectedKeys = [
        'aggregate_id',
        'attempts',
        'error_code',
        'event_id',
        'event_type',
        'failed_at',
        'source_offset',
        'source_partition',
        'source_topic',
        'version',
      ].sort();

      expect(Object.keys(envelope).sort()).toEqual(expectedKeys);
    });

    it('forwards traceparent and x-request-id Kafka headers (AC3)', async () => {
      await publisher.publish(
        'product.events.v1',
        sampleEvent,
        new Error(),
        ctx,
        {
          partition: 0,
          offset: '1',
          attempts: 1,
        },
      );

      const message = mockProducer.send.mock.calls[0][0].messages[0];
      expect(message.headers).toEqual({
        traceparent: ctx.traceparent,
        'x-request-id': ctx.requestId,
      });
    });

    it('uses aggregate_id as key, or falls back to event_id', async () => {
      await publisher.publish(
        'product.events.v1',
        sampleEvent,
        new Error(),
        ctx,
      );
      expect(mockProducer.send.mock.calls[0][0].messages[0].key).toBe(
        'prod-456',
      );

      const noAggregateEvent = { ...sampleEvent, aggregate_id: '' };
      await publisher.publish(
        'product.events.v1',
        noAggregateEvent,
        new Error(),
        ctx,
      );
      expect(mockProducer.send.mock.calls[1][0].messages[0].key).toBe(
        'evt-test-123',
      );
    });

    it('records consumer state in ES with DLQ status', async () => {
      await publisher.publish(
        'product.events.v1',
        sampleEvent,
        new Error(),
        ctx,
      );

      expect(mockEsClient.index).toHaveBeenCalledWith(
        expect.objectContaining({
          index: 'test-search-consumer-state',
          id: 'evt-test-123',
          document: expect.objectContaining({
            event_id: 'evt-test-123',
            aggregate_id: 'prod-456',
            status: 'DLQ',
          }),
        }),
      );
    });

    it('rethrows when producer send fails', async () => {
      mockProducer.send.mockRejectedValue(new Error('Broker unreachable'));

      await expect(
        publisher.publish('product.events.v1', sampleEvent, new Error(), ctx),
      ).rejects.toThrow('Broker unreachable');
    });
  });

  describe('publishInvalidMessage (AC4 - unchanged behavior)', () => {
    it('publishes invalid event with topic, partition, offset and INVALID_EVENT error code', async () => {
      await publisher.publishInvalidMessage(
        'product.events.v1',
        3,
        '2048',
        ctx,
      );

      expect(mockProducer.send).toHaveBeenCalledTimes(1);
      const sendArgs = mockProducer.send.mock.calls[0][0];
      expect(sendArgs.topic).toBe('test-search.events.dlq.v1');

      const message = sendArgs.messages[0];
      expect(message.key).toBe('product.events.v1-3-2048');
      expect(JSON.parse(message.value.toString())).toEqual({
        topic: 'product.events.v1',
        partition: 3,
        offset: '2048',
        error_code: 'INVALID_EVENT',
      });
      expect(message.headers).toEqual({
        traceparent: ctx.traceparent,
        'x-request-id': ctx.requestId,
      });
    });
  });

  describe('recordConsumerState', () => {
    it('includes replay_of_event_id in consumer state document when present on event', async () => {
      const event: DomainEvent = {
        event_id: 'evt-replay-1',
        schema_version: 1,
        event_type: 'product.created',
        occurred_at: '2026-10-14T10:00:00Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-1',
        version: 2,
        payload: { product_id: 'prod-1' },
        replay_of_event_id: 'evt-orig-1',
      };

      await publisher.recordConsumerState(event, 'PROCESSED');

      expect(mockEsClient.index).toHaveBeenCalledWith(
        expect.objectContaining({
          index: 'test-search-consumer-state',
          id: 'evt-replay-1',
          document: expect.objectContaining({
            event_id: 'evt-replay-1',
            aggregate_id: 'prod-1',
            source_version: 2,
            status: 'PROCESSED',
            replay_of_event_id: 'evt-orig-1',
          }),
        }),
      );
    });

    it('omits replay_of_event_id when event does not have it', async () => {
      const event: DomainEvent = {
        event_id: 'evt-normal-1',
        schema_version: 1,
        event_type: 'product.created',
        occurred_at: '2026-10-14T10:00:00Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-1',
        version: 1,
        payload: { product_id: 'prod-1' },
      };

      await publisher.recordConsumerState(event, 'PROCESSED');

      expect(mockEsClient.index).toHaveBeenCalledWith(
        expect.objectContaining({
          index: 'test-search-consumer-state',
          id: 'evt-normal-1',
          document: expect.objectContaining({
            event_id: 'evt-normal-1',
            aggregate_id: 'prod-1',
            source_version: 1,
            status: 'PROCESSED',
          }),
        }),
      );
      const doc = mockEsClient.index.mock.calls[0][0].document;
      expect(doc.replay_of_event_id).toBeUndefined();
    });
  });
});
