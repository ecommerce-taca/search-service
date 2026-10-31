import { Registry } from 'prom-client';
import { getKafkaTopic, KAFKA_TOPICS } from '../../common/constants';
import { ProductDocumentBuilder } from '../../product-index/document-builder/product-document.builder';
import { ProductHandler, DocumentMissingError } from '../handlers';
import { KafkaMessageInput } from '../interfaces';
import { EventProcessor } from './event-processor';

const CATALOG_TOPIC = getKafkaTopic(KAFKA_TOPICS.CATALOG_EVENTS, '');
const PRODUCT_TOPIC = getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, '');

function message(
  event: Record<string, unknown>,
  headers?: KafkaMessageInput['headers'],
): KafkaMessageInput {
  return {
    offset: '1',
    value: Buffer.from(JSON.stringify(event)),
    headers,
  };
}

describe('EventProcessor', () => {
  const categoryChanged = {
    event_id: 'evt-cat',
    schema_version: 1,
    event_type: 'product.category_changed',
    occurred_at: 1792031012815,
    aggregate_type: 'PRODUCT',
    aggregate_id: 'prod-1',
    version: 2,
    payload: { primary_category_id: 'cat-1' },
  };
  const created = {
    ...categoryChanged,
    event_id: 'evt-created',
    event_type: 'product.created',
    version: 1,
    payload: { title: 'x' },
  };

  let esClient: {
    get: jest.Mock;
    index: jest.Mock;
    search: jest.Mock;
    delete: jest.Mock;
  };
  let dlq: {
    publish: jest.Mock;
    publishInvalidMessage: jest.Mock;
    recordConsumerState: jest.Mock;
  };
  let productHandler: { handle: jest.Mock };
  let catalogHandler: { handle: jest.Mock };
  let registry: Registry;
  let processor: EventProcessor;

  beforeEach(() => {
    esClient = {
      get: jest.fn().mockResolvedValue({ found: false }),
      index: jest.fn().mockResolvedValue({}),
      search: jest.fn().mockResolvedValue({ hits: { hits: [] } }),
      delete: jest.fn().mockResolvedValue({}),
    };
    dlq = {
      publish: jest.fn(),
      publishInvalidMessage: jest.fn().mockResolvedValue(undefined),
      recordConsumerState: jest.fn().mockResolvedValue(undefined),
    };
    productHandler = { handle: jest.fn().mockResolvedValue(undefined) };
    catalogHandler = { handle: jest.fn() };
    registry = new Registry();
    processor = new EventProcessor(
      { esIndexPrefix: '', kafkaTopicPrefix: '' } as never,
      esClient as never,
      { getRegistry: () => registry } as never,
      dlq as never,
      productHandler as never,
      { handle: jest.fn() } as never,
      { handle: jest.fn() } as never,
      catalogHandler as never,
      { handle: jest.fn() } as never,
    );
  });

  it('reads event_id from the Debezium `id` header and aggregate_id from the key', async () => {
    // Hình dạng value do outbox-connector.json của Catalog sinh ra: không có event_id/aggregate_id.
    const debeziumValue = {
      payload: JSON.stringify({ product_id: 'prod-9', title: 'x' }),
      version: 3,
      event_type: 'product.created',
      schema_version: 1,
      occurred_at: 1792031012815,
      aggregate_type: 'PRODUCT',
    };
    await processor.processMessage(PRODUCT_TOPIC, {
      offset: '7',
      key: Buffer.from(JSON.stringify('prod-9')),
      value: Buffer.from(JSON.stringify(debeziumValue)),
      headers: { id: Buffer.from(JSON.stringify('evt-debezium-1')) },
    });

    expect(dlq.publishInvalidMessage).not.toHaveBeenCalled();
    expect(productHandler.handle).toHaveBeenCalledWith(
      expect.objectContaining({
        event_id: 'evt-debezium-1',
        aggregate_id: 'prod-9',
        payload: { product_id: 'prod-9', title: 'x' },
      }),
    );
  });

  it('still sends to the DLQ when neither body nor header has an event_id', async () => {
    const noId: Record<string, unknown> = { ...created };
    delete noId.event_id;
    await processor.processMessage(PRODUCT_TOPIC, message(noId));
    expect(dlq.publishInvalidMessage).toHaveBeenCalled();
    expect(productHandler.handle).not.toHaveBeenCalled();
  });

  it('parks an event whose product is not indexed instead of sending it to the DLQ', async () => {
    catalogHandler.handle.mockRejectedValue(new DocumentMissingError('prod-1'));

    await processor.processMessage(CATALOG_TOPIC, message(categoryChanged));

    expect(catalogHandler.handle).toHaveBeenCalledTimes(1);
    expect(dlq.publish).not.toHaveBeenCalled();
    expect(esClient.index).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'pending:evt-cat',
        document: expect.objectContaining({
          status: 'PENDING',
          aggregate_id: 'prod-1',
          topic: CATALOG_TOPIC,
        }),
      }),
    );
  });

  it('replays parked events after the product document is created', async () => {
    esClient.search.mockResolvedValue({
      hits: {
        hits: [
          {
            _id: 'pending:evt-cat',
            _source: { topic: CATALOG_TOPIC, event: categoryChanged },
          },
        ],
      },
    });
    catalogHandler.handle.mockResolvedValue(undefined);

    await processor.processMessage(PRODUCT_TOPIC, message(created));

    expect(productHandler.handle).toHaveBeenCalledTimes(1);
    expect(catalogHandler.handle).toHaveBeenCalledWith(categoryChanged);
    expect(dlq.recordConsumerState).toHaveBeenCalledWith(
      categoryChanged,
      'PROCESSED',
    );
    expect(esClient.delete).toHaveBeenCalledWith({
      index: 'search-consumer-state',
      id: 'pending:evt-cat',
    });
  });

  it('keeps parked events when the product is still missing', async () => {
    esClient.search.mockResolvedValue({
      hits: {
        hits: [
          {
            _id: 'pending:evt-cat',
            _source: { topic: CATALOG_TOPIC, event: categoryChanged },
          },
        ],
      },
    });
    catalogHandler.handle.mockRejectedValue(new DocumentMissingError('prod-1'));

    await processor.processMessage(PRODUCT_TOPIC, message(created));

    expect(esClient.delete).not.toHaveBeenCalled();
    expect(dlq.publish).not.toHaveBeenCalled();
  });

  describe('DLQ routing on retry exhaustion (AC5)', () => {
    it('threads partition and message.offset down to DlqPublisher.publish when retries are exhausted', async () => {
      const permanentError = new Error('Database down');
      productHandler.handle.mockRejectedValue(permanentError);

      const msg = {
        offset: '54321',
        value: Buffer.from(JSON.stringify(created)),
      };

      await processor.processMessage(PRODUCT_TOPIC, msg, 4);

      expect(productHandler.handle).toHaveBeenCalledTimes(3);
      expect(dlq.publish).toHaveBeenCalledTimes(1);
      expect(dlq.publish).toHaveBeenCalledWith(
        PRODUCT_TOPIC,
        expect.objectContaining({
          event_id: 'evt-created',
          aggregate_id: 'prod-1',
        }),
        permanentError,
        expect.anything(),
        expect.objectContaining({
          partition: 4,
          offset: '54321',
          attempts: 3,
        }),
      );
    });
  });

  describe('replay_of_event_id handling', () => {
    const replayedEvent = {
      event_id: 'evt-replay-100',
      schema_version: 1,
      event_type: 'product.created',
      occurred_at: '2026-10-14T10:00:00Z',
      aggregate_type: 'PRODUCT',
      aggregate_id: 'prod-1',
      version: 2,
      payload: { product_id: 'prod-1', title: 'Replayed item' },
      replay_of_event_id: 'evt-orig-050',
    };

    it('records replay_of_event_id in consumer-state when processed successfully', async () => {
      await processor.processMessage(PRODUCT_TOPIC, message(replayedEvent));

      expect(productHandler.handle).toHaveBeenCalledTimes(1);
      expect(dlq.recordConsumerState).toHaveBeenCalledWith(
        expect.objectContaining({
          event_id: 'evt-replay-100',
          replay_of_event_id: 'evt-orig-050',
        }),
        'PROCESSED',
      );
    });

    it('logs one structured line with event="event.replayed", event_id, replay_of_event_id, and trace_id without payload', async () => {
      const logSpy = jest.spyOn(processor['logger'], 'log');
      const traceId = '4bf92f3577b34da6a3ce929d0e0e4736';
      const headers = {
        traceparent: Buffer.from(`00-${traceId}-00f067aa0ba902b7-01`),
      };

      await processor.processMessage(
        PRODUCT_TOPIC,
        message(replayedEvent, headers),
      );

      const replayedLogs = logSpy.mock.calls.filter((call) => {
        const arg = call[0];
        return (
          typeof arg === 'object' &&
          arg !== null &&
          (arg as { event?: string }).event === 'event.replayed'
        );
      });
      expect(replayedLogs).toHaveLength(1);
      const logEntry = replayedLogs[0][0] as Record<string, unknown>;
      expect(logEntry).toEqual({
        event: 'event.replayed',
        event_id: 'evt-replay-100',
        replay_of_event_id: 'evt-orig-050',
        trace_id: traceId,
      });
      expect(logEntry.payload).toBeUndefined();
      expect(JSON.stringify(logEntry)).not.toContain('Replayed item');
    });

    it('increments search_events_replayed_total Prometheus counter with topic label only once', async () => {
      await processor.processMessage(PRODUCT_TOPIC, message(replayedEvent));

      const counter = registry.getSingleMetric('search_events_replayed_total');
      expect(counter).toBeDefined();
      const metricData = await counter!.get();
      expect(metricData.values).toHaveLength(1);
      expect(metricData.values[0].labels).toEqual({ topic: PRODUCT_TOPIC });
      expect(metricData.values[0].value).toBe(1);
    });

    it('skips a replay as duplicate when its event_id has already been processed', async () => {
      esClient.get.mockImplementation(async ({ index, id }) => {
        if (index === 'search-consumer-state' && id === 'evt-replay-100') {
          return { found: true };
        }
        return { found: false };
      });
      const logSpy = jest.spyOn(processor['logger'], 'log');

      await processor.processMessage(PRODUCT_TOPIC, message(replayedEvent));

      expect(productHandler.handle).not.toHaveBeenCalled();
      expect(dlq.recordConsumerState).not.toHaveBeenCalled();
      const replayedLogs = logSpy.mock.calls.filter(
        (call) => (call[0] as { event?: string })?.event === 'event.replayed',
      );
      expect(replayedLogs).toHaveLength(0);

      const counter = registry.getSingleMetric('search_events_replayed_total');
      if (counter) {
        const metricData = await counter.get();
        expect(metricData.values).toHaveLength(0);
      }
    });

    it('does not lower the indexed version when a replay has an older version', async () => {
      // Current document in Elasticsearch has source_version: 5
      esClient.get.mockImplementation(async ({ index, id }) => {
        if (index === 'products-read' && id === 'prod-1') {
          return {
            found: true,
            _source: {
              product_id: 'prod-1',
              source_version: 5,
              title: 'Current title v5',
              visibility_status: 'PUBLISHED',
            },
          };
        }
        return { found: false };
      });

      const mockWriter = {
        bulkUpsert: jest.fn().mockResolvedValue(undefined),
        bulkPartialUpdate: jest.fn().mockResolvedValue(undefined),
        getCurrentVersions: jest.fn().mockResolvedValue(new Map()),
      };
      const realProductHandler = new ProductHandler(
        esClient as never,
        { esIndexPrefix: '' } as never,
        new ProductDocumentBuilder(),
        mockWriter as never,
        {
          getMany: jest.fn().mockResolvedValue(new Map()),
          resolvePathIds: jest.fn().mockResolvedValue([]),
        } as never,
      );

      const customProcessor = new EventProcessor(
        { esIndexPrefix: '', kafkaTopicPrefix: '' } as never,
        esClient as never,
        { getRegistry: () => registry } as never,
        dlq as never,
        realProductHandler as never,
        { handle: jest.fn() } as never,
        { handle: jest.fn() } as never,
        catalogHandler as never,
        { handle: jest.fn() } as never,
      );

      // Replayed event has older version: 2 < 5
      const olderReplayEvent = {
        ...replayedEvent,
        event_id: 'evt-replay-old',
        version: 2,
      };

      await customProcessor.processMessage(
        PRODUCT_TOPIC,
        message(olderReplayEvent),
      );

      // The writer was NOT called to upsert or update the document with an older version
      expect(mockWriter.bulkUpsert).not.toHaveBeenCalled();
      expect(mockWriter.bulkPartialUpdate).not.toHaveBeenCalled();
    });

    it('events without replay_of_event_id behave exactly as before', async () => {
      const logSpy = jest.spyOn(processor['logger'], 'log');
      const normalEvent = {
        event_id: 'evt-normal-100',
        schema_version: 1,
        event_type: 'product.created',
        occurred_at: '2026-10-14T10:00:00Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-1',
        version: 1,
        payload: { product_id: 'prod-1' },
      };

      await processor.processMessage(PRODUCT_TOPIC, message(normalEvent));

      expect(productHandler.handle).toHaveBeenCalledTimes(1);
      expect(dlq.recordConsumerState).toHaveBeenCalledWith(
        expect.objectContaining({ event_id: 'evt-normal-100' }),
        'PROCESSED',
      );
      const dlqCall = dlq.recordConsumerState.mock.calls[0][0];
      expect(dlqCall.replay_of_event_id).toBeUndefined();

      const replayedLogs = logSpy.mock.calls.filter(
        (call) => (call[0] as { event?: string })?.event === 'event.replayed',
      );
      expect(replayedLogs).toHaveLength(0);

      const counter = registry.getSingleMetric('search_events_replayed_total');
      if (counter) {
        const metricData = await counter.get();
        expect(metricData.values).toHaveLength(0);
      }
    });
  });
});
