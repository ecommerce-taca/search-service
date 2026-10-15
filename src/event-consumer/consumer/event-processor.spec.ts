import { Registry } from 'prom-client';
import { getKafkaTopic, KAFKA_TOPICS } from '../../common/constants';
import { DocumentMissingError } from '../handlers';
import { EventProcessor } from './event-processor';

const CATALOG_TOPIC = getKafkaTopic(KAFKA_TOPICS.CATALOG_EVENTS, '');
const PRODUCT_TOPIC = getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, '');

function message(event: Record<string, unknown>) {
  return { offset: '1', value: Buffer.from(JSON.stringify(event)) };
}

describe('EventProcessor out-of-order handling', () => {
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
  let dlq: { publish: jest.Mock; recordConsumerState: jest.Mock };
  let productHandler: { handle: jest.Mock };
  let catalogHandler: { handle: jest.Mock };
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
      recordConsumerState: jest.fn().mockResolvedValue(undefined),
    };
    productHandler = { handle: jest.fn().mockResolvedValue(undefined) };
    catalogHandler = { handle: jest.fn() };
    processor = new EventProcessor(
      { esIndexPrefix: '', kafkaTopicPrefix: '' } as never,
      esClient as never,
      { getRegistry: () => new Registry() } as never,
      dlq as never,
      productHandler as never,
      { handle: jest.fn() } as never,
      { handle: jest.fn() } as never,
      catalogHandler as never,
      { handle: jest.fn() } as never,
    );
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
});
