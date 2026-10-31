import { Client } from '@elastic/elasticsearch';
import { IIndexWriter } from '../../product-index/interfaces';
import { DomainEvent } from '../interfaces';
import { RatingEventHandler } from './rating.handler';

describe('RatingEventHandler', () => {
  let handler: RatingEventHandler;
  let mockWriter: jest.Mocked<IIndexWriter>;
  let mockClient: jest.Mocked<Partial<Client>>;

  beforeEach(() => {
    mockWriter = {
      bulkUpsert: jest.fn().mockResolvedValue(undefined),
      bulkPartialUpdate: jest.fn().mockResolvedValue(undefined),
      getCurrentVersions: jest.fn().mockResolvedValue(new Map()),
      updateShopSnapshot: jest.fn().mockResolvedValue(0),
    };

    mockClient = {
      get: jest.fn().mockResolvedValue({
        found: false,
      }),
      index: jest.fn().mockResolvedValue({ result: 'created' }),
    };

    handler = new RatingEventHandler(
      mockWriter,
      mockClient as unknown as Client,
    );
  });

  describe('[IDX-RAT-01] updating rating fields on product', () => {
    it('[IDX-RAT-01] should patch rating_avg and rating_count on product document', async () => {
      const occurredAt = '2026-10-14T12:00:00.000Z';
      const event: DomainEvent = {
        event_id: 'evt-rat-1',
        schema_version: 1,
        event_type: 'rating.aggregate.updated',
        occurred_at: occurredAt,
        aggregate_type: 'RATING',
        aggregate_id: 'prod-001',
        version: 1,
        payload: {
          product_id: 'prod-001',
          avg: 4.8,
          count: 120,
        },
      };

      await handler.handle(event);

      // Verify partial update called on products
      expect(mockWriter.bulkPartialUpdate).toHaveBeenCalledWith(
        expect.any(String),
        [
          expect.objectContaining({
            id: 'prod-001',
            doc: expect.objectContaining({
              rating_avg: 4.8,
              rating_count: 120,
            }),
          }),
        ],
      );

      // Verify state doc updated with _id: rating:prod-001 and source_version = occurred_at epoch ms
      const expectedEpochMs = new Date(occurredAt).getTime();
      expect(mockClient.index).toHaveBeenCalledWith(
        expect.objectContaining({
          id: 'rating:prod-001',
          document: expect.objectContaining({
            source_version: expectedEpochMs,
          }),
        }),
      );
    });
  });

  describe('[IDX-RAT-02] ordering guard with consumer-state', () => {
    it('[IDX-RAT-02] should skip older rating event when occurred_at epoch ms is older than stored rating version', async () => {
      const storedEpochMs = new Date('2026-10-14T12:00:00.000Z').getTime();

      // State doc already has higher epoch ms
      mockClient.get = jest.fn().mockResolvedValue({
        found: true,
        _source: {
          source_version: storedEpochMs,
        },
      });

      const olderEvent: DomainEvent = {
        event_id: 'evt-rat-old',
        schema_version: 1,
        event_type: 'rating.aggregate.updated',
        occurred_at: '2026-10-14T11:00:00.000Z', // 1 hour earlier
        aggregate_type: 'RATING',
        aggregate_id: 'prod-001',
        version: 1,
        payload: {
          product_id: 'prod-001',
          avg: 3.5,
          count: 50,
        },
      };

      await handler.handle(olderEvent);

      // Should skip updating products
      expect(mockWriter.bulkPartialUpdate).not.toHaveBeenCalled();
      expect(mockClient.index).not.toHaveBeenCalled();
    });
  });
});
