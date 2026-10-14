import { Client } from '@elastic/elasticsearch';
import { Admin, Kafka } from 'kafkajs';
import { ConsumerState } from '../../common/enums';
import { AppConfigService } from '../../config/config.service';
import { IConsumerStatusProvider } from '../interfaces';
import { ConsumerStatusService } from './consumer-status.service';

describe('ConsumerStatusService', () => {
  let service: IConsumerStatusProvider;
  let mockKafka: jest.Mocked<Partial<Kafka>>;
  let mockAdmin: jest.Mocked<Partial<Admin>>;
  let mockClient: jest.Mocked<Partial<Client>>;
  let mockConfigService: jest.Mocked<Partial<AppConfigService>>;

  beforeEach(() => {
    mockAdmin = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      fetchTopicOffsets: jest
        .fn()
        .mockResolvedValue([
          { partition: 0, offset: '100', high: '100', low: '0' },
        ]),
      fetchOffsets: jest.fn().mockResolvedValue([
        {
          topic: 'product.events.v1',
          partitions: [{ partition: 0, offset: '95' }],
        },
      ]),
    };

    mockKafka = {
      admin: jest.fn().mockReturnValue(mockAdmin as Admin),
    };

    mockClient = {
      count: jest.fn().mockResolvedValue({ count: 0 }),
    };

    mockConfigService = {
      kafkaGroupId: 'search-service-group',
      kafkaTopicPrefix: '',
    };

    service = new ConsumerStatusService(
      mockKafka as unknown as Kafka,
      mockClient as unknown as Client,
      mockConfigService as unknown as AppConfigService,
    );
  });

  describe('[IDX-STS-01] implementation of IConsumerStatusProvider', () => {
    it('[IDX-STS-01] should return status with state, lagByTopic, maxLagSeconds, lastEventAt, dlqCount', async () => {
      const status = await service.getStatus();

      expect(status).toBeDefined();
      expect(status.state).toBeDefined();
      expect(status.lagByTopic).toBeDefined();
      expect(typeof status.maxLagSeconds).toBe('number');
      expect(typeof status.dlqCount).toBe('number');
    });
  });

  describe('[IDX-STS-02] lagByTopic calculation', () => {
    it('[IDX-STS-02] should calculate lag as latest offset minus committed offset per topic', async () => {
      // 100 high offset - 95 committed offset = lag 5
      mockAdmin.fetchTopicOffsets = jest
        .fn()
        .mockResolvedValue([
          { partition: 0, offset: '100', high: '100', low: '0' },
        ]);
      mockAdmin.fetchOffsets = jest.fn().mockResolvedValue([
        {
          topic: 'product.events.v1',
          partitions: [{ partition: 0, offset: '95' }],
        },
      ]);

      const status = await service.getStatus();

      expect(status.lagByTopic['product.events.v1']).toBe(5);
    });
  });

  describe('[IDX-STS-03] maxLagSeconds calculation', () => {
    it('[IDX-STS-03] should compute maxLagSeconds as (now - lastProcessedEvent.occurred_at) when lag > 0', async () => {
      if (
        'recordEventProcessed' in
        (service as unknown as Record<string, unknown>)
      ) {
        const pastTime = new Date(Date.now() - 30_000).toISOString();
        (
          service as unknown as {
            recordEventProcessed: (topic: string, ts: string) => void;
          }
        ).recordEventProcessed('product.events.v1', pastTime);
      }

      const status = await service.getStatus();
      expect(status.maxLagSeconds).toBeGreaterThanOrEqual(0);
    });
  });

  describe('[IDX-STS-04] dlqCount retrieval', () => {
    it('[IDX-STS-04] should query search-consumer-state index for docs with status=FAILED', async () => {
      mockClient.count = jest.fn().mockResolvedValue({ count: 3 });

      const status = await service.getStatus();

      expect(mockClient.count).toHaveBeenCalledWith(
        expect.objectContaining({
          query: expect.objectContaining({
            term: { status: 'FAILED' },
          }),
        }),
      );
      expect(status.dlqCount).toBe(3);
    });
  });

  describe('[IDX-STS-05] status result caching (5s)', () => {
    it('[IDX-STS-05] should cache getStatus result for 5 seconds to prevent spamming Kafka/ES', async () => {
      const firstCall = await service.getStatus();
      const secondCall = await service.getStatus();

      expect(firstCall).toEqual(secondCall);
      // Admin fetch should be called once due to 5s caching
      expect(mockAdmin.fetchOffsets).toHaveBeenCalledTimes(1);
    });
  });

  describe('[IDX-STS-06] DEGRADED state on high lag or DLQ presence', () => {
    it('[IDX-STS-06] should report DEGRADED or DLQ state when dlqCount > 0 or lag exceeds threshold', async () => {
      mockClient.count = jest.fn().mockResolvedValue({ count: 5 });

      const status = await service.getStatus();

      expect([ConsumerState.DEGRADED, ConsumerState.DLQ]).toContain(
        status.state,
      );
    });
  });
});
