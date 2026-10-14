import { Client } from '@elastic/elasticsearch';
import { ConsumerState } from '../../common/enums';
import { AppConfigService } from '../../config/config.service';
import { ConsumerStatusService } from './consumer-status.service';

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

  it('returns PAUSED status when consumer is disabled', async () => {
    service.setConsumerState(false);
    const status = await service.getStatus();
    expect(status.state).toBe(ConsumerState.PAUSED);
  });

  it('caches status for 5s', async () => {
    const firstStatus = await service.getStatus();
    (clientMock.count as jest.Mock).mockResolvedValue({ count: 99 });

    // Calling immediately should return cached status
    const secondStatus = await service.getStatus();
    expect(secondStatus.dlqCount).toBe(firstStatus.dlqCount);
    expect(clientMock.count).toHaveBeenCalledTimes(1);
  });
});
