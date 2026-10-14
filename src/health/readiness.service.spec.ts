import { Client, errors } from '@elastic/elasticsearch';
import { Provider } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { getProductsAlias } from '../common/constants';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';
import { ELASTICSEARCH_CLIENT } from '../product-index/elasticsearch.module';
import {
  CONSUMER_STATUS_PROVIDER,
  ConsumerStatusProvider,
  ReadinessService,
} from './readiness.service';

describe('ReadinessService (Unit)', () => {
  let service: ReadinessService;
  let mockEsClient: {
    cluster: { health: jest.Mock };
    indices: { existsAlias: jest.Mock };
  };
  let mockConfigService: {
    esIndexPrefix: string;
  };
  let mockConsumerProvider: ConsumerStatusProvider | undefined;

  const aliasName = getProductsAlias('test-');

  const createService = async (consumerProvider?: ConsumerStatusProvider) => {
    const providers: Provider[] = [
      ReadinessService,
      {
        provide: ELASTICSEARCH_CLIENT,
        useValue: mockEsClient,
      },
      {
        provide: Client,
        useValue: mockEsClient,
      },
      {
        provide: AppConfigService,
        useValue: mockConfigService,
      },
    ];

    if (consumerProvider) {
      providers.push({
        provide: CONSUMER_STATUS_PROVIDER,
        useValue: consumerProvider,
      });
    }

    const module: TestingModule = await Test.createTestingModule({
      providers,
    }).compile();

    return module.get<ReadinessService>(ReadinessService);
  };

  beforeEach(() => {
    mockEsClient = {
      cluster: {
        health: jest.fn().mockResolvedValue({ status: 'green' }),
      },
      indices: {
        existsAlias: jest.fn().mockResolvedValue(true),
      },
    };

    mockConfigService = {
      esIndexPrefix: 'test-',
    };
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('checkReadiness branches', () => {
    it('HL-U-01: returns UP, alias, consumer, and lag when everything is healthy', async () => {
      mockConsumerProvider = {
        getStatus: jest.fn().mockResolvedValue({
          consumer: 'RUNNING',
          event_lag_seconds: 2,
        }),
      };
      service = await createService(mockConsumerProvider);

      const res = await service.check();
      expect(res).toEqual({
        elasticsearch: 'UP',
        alias: aliasName,
        consumer: 'RUNNING',
        event_lag_seconds: 2,
      });
      expect(mockEsClient.cluster.health).toHaveBeenCalledWith({
        timeout: '1s',
      });
      expect(mockEsClient.indices.existsAlias).toHaveBeenCalledWith({
        name: aliasName,
      });
    });

    it('HL-U-02: throws SEARCH_UNAVAILABLE 503 when ES cluster.health throws ConnectionError', async () => {
      mockEsClient.cluster.health.mockRejectedValue(
        new errors.ConnectionError(
          'ES down',
          {} as unknown as ConstructorParameters<
            typeof errors.ConnectionError
          >[1],
        ),
      );
      service = await createService();

      await expect(service.check()).rejects.toThrow(SearchException);
      await expect(service.check()).rejects.toMatchObject({
        code: SearchErrorCode.SEARCH_UNAVAILABLE,
        errorCode: SearchErrorCode.SEARCH_UNAVAILABLE,
      });
    });

    it('HL-U-02: throws SEARCH_UNAVAILABLE 503 when ES cluster.health status is red', async () => {
      mockEsClient.cluster.health.mockResolvedValue({ status: 'red' });
      service = await createService();

      await expect(service.check()).rejects.toMatchObject({
        code: SearchErrorCode.SEARCH_UNAVAILABLE,
        errorCode: SearchErrorCode.SEARCH_UNAVAILABLE,
      });
    });

    it('HL-U-03: throws SEARCH_INDEX_NOT_READY 503 when alias does not exist', async () => {
      mockEsClient.indices.existsAlias.mockResolvedValue(false);
      service = await createService();

      await expect(service.check()).rejects.toThrow(SearchException);
      await expect(service.check()).rejects.toMatchObject({
        code: SearchErrorCode.SEARCH_INDEX_NOT_READY,
        errorCode: SearchErrorCode.SEARCH_INDEX_NOT_READY,
      });
    });

    it('HL-U-04: returns 200 report with consumer DEGRADED and high event lag (D14)', async () => {
      mockConsumerProvider = {
        getStatus: jest.fn().mockResolvedValue({
          consumer: 'DEGRADED',
          event_lag_seconds: 75,
        }),
      };
      service = await createService(mockConsumerProvider);

      const res = await service.check();
      expect(res).toEqual({
        elasticsearch: 'UP',
        alias: aliasName,
        consumer: 'DEGRADED',
        event_lag_seconds: 75,
      });
    });

    it('defaults consumer to PAUSED and lag to 0 when CONSUMER_STATUS_PROVIDER is absent', async () => {
      service = await createService(undefined);

      const res = await service.check();
      expect(res).toEqual({
        elasticsearch: 'UP',
        alias: aliasName,
        consumer: 'PAUSED',
        event_lag_seconds: 0,
      });
    });

    it('handles provider error gracefully by setting consumer to DEGRADED', async () => {
      mockConsumerProvider = {
        getStatus: jest.fn().mockRejectedValue(new Error('Broker unreachable')),
      };
      service = await createService(mockConsumerProvider);

      const res = await service.check();
      expect(res.consumer).toBe('DEGRADED');
      expect(res.event_lag_seconds).toBe(0);
    });
  });
});
