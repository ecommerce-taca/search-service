import { Client } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { getProductsAlias } from '../common/constants';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { toSearchException } from '../common/es';
import { AppConfigService } from '../config/config.service';
import {
  CONSUMER_STATUS_PROVIDER,
  IConsumerStatusProvider,
} from '../event-consumer/interfaces';

export interface ReadinessReport {
  elasticsearch: 'UP';
  alias: string;
  consumer: string;
  event_lag_seconds: number;
}

@Injectable()
export class ReadinessService {
  private readonly logger = new Logger(ReadinessService.name);

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
    @Optional()
    @Inject(CONSUMER_STATUS_PROVIDER)
    private readonly provider?: IConsumerStatusProvider,
  ) {}

  async check(): Promise<ReadinessReport> {
    await this.checkElasticsearch();
    const alias = await this.checkAlias();
    const { consumer, eventLagSeconds } = await this.consumerStatus();

    return {
      elasticsearch: 'UP',
      alias,
      consumer,
      event_lag_seconds: eventLagSeconds,
    };
  }

  private async checkElasticsearch(): Promise<void> {
    try {
      const clusterHealth = await this.client.cluster.health({ timeout: '1s' });
      const status = clusterHealth?.status;
      if (status !== 'green' && status !== 'yellow') {
        throw new SearchException(
          SearchErrorCode.SEARCH_UNAVAILABLE,
          undefined,
          'Elasticsearch connection refused or timed out',
        );
      }
    } catch (err: unknown) {
      if (err instanceof SearchException) {
        throw err;
      }
      throw new SearchException(
        SearchErrorCode.SEARCH_UNAVAILABLE,
        undefined,
        'Elasticsearch connection refused or timed out',
      );
    }
  }

  private async checkAlias(): Promise<string> {
    const alias = getProductsAlias(this.configService.esIndexPrefix);
    try {
      const exists = await this.client.indices.existsAlias({ name: alias });
      if (!exists) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INDEX_NOT_READY,
          undefined,
          `Required alias ${alias} not found`,
        );
      }
    } catch (err: unknown) {
      if (err instanceof SearchException) {
        throw err;
      }
      throw toSearchException(err);
    }
    return alias;
  }

  private async consumerStatus(): Promise<{
    consumer: string;
    eventLagSeconds: number;
  }> {
    let consumer = 'PAUSED';
    let eventLagSeconds = 0;

    if (this.provider) {
      try {
        const resolved = await this.provider.getStatus();
        if (resolved) {
          consumer = resolved.state;
          eventLagSeconds = Math.round(resolved.maxLagSeconds ?? 0);
        }
      } catch (providerErr: unknown) {
        this.logger.warn({
          event: 'consumer_status_provider_error',
          error:
            providerErr instanceof Error
              ? providerErr.message
              : String(providerErr),
        });
        consumer = 'DEGRADED';
        eventLagSeconds = 0;
      }
    }

    return { consumer, eventLagSeconds };
  }
}
