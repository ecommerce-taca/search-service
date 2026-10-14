import { Client } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { getProductsAlias } from '../common/constants';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { toSearchException } from '../common/es';
import { AppConfigService } from '../config/config.service';

export const CONSUMER_STATUS_PROVIDER = 'CONSUMER_STATUS_PROVIDER';

export interface ConsumerStatus {
  consumer?: string;
  status?: string;
  event_lag_seconds?: number;
  lag_seconds?: number;
}

export interface ConsumerStatusProvider {
  getStatus?(): ConsumerStatus | Promise<ConsumerStatus>;
  getConsumerStatus?(): ConsumerStatus | Promise<ConsumerStatus>;
  consumer?: string;
  status?: string;
  event_lag_seconds?: number;
  lag_seconds?: number;
}

export interface ReadinessReport {
  elasticsearch: 'UP';
  alias: string;
  consumer: string;
  event_lag_seconds: number;
}

function attachErrorCode(ex: SearchException): SearchException {
  (ex as unknown as { errorCode: SearchErrorCode }).errorCode = ex.code;
  return ex;
}

@Injectable()
export class ReadinessService {
  private readonly logger = new Logger(ReadinessService.name);

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
    @Optional()
    @Inject(CONSUMER_STATUS_PROVIDER)
    private readonly consumerStatusProvider?: ConsumerStatusProvider,
  ) {}

  async check(): Promise<ReadinessReport> {
    // 1. Elasticsearch cluster health check
    try {
      let healthy = false;
      if (
        this.client.cluster &&
        typeof this.client.cluster.health === 'function'
      ) {
        const clusterHealth = await this.client.cluster.health({
          timeout: '1s',
        });
        const status = clusterHealth?.status;
        healthy = status === 'green' || status === 'yellow';
      } else if (
        typeof (this.client as unknown as { ping?: () => Promise<boolean> })
          .ping === 'function'
      ) {
        healthy = Boolean(
          await (
            this.client as unknown as { ping: () => Promise<boolean> }
          ).ping(),
        );
      }

      if (!healthy) {
        throw attachErrorCode(
          new SearchException(
            SearchErrorCode.SEARCH_UNAVAILABLE,
            undefined,
            'Elasticsearch connection refused or timed out',
          ),
        );
      }
    } catch (err: unknown) {
      if (err instanceof SearchException) {
        throw attachErrorCode(err);
      }
      throw attachErrorCode(
        new SearchException(
          SearchErrorCode.SEARCH_UNAVAILABLE,
          undefined,
          'Elasticsearch connection refused or timed out',
        ),
      );
    }

    // 2. Alias existence check
    const alias = getProductsAlias(this.configService.esIndexPrefix);
    try {
      const exists = await this.client.indices.existsAlias({ name: alias });
      const aliasExists =
        typeof exists === 'boolean'
          ? exists
          : Boolean((exists as unknown as { body?: boolean })?.body ?? exists);

      if (!aliasExists) {
        throw attachErrorCode(
          new SearchException(
            SearchErrorCode.SEARCH_INDEX_NOT_READY,
            undefined,
            `Required alias ${alias} not found`,
          ),
        );
      }
    } catch (err: unknown) {
      if (err instanceof SearchException) {
        throw attachErrorCode(err);
      }
      const searchEx = toSearchException(err);
      throw attachErrorCode(searchEx);
    }

    // 3. Consumer status provider check (optional)
    let consumer = 'PAUSED';
    let eventLagSeconds = 0;

    if (this.consumerStatusProvider) {
      try {
        let resolved: ConsumerStatus | undefined;
        if (typeof this.consumerStatusProvider.getStatus === 'function') {
          resolved = await this.consumerStatusProvider.getStatus();
        } else if (
          typeof this.consumerStatusProvider.getConsumerStatus === 'function'
        ) {
          resolved = await this.consumerStatusProvider.getConsumerStatus();
        } else {
          resolved = this.consumerStatusProvider;
        }

        if (resolved) {
          consumer = resolved.consumer ?? resolved.status ?? 'PAUSED';
          eventLagSeconds =
            resolved.event_lag_seconds ?? resolved.lag_seconds ?? 0;
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

    return {
      elasticsearch: 'UP',
      alias,
      consumer,
      event_lag_seconds: eventLagSeconds,
    };
  }
}
