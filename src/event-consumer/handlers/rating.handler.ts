import { Client, errors } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import {
  ES_REQUEST_TIMEOUT_MS,
  getConsumerStateIndex,
  getProductsAlias,
} from '../../common/constants';
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { AppConfigService } from '../../config/config.service';
import { ELASTICSEARCH_CLIENT } from '../../product-index/elasticsearch.module';
import { IIndexWriter, INDEX_WRITER } from '../../product-index/interfaces';
import { DomainEvent } from '../interfaces';

export interface RatingEventPayload {
  product_id?: string;
  avg?: number;
  rating_avg?: number;
  count?: number;
  rating_count?: number;
}

@Injectable()
export class RatingHandler {
  private readonly logger = new Logger(RatingHandler.name);
  private readonly client: Client;
  private readonly configService?: AppConfigService;
  private readonly indexWriter: IIndexWriter;

  constructor(
    @Inject(ELASTICSEARCH_CLIENT) clientOrWriter: Client | IIndexWriter,
    @Optional()
    @Inject(AppConfigService)
    configServiceOrClient?: AppConfigService | Client,
    @Optional()
    @Inject(INDEX_WRITER)
    writerOrConfig?: IIndexWriter | AppConfigService,
  ) {
    if (clientOrWriter && 'bulkPartialUpdate' in (clientOrWriter as object)) {
      this.indexWriter = clientOrWriter as IIndexWriter;
      this.client = configServiceOrClient as Client;
      this.configService = writerOrConfig as AppConfigService;
    } else {
      this.client = clientOrWriter as Client;
      this.configService = configServiceOrClient as AppConfigService;
      this.indexWriter = writerOrConfig as IIndexWriter;
    }
  }

  async handle(evt: DomainEvent<unknown>): Promise<void> {
    const payload = (evt.payload || {}) as RatingEventPayload;
    const productId = payload.product_id || evt.aggregate_id;

    if (!productId) {
      throw new SearchException(
        SearchErrorCode.SEARCH_EVENT_INVALID,
        'Rating event missing product_id and aggregate_id',
      );
    }

    const occurredEpochMs = evt.occurred_at
      ? new Date(evt.occurred_at).getTime()
      : Date.now();

    const consumerStateIndex = getConsumerStateIndex(
      this.configService?.esIndexPrefix || '',
    );
    const guardDocId = `rating:${productId}`;

    // Read existing consumer-state doc for rating guard
    try {
      const existing = await this.client.get<{ source_version?: number }>(
        {
          index: consumerStateIndex,
          id: guardDocId,
        },
        {
          requestTimeout: ES_REQUEST_TIMEOUT_MS,
        },
      );

      if (
        existing.found &&
        existing._source &&
        typeof existing._source.source_version === 'number' &&
        occurredEpochMs <= existing._source.source_version
      ) {
        this.logger.debug(
          `Skipping older rating event for ${productId}: occurred ${occurredEpochMs} <= stored ${existing._source.source_version}`,
        );
        return;
      }
    } catch (err: unknown) {
      if (!(
        (err instanceof errors.ResponseError ||
          (err as { name?: string })?.name === 'ResponseError') &&
        ((err as { statusCode?: number })?.statusCode === 404 ||
          (err as { meta?: { statusCode?: number } })?.meta?.statusCode === 404)
      )) {
        throw err;
      }
    }

    // Save rating guard in consumer-state
    try {
      await this.client.index({
        index: consumerStateIndex,
        id: guardDocId,
        document: {
          event_id: evt.event_id,
          aggregate_id: guardDocId,
          source_version: occurredEpochMs,
          processed_at: new Date().toISOString(),
          status: 'PROCESSED',
        },
      });
    } catch (err: unknown) {
      this.logger.debug(
        `Failed to write rating state guard for ${productId}: ${err}`,
      );
    }

    const ratingAvg =
      typeof payload.avg === 'number'
        ? payload.avg
        : typeof payload.rating_avg === 'number'
          ? payload.rating_avg
          : 0;

    const ratingCount =
      typeof payload.count === 'number'
        ? payload.count
        : typeof payload.rating_count === 'number'
          ? payload.rating_count
          : 0;

    const productsAlias = getProductsAlias(
      this.configService?.esIndexPrefix || '',
    );

    await this.indexWriter.bulkPartialUpdate(productsAlias, [
      {
        id: productId,
        doc: {
          rating_avg: ratingAvg,
          rating_count: ratingCount,
          indexed_at: evt.occurred_at || new Date().toISOString(),
        },
      },
    ]);
  }
}

export { RatingHandler as RatingEventHandler };
