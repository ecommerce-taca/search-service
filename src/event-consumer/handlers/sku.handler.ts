import { Client, errors } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  ES_REQUEST_TIMEOUT_MS,
  getProductsAlias,
} from '../../common/constants';
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { AppConfigService } from '../../config/config.service';
import { ELASTICSEARCH_CLIENT } from '../../product-index/elasticsearch.module';
import {
  IIndexWriter,
  INDEX_WRITER,
  ProductSearchDocument,
} from '../../product-index/interfaces';
import { VariantAggregator } from '../../product-index/sku/variant-aggregator';
import { DomainEvent } from '../interfaces';

@Injectable()
export class SkuHandler {
  private readonly logger = new Logger(SkuHandler.name);

  constructor(
    @Inject(ELASTICSEARCH_CLIENT) private readonly client: Client,
    private readonly configService: AppConfigService,
    private readonly variantAggregator: VariantAggregator,
    @Inject(INDEX_WRITER) private readonly indexWriter: IIndexWriter,
  ) {}

  async handle(evt: DomainEvent<unknown>): Promise<void> {
    const payload = (evt.payload || {}) as Record<string, any>;
    const skuId = payload.sku_id || evt.aggregate_id;
    const productId = payload.product_id;

    if (!skuId || !productId) {
      throw new SearchException(
        SearchErrorCode.SEARCH_EVENT_INVALID,
        'SKU event missing sku_id or product_id',
      );
    }

    const productsAlias = getProductsAlias(this.configService.esIndexPrefix);
    const current = await this.fetchCurrent(productsAlias, productId);

    if (!current) {
      this.logger.warn(
        `Product ${productId} not found when applying SKU event ${evt.event_id}`,
      );
      return;
    }

    const aggregated = this.variantAggregator.handleSkuEvent(current, evt);
    if (!aggregated) {
      this.logger.debug(
        `SKU event ${evt.event_id} skipped for product ${productId} (older version or no-op)`,
      );
      return;
    }

    await this.indexWriter.bulkPartialUpdate(productsAlias, [
      {
        id: productId,
        doc: {
          variant_skus: aggregated.variant_skus,
          attributes: aggregated.attributes,
          attribute_pairs: aggregated.attribute_pairs,
          indexed_at: new Date().toISOString(),
        },
      },
    ]);
  }

  private async fetchCurrent(
    index: string,
    id: string,
  ): Promise<ProductSearchDocument | null> {
    try {
      const response = await this.client.get<ProductSearchDocument>(
        {
          index,
          id,
        },
        {
          requestTimeout: ES_REQUEST_TIMEOUT_MS,
        },
      );
      if (response.found && response._source) {
        return response._source as ProductSearchDocument;
      }
      return null;
    } catch (err: unknown) {
      if (
        (err instanceof errors.ResponseError ||
          (err as { name?: string })?.name === 'ResponseError') &&
        ((err as { statusCode?: number })?.statusCode === 404 ||
          (err as { meta?: { statusCode?: number } })?.meta?.statusCode === 404)
      ) {
        return null;
      }
      throw err;
    }
  }
}
