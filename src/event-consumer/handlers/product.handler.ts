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
  CATEGORY_PROJECTION,
  ICategoryProjection,
  IIndexWriter,
  INDEX_WRITER,
  IProductDocumentBuilder,
  PRODUCT_DOCUMENT_BUILDER,
  ProductSearchDocument,
} from '../../product-index/interfaces';
import { ProductEventPayload } from '../../product-index/document-builder/product-document.builder';
import { DomainEvent } from '../interfaces';

@Injectable()
export class ProductHandler {
  private readonly logger = new Logger(ProductHandler.name);

  constructor(
    @Inject(ELASTICSEARCH_CLIENT) private readonly client: Client,
    private readonly configService: AppConfigService,
    @Inject(PRODUCT_DOCUMENT_BUILDER)
    private readonly builder: IProductDocumentBuilder,
    @Inject(INDEX_WRITER) private readonly indexWriter: IIndexWriter,
    @Inject(CATEGORY_PROJECTION)
    private readonly categoryProjection: ICategoryProjection,
  ) {}

  async handle(evt: DomainEvent<unknown>): Promise<void> {
    const payload = (evt.payload || {}) as ProductEventPayload;
    const productId = evt.aggregate_id || payload.product_id;

    if (!productId) {
      throw new SearchException(
        SearchErrorCode.SEARCH_EVENT_INVALID,
        'Product event missing aggregate_id and product_id',
      );
    }

    const productsAlias = getProductsAlias(this.configService.esIndexPrefix);
    const current = await this.fetchCurrent(productsAlias, productId);

    let categoryIds: string[] | undefined = undefined;
    const primaryCatId =
      payload.primary_category_id ||
      payload.category_id ||
      (Array.isArray(payload.category_ids) &&
        payload.category_ids[payload.category_ids.length - 1]);

    if (primaryCatId && typeof primaryCatId === 'string') {
      categoryIds = await this.categoryProjection.resolvePathIds(primaryCatId);
    }

    const change = this.builder.build(evt, current);

    if (change.kind === 'SKIP') {
      this.logger.debug(
        `Skipped product event ${evt.event_id} on ${productId}: ${change.reason}`,
      );
      return;
    }

    if (change.kind === 'UPSERT') {
      if (categoryIds && categoryIds.length > 0) {
        change.doc.category_ids = categoryIds;
      }
      await this.indexWriter.bulkUpsert(productsAlias, [change.doc]);
    } else if (change.kind === 'PATCH') {
      if (categoryIds && categoryIds.length > 0) {
        change.doc.category_ids = categoryIds;
      }
      await this.indexWriter.bulkPartialUpdate(productsAlias, [
        { id: change.id, doc: change.doc },
      ]);
    }
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
