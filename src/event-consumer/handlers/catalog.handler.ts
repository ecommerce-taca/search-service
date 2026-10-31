import { Inject, Injectable, Logger } from '@nestjs/common';
import { getProductsAlias } from '../../common/constants';
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { AppConfigService } from '../../config/config.service';
import {
  CATEGORY_PROJECTION,
  ICategoryProjection,
  IIndexWriter,
  INDEX_WRITER,
  ProductSearchDocument,
} from '../../product-index/interfaces';
import { DomainEvent } from '../interfaces';

export interface CatalogEventPayload {
  product_id?: string;
  primary_category_id?: string;
  category_id?: string;
  category_ids?: string[];
  secondary_category_ids?: string[];
  category_path?: string[];
  media?: Array<{ url: string; alt?: string; order?: number }>;
  cover_url?: string;
  shop?: { shop_id: string; name: string };
  shop_id?: string;
  name?: string;
  shop_name?: string;
}

@Injectable()
export class CatalogHandler {
  private readonly logger = new Logger(CatalogHandler.name);

  constructor(
    private readonly configService: AppConfigService,
    @Inject(INDEX_WRITER) private readonly indexWriter: IIndexWriter,
    @Inject(CATEGORY_PROJECTION)
    private readonly categoryProjection: ICategoryProjection,
  ) {}

  async handle(evt: DomainEvent<unknown>): Promise<void> {
    const payload = (evt.payload || {}) as CatalogEventPayload;

    // Catalog phát event này một lần cho cả shop: aggregate_type=SHOP_PROJECTION,
    // aggregate_id=shop_id, payload không có product_id (contract đã triển khai).
    if (
      evt.event_type === 'product.shop_snapshot_updated' &&
      !payload.product_id
    ) {
      return this.handleShopSnapshotByShop(evt, payload);
    }

    const productId = evt.aggregate_id || payload.product_id;

    if (!productId) {
      throw new SearchException(
        SearchErrorCode.SEARCH_EVENT_INVALID,
        'Catalog event missing product_id and aggregate_id',
      );
    }

    const productsAlias = getProductsAlias(this.configService.esIndexPrefix);

    switch (evt.event_type) {
      case 'product.category_changed': {
        const primaryCatId =
          payload.primary_category_id ||
          payload.category_id ||
          (Array.isArray(payload.category_ids) &&
            payload.category_ids[payload.category_ids.length - 1]);

        let categoryIds: string[] = [];
        if (primaryCatId && typeof primaryCatId === 'string') {
          const resolved =
            await this.categoryProjection.resolvePathIds(primaryCatId);
          // Projection may not know the category (catalog emits no category events).
          categoryIds = resolved.length > 0 ? resolved : [primaryCatId];
          for (const secondary of payload.secondary_category_ids ?? []) {
            const path =
              await this.categoryProjection.resolvePathIds(secondary);
            for (const id of path.length > 0 ? path : [secondary]) {
              if (!categoryIds.includes(id)) {
                categoryIds.push(id);
              }
            }
          }
        } else if (Array.isArray(payload.category_ids)) {
          categoryIds = payload.category_ids;
        }

        const patch: Partial<ProductSearchDocument> = {
          category_ids: categoryIds,
          indexed_at: String(evt.occurred_at || new Date().toISOString()),
        };
        if (Array.isArray(payload.category_path)) {
          patch.category_path = payload.category_path;
        }

        await this.indexWriter.bulkPartialUpdate(productsAlias, [
          { id: productId, doc: patch },
        ]);
        break;
      }

      case 'product.media_updated': {
        const coverUrl =
          payload.media?.[0]?.url || payload.cover_url || undefined;
        if (!coverUrl) {
          this.logger.debug(
            `product.media_updated on ${productId} has no valid cover image`,
          );
          return;
        }

        await this.indexWriter.bulkPartialUpdate(productsAlias, [
          {
            id: productId,
            doc: {
              cover_url: coverUrl,
              indexed_at: String(evt.occurred_at || new Date().toISOString()),
            },
          },
        ]);
        break;
      }

      case 'product.shop_snapshot_updated': {
        const shop =
          payload.shop ||
          (payload.shop_id
            ? {
                shop_id: payload.shop_id,
                name: payload.name || payload.shop_name || '',
              }
            : undefined);
        if (!shop) {
          this.logger.debug(
            `product.shop_snapshot_updated on ${productId} has no valid shop snapshot`,
          );
          return;
        }

        await this.indexWriter.bulkPartialUpdate(productsAlias, [
          {
            id: productId,
            doc: {
              shop,
              indexed_at: String(evt.occurred_at || new Date().toISOString()),
            },
          },
        ]);
        break;
      }

      default:
        this.logger.debug(
          `Unhandled catalog event type ${evt.event_type} on ${productId}`,
        );
        break;
    }
  }

  private async handleShopSnapshotByShop(
    evt: DomainEvent<unknown>,
    payload: CatalogEventPayload,
  ): Promise<void> {
    const shopId = payload.shop_id || payload.shop?.shop_id || evt.aggregate_id;
    const name = payload.name || payload.shop_name || payload.shop?.name;
    if (!shopId || !name) {
      this.logger.debug(
        `product.shop_snapshot_updated ${evt.event_id} has no shop_id/name`,
      );
      return;
    }
    const productsAlias = getProductsAlias(this.configService.esIndexPrefix);
    const updated = await this.indexWriter.updateShopSnapshot(productsAlias, {
      shop_id: shopId,
      name,
    });
    this.logger.debug(
      `product.shop_snapshot_updated shop=${shopId} updated ${updated} documents`,
    );
  }
}
