import { Injectable } from '@nestjs/common';
import { VisibilityStatus } from '../../common/enums';
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { DomainEvent } from '../../event-consumer/interfaces';
import {
  DocumentChange,
  IProductDocumentBuilder,
  ProductSearchDocument,
} from '../interfaces';

export interface ProductEventPayload {
  product_id?: string;
  shop_id?: string;
  slug?: string;
  title?: string;
  description?: string;
  brand?: string;
  price?: PriceField;
  /** Catalog `product.updated` shape: { base_price, sale_price, currency }. */
  price_summary?: {
    base_price?: number;
    sale_price?: number;
    currency?: string;
  };
  currency?: string;
  category_path?: string[];
  category_ids?: string[];
  primary_category_id?: string;
  category_id?: string;
  /**
   * Attribute values keyed by name. Catalog `product.published` sends attribute
   * *definitions* as an array instead; those carry no values and are ignored.
   */
  attributes?: Record<string, unknown> | unknown[];
  media?: MediaField[];
  cover_url?: string;
  shop?: {
    shop_id: string;
    name: string;
  };
  visibility_status?: VisibilityStatus;
  published_at?: string;
  created_at?: string;
  next_status?: string;
}

export type PriceField =
  | number
  | {
      /** Catalog: list price. */
      base?: number;
      sale?: number;
      currency?: string;
      original?: number;
    };

export interface MediaField {
  url?: string;
  alt?: string;
  order?: number;
  is_cover?: boolean;
  status?: string;
}

export interface BuildContext {
  categoryIds?: string[];
}

@Injectable()
export class ProductDocumentBuilder implements IProductDocumentBuilder {
  build(
    evt: DomainEvent<unknown>,
    current: ProductSearchDocument | null,
    context?: BuildContext,
  ): DocumentChange {
    this.validateEvent(evt);

    // Deduplication and version ordering guards
    if (current?.source_event_id && current.source_event_id === evt.event_id) {
      return { kind: 'SKIP', reason: 'DUPLICATE' };
    }

    if (
      current &&
      typeof current.source_version === 'number' &&
      evt.version <= current.source_version
    ) {
      return { kind: 'SKIP', reason: 'OLD_VERSION' };
    }

    const payload = (evt.payload || {}) as ProductEventPayload;
    const productId = evt.aggregate_id || payload.product_id;
    if (!productId) {
      throw new SearchException(
        SearchErrorCode.SEARCH_EVENT_INVALID,
        'Event missing aggregate_id and product_id',
      );
    }

    switch (evt.event_type) {
      case 'product.created':
        return this.buildProductCreated(evt, payload, current, context);

      case 'product.updated':
        return this.buildProductUpdated(evt, payload, current, context);

      case 'product.published':
        return this.buildProductPublished(evt, payload, current, context);

      case 'product.unpublished':
        return {
          kind: 'PATCH',
          id: productId,
          doc: {
            visibility_status: VisibilityStatus.HIDDEN,
            source_version: evt.version,
            source_event_id: evt.event_id,
            indexed_at: String(evt.occurred_at || new Date().toISOString()),
          },
          sourceVersion: evt.version,
          sourceEventId: evt.event_id,
        };

      case 'product.archived':
        return {
          kind: 'PATCH',
          id: productId,
          doc: {
            visibility_status: VisibilityStatus.DELETED,
            source_version: evt.version,
            source_event_id: evt.event_id,
            indexed_at: String(evt.occurred_at || new Date().toISOString()),
          },
          sourceVersion: evt.version,
          sourceEventId: evt.event_id,
        };

      case 'product.blocked':
        return {
          kind: 'PATCH',
          id: productId,
          doc: {
            visibility_status: VisibilityStatus.HIDDEN,
            source_version: evt.version,
            source_event_id: evt.event_id,
            indexed_at: String(evt.occurred_at || new Date().toISOString()),
          },
          sourceVersion: evt.version,
          sourceEventId: evt.event_id,
        };

      case 'product.unblocked': {
        const nextStatus = payload.next_status?.toUpperCase();
        const nextVisibility =
          nextStatus === 'ACTIVE' || nextStatus === VisibilityStatus.PUBLISHED
            ? VisibilityStatus.PUBLISHED
            : VisibilityStatus.HIDDEN;

        return {
          kind: 'PATCH',
          id: productId,
          doc: {
            visibility_status: nextVisibility,
            source_version: evt.version,
            source_event_id: evt.event_id,
            indexed_at: String(evt.occurred_at || new Date().toISOString()),
          },
          sourceVersion: evt.version,
          sourceEventId: evt.event_id,
        };
      }

      case 'product.media_uploaded': {
        // Catalog emits one event per uploaded file; only a cover changes the card image.
        const media = payload as { is_cover?: boolean; url?: string };
        if (!media.is_cover || !media.url) {
          return { kind: 'SKIP', reason: 'NOT_APPLICABLE' };
        }
        return {
          kind: 'PATCH',
          id: productId,
          doc: {
            cover_url: media.url,
            source_version: evt.version,
            source_event_id: evt.event_id,
            indexed_at: String(evt.occurred_at || new Date().toISOString()),
          },
          sourceVersion: evt.version,
          sourceEventId: evt.event_id,
        };
      }

      default:
        return { kind: 'SKIP', reason: 'NOT_APPLICABLE' };
    }
  }

  withCategoryIds(change: DocumentChange, ids: string[]): DocumentChange {
    if (change.kind === 'UPSERT') {
      return {
        ...change,
        doc: {
          ...change.doc,
          category_ids: ids,
        },
      };
    }
    if (change.kind === 'PATCH') {
      return {
        ...change,
        doc: {
          ...change.doc,
          category_ids: ids,
        },
      };
    }
    return change;
  }

  private validateEvent(evt: DomainEvent<unknown>): void {
    if (
      !evt ||
      typeof evt !== 'object' ||
      !evt.event_id ||
      !evt.event_type ||
      typeof evt.version !== 'number' ||
      isNaN(evt.version) ||
      evt.version < 1 ||
      !evt.payload ||
      typeof evt.payload !== 'object' ||
      (!evt.aggregate_id &&
        !(evt.payload as Record<string, unknown>).product_id)
    ) {
      throw new SearchException(
        SearchErrorCode.SEARCH_EVENT_INVALID,
        'Invalid domain event structure',
      );
    }
  }

  private buildProductCreated(
    evt: DomainEvent<unknown>,
    payload: ProductEventPayload,
    current: ProductSearchDocument | null,
    context?: BuildContext,
  ): DocumentChange {
    if (current) {
      return this.buildProductUpdated(evt, payload, current, context);
    }
    const productId = evt.aggregate_id || payload.product_id || '';
    const title = payload.title || '';
    const brand = payload.brand || '';

    const price = this.resolvePrice(payload);
    const currency = this.resolveCurrency(payload);

    const categoryIds = context?.categoryIds || payload.category_ids || [];

    const doc: ProductSearchDocument = {
      product_id: productId,
      shop_id: payload.shop_id || payload.shop?.shop_id || '',
      slug: payload.slug || '',
      title,
      title_exact: title,
      description: payload.description || '',
      category_ids: categoryIds,
      category_path: payload.category_path || [],
      brand,
      price,
      currency,
      rating_avg: 0,
      rating_count: 0,
      attributes: {},
      spec_attributes: this.specAttributesOf(payload.attributes),
      attribute_pairs: [],
      variant_skus: {},
      suggest: {
        input: [title, brand].filter((item): item is string => Boolean(item)),
      },
      visibility_status: VisibilityStatus.HIDDEN,
      source_version: evt.version,
      source_event_id: evt.event_id,
      indexed_at: String(evt.occurred_at || new Date().toISOString()),
      created_at:
        payload.created_at ||
        (evt.occurred_at ? String(evt.occurred_at) : undefined) ||
        new Date().toISOString(),
      cover_url: this.pickCoverUrl(payload.media) || payload.cover_url,
      shop:
        payload.shop ||
        (payload.shop_id ? { shop_id: payload.shop_id, name: '' } : undefined),
    };

    return {
      kind: 'UPSERT',
      id: productId,
      doc,
    };
  }

  private buildProductUpdated(
    evt: DomainEvent<unknown>,
    payload: ProductEventPayload,
    current: ProductSearchDocument | null,
    context?: BuildContext,
  ): DocumentChange {
    const productId = evt.aggregate_id || payload.product_id || '';
    const doc: Partial<ProductSearchDocument> = {
      source_version: evt.version,
      source_event_id: evt.event_id,
      indexed_at: String(evt.occurred_at || new Date().toISOString()),
    };

    if (payload.title !== undefined) {
      doc.title = payload.title;
      doc.title_exact = payload.title;
    }
    if (payload.slug !== undefined) {
      doc.slug = payload.slug;
    }
    if (payload.description !== undefined) {
      doc.description = payload.description;
    }
    if (payload.brand !== undefined) {
      doc.brand = payload.brand;
    }
    if (payload.shop_id !== undefined) {
      doc.shop_id = payload.shop_id;
    }
    if (payload.shop !== undefined) {
      doc.shop = payload.shop;
    }
    if (payload.category_path !== undefined) {
      doc.category_path = payload.category_path;
    }
    if (context?.categoryIds) {
      doc.category_ids = context.categoryIds;
    } else if (payload.category_ids !== undefined) {
      doc.category_ids = payload.category_ids;
    }
    if (payload.price !== undefined || payload.price_summary !== undefined) {
      doc.price = this.resolvePrice(payload);
      doc.currency = this.resolveCurrency(payload);
    } else if (payload.currency !== undefined) {
      doc.currency = payload.currency;
    }
    const specAttributes = this.specAttributesOf(payload.attributes);
    if (specAttributes !== undefined) {
      doc.spec_attributes = specAttributes;
    }
    const coverUrl = this.pickCoverUrl(payload.media);
    if (coverUrl) {
      doc.cover_url = coverUrl;
    } else if (payload.cover_url !== undefined) {
      doc.cover_url = payload.cover_url;
    }

    const titleForSuggest =
      payload.title !== undefined ? payload.title : current?.title;
    const brandForSuggest =
      payload.brand !== undefined ? payload.brand : current?.brand;
    if (payload.title !== undefined || payload.brand !== undefined) {
      doc.suggest = {
        input: [titleForSuggest, brandForSuggest].filter(
          (item): item is string => Boolean(item),
        ),
      };
    }

    return {
      kind: 'PATCH',
      id: productId,
      doc,
      sourceVersion: evt.version,
      sourceEventId: evt.event_id,
    };
  }

  private buildProductPublished(
    evt: DomainEvent<unknown>,
    payload: ProductEventPayload,
    current: ProductSearchDocument | null,
    context?: BuildContext,
  ): DocumentChange {
    const productId = evt.aggregate_id || payload.product_id || '';
    const title = payload.title || current?.title || '';
    const brand = payload.brand || current?.brand || '';

    const price = this.resolvePrice(payload);
    const currency = this.resolveCurrency(payload);

    const categoryIds =
      context?.categoryIds ||
      payload.category_ids ||
      current?.category_ids ||
      [];

    const shop =
      payload.shop ||
      current?.shop ||
      (payload.shop_id
        ? { shop_id: payload.shop_id, name: '' }
        : current?.shop_id
          ? { shop_id: current.shop_id, name: '' }
          : undefined);

    const coverUrl =
      this.pickCoverUrl(payload.media) ||
      payload.cover_url ||
      current?.cover_url;

    const createdAt =
      current?.created_at ||
      payload.created_at ||
      payload.published_at ||
      (evt.occurred_at ? String(evt.occurred_at) : undefined) ||
      new Date().toISOString();

    const visibilityStatus =
      payload.visibility_status || VisibilityStatus.PUBLISHED;

    const doc: ProductSearchDocument = {
      product_id: productId,
      shop_id:
        payload.shop_id || payload.shop?.shop_id || current?.shop_id || '',
      slug: payload.slug || current?.slug || '',
      title,
      title_exact: title,
      description: payload.description || current?.description || '',
      category_ids: categoryIds,
      category_path: payload.category_path || current?.category_path || [],
      brand,
      price,
      currency,
      rating_avg: current?.rating_avg ?? 0,
      rating_count: current?.rating_count ?? 0,
      attributes: current?.attributes ?? {},
      spec_attributes:
        this.specAttributesOf(payload.attributes) ?? current?.spec_attributes,
      attribute_pairs: current?.attribute_pairs ?? [],
      variant_skus: current?.variant_skus ?? {},
      suggest: {
        input: [title, brand].filter((item): item is string => Boolean(item)),
      },
      visibility_status: visibilityStatus,
      source_version: evt.version,
      source_event_id: evt.event_id,
      indexed_at: String(evt.occurred_at || new Date().toISOString()),
      created_at: createdAt,
      cover_url: coverUrl,
      shop,
    };

    return {
      kind: 'UPSERT',
      id: productId,
      doc,
    };
  }

  /** sale price when set (> 0), otherwise the list price. */
  private resolvePrice(payload: ProductEventPayload): number {
    const field = payload.price;
    if (typeof field === 'number') {
      return field;
    }
    if (typeof field === 'object' && field !== null) {
      for (const candidate of [field.sale, field.base, field.original]) {
        if (typeof candidate === 'number' && candidate > 0) {
          return candidate;
        }
      }
    }
    const summary = payload.price_summary;
    if (summary) {
      for (const candidate of [summary.sale_price, summary.base_price]) {
        if (typeof candidate === 'number' && candidate > 0) {
          return candidate;
        }
      }
    }
    return 0;
  }

  private resolveCurrency(payload: ProductEventPayload): string {
    const candidates = [
      typeof payload.price === 'object' && payload.price !== null
        ? payload.price.currency
        : undefined,
      payload.price_summary?.currency,
      payload.currency,
    ];
    const found = candidates.find(
      (c): c is string => typeof c === 'string' && c.trim().length > 0,
    );
    return found ?? 'VND';
  }

  /** Cover image: the media flagged `is_cover`, otherwise the first one. */
  private pickCoverUrl(media: MediaField[] | undefined): string | undefined {
    if (!Array.isArray(media) || media.length === 0) {
      return undefined;
    }
    const usable = media.filter((m) => typeof m?.url === 'string' && m.url);
    return (usable.find((m) => m.is_cover) ?? usable[0])?.url;
  }

  /** Only a key→value object carries attribute values; arrays are definitions. */
  private specAttributesOf(
    raw: Record<string, unknown> | unknown[] | undefined,
  ): Record<string, string[] | string> | undefined {
    if (!raw || Array.isArray(raw) || typeof raw !== 'object') {
      return undefined;
    }
    return this.normalizeSpecAttributes(raw);
  }

  private normalizeSpecAttributes(
    raw: Record<string, unknown>,
  ): Record<string, string[] | string> {
    const result: Record<string, string[] | string> = {};
    for (const [k, v] of Object.entries(raw)) {
      if (v === null || v === undefined) {
        continue;
      }
      if (Array.isArray(v)) {
        result[k] = v.map((item) => String(item));
      } else {
        result[k] = String(v);
      }
    }
    return result;
  }
}
