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
  price?:
    | number
    | {
        sale?: number;
        currency?: string;
        original?: number;
      };
  currency?: string;
  category_path?: string[];
  category_ids?: string[];
  primary_category_id?: string;
  category_id?: string;
  attributes?: Record<string, unknown>;
  media?: Array<{ url: string; alt?: string; order?: number }>;
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
            indexed_at: evt.occurred_at || new Date().toISOString(),
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
            indexed_at: evt.occurred_at || new Date().toISOString(),
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
            indexed_at: evt.occurred_at || new Date().toISOString(),
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
            indexed_at: evt.occurred_at || new Date().toISOString(),
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
      (!evt.aggregate_id &&
        (!evt.payload ||
          typeof evt.payload !== 'object' ||
          !(evt.payload as Record<string, unknown>).product_id))
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
    const productId = evt.aggregate_id || payload.product_id || '';
    const title = payload.title || current?.title || '';
    const brand = payload.brand || current?.brand || '';

    const price = this.extractPrice(payload.price);
    const currency = this.extractCurrency(payload.price, payload.currency);

    const categoryIds =
      context?.categoryIds ||
      payload.category_ids ||
      current?.category_ids ||
      [];

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
      spec_attributes: payload.attributes
        ? this.normalizeSpecAttributes(payload.attributes)
        : current?.spec_attributes,
      attribute_pairs: current?.attribute_pairs ?? [],
      variant_skus: current?.variant_skus ?? {},
      suggest: {
        input: [title, brand].filter((item): item is string => Boolean(item)),
      },
      visibility_status: VisibilityStatus.HIDDEN,
      source_version: evt.version,
      source_event_id: evt.event_id,
      indexed_at: evt.occurred_at || new Date().toISOString(),
      created_at:
        payload.created_at ||
        current?.created_at ||
        evt.occurred_at ||
        new Date().toISOString(),
      cover_url:
        payload.media?.[0]?.url || payload.cover_url || current?.cover_url,
      shop:
        payload.shop ||
        current?.shop ||
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
      indexed_at: evt.occurred_at || new Date().toISOString(),
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
    if (payload.price !== undefined) {
      doc.price = this.extractPrice(payload.price);
      doc.currency = this.extractCurrency(payload.price, payload.currency);
    } else if (payload.currency !== undefined) {
      doc.currency = payload.currency;
    }
    if (payload.attributes !== undefined) {
      doc.spec_attributes = this.normalizeSpecAttributes(payload.attributes);
    }
    if (payload.media !== undefined && payload.media.length > 0) {
      doc.cover_url = payload.media[0]?.url;
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

    const price = this.extractPrice(payload.price);
    const currency = this.extractCurrency(payload.price, payload.currency);

    const categoryIds =
      context?.categoryIds ||
      payload.category_ids ||
      current?.category_ids ||
      [];

    const shop =
      current?.shop ||
      payload.shop ||
      (payload.shop_id
        ? { shop_id: payload.shop_id, name: '' }
        : current?.shop_id
          ? { shop_id: current.shop_id, name: '' }
          : undefined);

    const coverUrl =
      payload.media?.[0]?.url || payload.cover_url || current?.cover_url;

    const createdAt =
      current?.created_at ||
      payload.published_at ||
      payload.created_at ||
      evt.occurred_at ||
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
      spec_attributes: payload.attributes
        ? this.normalizeSpecAttributes(payload.attributes)
        : current?.spec_attributes,
      attribute_pairs: current?.attribute_pairs ?? [],
      variant_skus: current?.variant_skus ?? {},
      suggest: {
        input: [title, brand].filter((item): item is string => Boolean(item)),
      },
      visibility_status: visibilityStatus,
      source_version: evt.version,
      source_event_id: evt.event_id,
      indexed_at: evt.occurred_at || new Date().toISOString(),
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

  private extractPrice(
    priceField:
      | number
      | { sale?: number; currency?: string; original?: number }
      | undefined,
  ): number {
    if (typeof priceField === 'number') {
      return priceField;
    }
    if (typeof priceField === 'object' && priceField !== null) {
      if (typeof priceField.sale === 'number') {
        return priceField.sale;
      }
      if (typeof priceField.original === 'number') {
        return priceField.original;
      }
    }
    return 0;
  }

  private extractCurrency(
    priceField:
      | number
      | { sale?: number; currency?: string; original?: number }
      | undefined,
    fallbackCurrency?: string,
  ): string {
    if (
      typeof priceField === 'object' &&
      priceField !== null &&
      typeof priceField.currency === 'string' &&
      priceField.currency.trim()
    ) {
      return priceField.currency;
    }
    if (typeof fallbackCurrency === 'string' && fallbackCurrency.trim()) {
      return fallbackCurrency;
    }
    return 'VND';
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
