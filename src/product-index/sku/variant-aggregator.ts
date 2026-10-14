import { Injectable, Logger } from '@nestjs/common';
import { DomainEvent } from '../../event-consumer/interfaces';
import { ProductSearchDocument } from '../interfaces';

export interface SkuEventPayload {
  sku_id: string;
  product_id?: string;
  status: string;
  attributes?: Record<string, string>;
  version?: number;
}

export interface VariantSkuEntry {
  status: string;
  attributes: Record<string, string>;
  version: number;
}

export interface AggregatedVariantState {
  variant_skus: Record<string, VariantSkuEntry>;
  attributes: Record<string, string[]>;
  attribute_pairs: string[];
}

const ATTRIBUTE_KEY_REGEX = /^[a-z0-9_]{1,64}$/;

@Injectable()
export class VariantAggregator {
  private readonly logger = new Logger(VariantAggregator.name);

  /**
   * Aggregates variant state from a SKU event and existing product document.
   */
  aggregate(
    existingDoc: {
      variant_skus?: Record<string, VariantSkuEntry>;
      attributes?: Record<string, string[]>;
      attribute_pairs?: string[];
    } | null,
    skuEvent:
      | SkuEventPayload
      | DomainEvent<unknown>
      | {
          sku_id: string;
          product_id?: string;
          status: string;
          attributes?: Record<string, string>;
          version?: number;
        },
  ): AggregatedVariantState {
    const payload = (
      'payload' in skuEvent && skuEvent.payload ? skuEvent.payload : skuEvent
    ) as SkuEventPayload;
    const skuId =
      payload.sku_id ||
      ('aggregate_id' in skuEvent
        ? (skuEvent as DomainEvent<unknown>).aggregate_id
        : '');
    const version =
      typeof payload.version === 'number'
        ? payload.version
        : 'version' in skuEvent
          ? (skuEvent as DomainEvent<unknown>).version
          : 1;

    const syntheticEvt: DomainEvent<unknown> = {
      event_id:
        ('event_id' in skuEvent
          ? (skuEvent as DomainEvent<unknown>).event_id
          : '') || 'synthetic-event',
      schema_version: 1,
      event_type: 'sku.updated',
      occurred_at:
        ('occurred_at' in skuEvent
          ? (skuEvent as DomainEvent<unknown>).occurred_at
          : '') || new Date().toISOString(),
      aggregate_type: 'SKU',
      aggregate_id: skuId,
      version: version ?? 1,
      payload: {
        ...payload,
        sku_id: skuId,
        version: version ?? 1,
      },
    };

    const res = this.handleSkuEvent(
      existingDoc as ProductSearchDocument | null,
      syntheticEvt,
    );

    return (
      res || {
        variant_skus: existingDoc?.variant_skus || {},
        attributes: existingDoc?.attributes || {},
        attribute_pairs: existingDoc?.attribute_pairs || [],
      }
    );
  }

  /**
   * Applies an incoming SKU event against the current document's variant_skus.
   * Returns the updated state ({ variant_skus, attributes, attribute_pairs }) or null if the event should be skipped (e.g. older version).
   */
  handleSkuEvent(
    current: ProductSearchDocument | null,
    evt: DomainEvent<unknown>,
  ): AggregatedVariantState | null {
    const payload = (evt.payload || {}) as SkuEventPayload;
    const skuId = payload.sku_id || evt.aggregate_id;
    const productId = payload.product_id || current?.product_id;

    if (!skuId) {
      return null;
    }

    const skuVersion =
      typeof payload.version === 'number' ? payload.version : evt.version;

    const currentVariantSkus: Record<string, VariantSkuEntry> = {
      ...(current?.variant_skus || {}),
    };

    const existingSku = currentVariantSkus[skuId];
    if (existingSku && skuVersion <= existingSku.version) {
      // Ignore sku events older than or equal to the stored per-sku version
      return null;
    }

    // Filter and sanitize attributes
    const rawAttrs =
      payload.attributes || (existingSku ? existingSku.attributes : {});
    const sanitizedAttrs: Record<string, string> = {};

    for (const [key, val] of Object.entries(rawAttrs)) {
      if (!ATTRIBUTE_KEY_REGEX.test(key)) {
        // Drop key not matching /^[a-z0-9_]{1,64}$/ and log a warn with product_id only
        this.logger.warn(
          `Invalid attribute key dropped for product_id: ${productId || 'unknown'}`,
        );
        continue;
      }
      if (val !== null && val !== undefined) {
        sanitizedAttrs[key] = String(val);
      }
    }

    const newStatus =
      payload.status || (existingSku ? existingSku.status : 'INACTIVE');

    currentVariantSkus[skuId] = {
      status: newStatus.toUpperCase(),
      attributes: sanitizedAttrs,
      version: skuVersion,
    };

    const { attributes, attribute_pairs } = this.computeAttributes(
      productId || 'unknown',
      currentVariantSkus,
    );

    return {
      variant_skus: currentVariantSkus,
      attributes,
      attribute_pairs,
    };
  }

  /**
   * Replaces the whole SKU set of a product (Catalog `product.skus_updated` is a
   * full snapshot, SKUs missing from it were deleted). Returns null when the
   * snapshot is older than state already applied.
   */
  replaceAll(
    current: ProductSearchDocument | null,
    skus: SkuEventPayload[],
    version: number,
    productId: string,
  ): AggregatedVariantState | null {
    const applied = Object.values(current?.variant_skus || {});
    if (applied.some((entry) => entry.version > version)) {
      return null;
    }

    const variantSkus: Record<string, VariantSkuEntry> = {};
    for (const sku of skus) {
      if (!sku?.sku_id) {
        continue;
      }
      const attributes: Record<string, string> = {};
      for (const [key, val] of Object.entries(sku.attributes || {})) {
        if (!ATTRIBUTE_KEY_REGEX.test(key)) {
          this.logger.warn(
            `Invalid attribute key dropped for product_id: ${productId}`,
          );
          continue;
        }
        if (val !== null && val !== undefined) {
          attributes[key] = String(val);
        }
      }
      variantSkus[sku.sku_id] = {
        status: String(sku.status || 'ACTIVE').toUpperCase(),
        attributes,
        version,
      };
    }

    return {
      variant_skus: variantSkus,
      ...this.computeAttributes(productId, variantSkus),
    };
  }

  /**
   * Recomputes attributes and attribute_pairs from all active SKUs.
   */
  computeAttributes(
    productId: string,
    variantSkus: Record<string, VariantSkuEntry>,
  ): { attributes: Record<string, string[]>; attribute_pairs: string[] } {
    const aggregatedValues: Record<string, Set<string>> = {};

    for (const entry of Object.values(variantSkus)) {
      if (entry.status === 'ACTIVE' && entry.attributes) {
        for (const [key, val] of Object.entries(entry.attributes)) {
          if (!ATTRIBUTE_KEY_REGEX.test(key)) {
            this.logger.warn(
              `Invalid attribute key dropped for product_id: ${productId}`,
            );
            continue;
          }
          if (val !== null && val !== undefined && String(val).trim()) {
            if (!aggregatedValues[key]) {
              aggregatedValues[key] = new Set<string>();
            }
            aggregatedValues[key].add(String(val).trim());
          }
        }
      }
    }

    const attributes: Record<string, string[]> = {};
    const attributePairsSet = new Set<string>();

    const sortedKeys = Object.keys(aggregatedValues).sort();
    for (const key of sortedKeys) {
      const values = Array.from(aggregatedValues[key]).sort((a, b) =>
        a.localeCompare(b, 'vi'),
      );
      if (values.length > 0) {
        attributes[key] = values;
        for (const val of values) {
          attributePairsSet.add(`${key}=${val}`);
        }
      }
    }

    const attribute_pairs = Array.from(attributePairsSet).sort((a, b) =>
      a.localeCompare(b, 'vi'),
    );

    return {
      attributes,
      attribute_pairs,
    };
  }
}
