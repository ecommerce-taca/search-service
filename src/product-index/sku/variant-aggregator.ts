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
      const values = Array.from(aggregatedValues[key]).sort();
      if (values.length > 0) {
        attributes[key] = values;
        for (const val of values) {
          attributePairsSet.add(`${key}=${val}`);
        }
      }
    }

    const attribute_pairs = Array.from(attributePairsSet).sort();

    return {
      attributes,
      attribute_pairs,
    };
  }
}
