import { estypes } from '@elastic/elasticsearch';
import { Inject, Injectable } from '@nestjs/common';
import {
  CATEGORY_PROJECTION,
  ICategoryProjection,
} from '../product-index/interfaces';
import {
  MAX_ATTRIBUTE_FACET_KEYS,
  MAX_ATTRIBUTE_FACET_VALUES,
} from './constants';
import { PRICE_BUCKETS } from './es/price-buckets';
import {
  AttributeFacetItem,
  BrandFacetItem,
  CategoryFacetItem,
  PriceFacetBucket,
  SearchFacets,
} from './result.mapper';

export interface AggWithValues<T> {
  values?: {
    buckets?: T[];
  };
}

@Injectable()
export class FacetMapper {
  constructor(
    @Inject(CATEGORY_PROJECTION)
    private readonly categoryProjection: ICategoryProjection,
  ) {}

  async mapFacets(
    aggs: Record<string, estypes.AggregationsAggregate> | undefined,
    selectedCategoryId?: string,
    selectedAttributeKeys: string[] = [],
  ): Promise<SearchFacets> {
    const categoryAgg = aggs?.category as
      AggWithValues<{ key: string; doc_count: number }> | undefined;
    const rawCategoryBuckets = categoryAgg?.values?.buckets || [];

    const brandAgg = aggs?.brand as
      AggWithValues<{ key: string; doc_count: number }> | undefined;
    const rawBrandBuckets = brandAgg?.values?.buckets || [];

    const priceAgg = aggs?.price as
      | AggWithValues<{
          key?: string;
          from?: number;
          to?: number;
          doc_count: number;
        }>
      | undefined;
    const rawPriceBuckets = priceAgg?.values?.buckets || [];

    const category = await this.buildCategoryFacet(
      rawCategoryBuckets,
      selectedCategoryId,
    );
    const brand = this.buildBrandFacet(rawBrandBuckets);
    const price = this.buildPriceFacet(rawPriceBuckets);
    const attributes = this.buildAttributeFacets(aggs, selectedAttributeKeys);

    return {
      category,
      brand,
      price,
      attributes,
    };
  }

  async buildCategoryFacet(
    rawCategoryBuckets: Array<{ key: string; doc_count: number }>,
    selectedCategoryId?: string,
  ): Promise<CategoryFacetItem[]> {
    const categoryIds = rawCategoryBuckets
      .filter((b) => b.doc_count > 0)
      .map((b) => String(b.key));

    const projectionMap = await this.categoryProjection.getMany(categoryIds);

    const categoryItems: CategoryFacetItem[] = [];

    for (const bucket of rawCategoryBuckets) {
      if (bucket.doc_count <= 0) continue;
      const proj = projectionMap.get(String(bucket.key));
      if (!proj) continue; // drop unknown ids

      // D4 scope: without category filter keep ids whose projection parent_id is null;
      // with category_id=X keep ids whose parent_id === X
      if (!selectedCategoryId) {
        if (
          proj.parent_id !== null &&
          proj.parent_id !== undefined &&
          proj.parent_id !== ''
        ) {
          continue;
        }
      } else {
        if (proj.parent_id !== selectedCategoryId) {
          continue;
        }
      }

      categoryItems.push({
        category_id: proj.category_id,
        name: proj.name,
        count: bucket.doc_count,
      });
    }

    categoryItems.sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.name.localeCompare(b.name);
    });

    return categoryItems;
  }

  buildBrandFacet(
    rawBrandBuckets: Array<{ key: string; doc_count: number }>,
  ): BrandFacetItem[] {
    return rawBrandBuckets
      .filter((b) => b.doc_count > 0)
      .map((b) => ({
        value: String(b.key),
        count: b.doc_count,
      }))
      .sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        return a.value.localeCompare(b.value);
      });
  }

  buildPriceFacet(
    rawPriceBuckets: Array<{
      key?: string;
      from?: number;
      to?: number;
      doc_count: number;
    }>,
  ): PriceFacetBucket[] {
    const bucketMap = new Map<string, number>();
    for (const b of rawPriceBuckets) {
      if (b.key !== undefined) {
        bucketMap.set(String(b.key), b.doc_count);
      }
    }

    return PRICE_BUCKETS.map((def) => {
      let count = bucketMap.get(def.key);
      if (count === undefined) {
        const matched = rawPriceBuckets.find(
          (b) =>
            (b.from ?? 0) === (def.from ?? 0) &&
            (b.to ?? null) === (def.to ?? null),
        );
        count = matched?.doc_count;
      }
      return {
        min: def.min,
        max: def.max,
        count: count ?? 0,
      };
    });
  }

  buildAttributeFacets(
    aggregations: Record<string, estypes.AggregationsAggregate> | undefined,
    selectedKeys: string[],
  ): Record<string, AttributeFacetItem[]> {
    const keyValuesMap = new Map<string, Map<string, number>>();

    const addBucket = (keyVal: string, count: number) => {
      if (count <= 0) return;
      const eqIdx = keyVal.indexOf('=');
      if (eqIdx <= 0) return;
      const attrKey = keyVal.substring(0, eqIdx);
      const attrVal = keyVal.substring(eqIdx + 1);
      if (!attrKey || !attrVal) return;

      if (!keyValuesMap.has(attrKey)) {
        keyValuesMap.set(attrKey, new Map<string, number>());
      }
      const valMap = keyValuesMap.get(attrKey)!;
      valMap.set(attrVal, (valMap.get(attrVal) || 0) + count);
    };

    // Selected attribute keys
    for (const selKey of selectedKeys) {
      const selAgg = aggregations?.[`attr_sel_${selKey}`] as
        AggWithValues<{ key: string; doc_count: number }> | undefined;
      for (const b of selAgg?.values?.buckets || []) {
        addBucket(String(b.key), b.doc_count);
      }
    }

    // attr_rest
    const restAgg = aggregations?.['attr_rest'] as
      AggWithValues<{ key: string; doc_count: number }> | undefined;
    for (const b of restAgg?.values?.buckets || []) {
      addBucket(String(b.key), b.doc_count);
    }

    // Keeps the MAX_ATTRIBUTE_FACET_KEYS (10) keys with the highest total count, <= MAX_ATTRIBUTE_FACET_VALUES (30) values each, values sorted by count desc
    const keyTotals: Array<{ key: string; total: number }> = [];
    for (const [k, valMap] of keyValuesMap.entries()) {
      let total = 0;
      for (const cnt of valMap.values()) {
        total += cnt;
      }
      keyTotals.push({ key: k, total });
    }

    keyTotals.sort((a, b) => {
      if (b.total !== a.total) return b.total - a.total;
      return a.key.localeCompare(b.key);
    });

    const topKeys = keyTotals.slice(0, MAX_ATTRIBUTE_FACET_KEYS);
    const attributesFacets: Record<string, AttributeFacetItem[]> = {};

    for (const { key } of topKeys) {
      const valMap = keyValuesMap.get(key)!;
      const items: AttributeFacetItem[] = [];
      for (const [v, cnt] of valMap.entries()) {
        if (cnt > 0) {
          items.push({ value: v, count: cnt });
        }
      }
      items.sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        return a.value.localeCompare(b.value);
      });
      attributesFacets[key] = items.slice(0, MAX_ATTRIBUTE_FACET_VALUES);
    }

    return attributesFacets;
  }
}
