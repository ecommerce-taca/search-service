import { Client, estypes } from '@elastic/elasticsearch';
import { Inject, Injectable } from '@nestjs/common';
import { ES_REQUEST_TIMEOUT_MS, getProductsAlias } from '../common/constants';
import { toSearchException } from '../common/es';
import { AppConfigService } from '../config/config.service';
import {
  CATEGORY_PROJECTION,
  ICategoryProjection,
  ProductSearchDocument,
} from '../product-index/interfaces';
import { SearchRequestBuilder } from './es/search-request.builder';
import { ParsedSearchQuery } from './query-parser';
import {
  AttributeFacetItem,
  BrandFacetItem,
  CategoryFacetItem,
  PriceFacetBucket,
  SearchFacets,
  SearchResult,
  SearchResultMapper,
} from './result.mapper';

interface AggWithValues<T> {
  values?: {
    buckets?: T[];
  };
}

@Injectable()
export class SearchProductsService {
  constructor(
    @Inject(Client) private readonly esClient: Client,
    private readonly configService: AppConfigService,
    @Inject(CATEGORY_PROJECTION)
    private readonly categoryProjection: ICategoryProjection,
    private readonly requestBuilder: SearchRequestBuilder,
    private readonly resultMapper: SearchResultMapper,
  ) {}

  async search(parsed: ParsedSearchQuery): Promise<SearchResult> {
    const alias = getProductsAlias(this.configService.esIndexPrefix);
    const esRequest = this.requestBuilder.build(parsed);

    const response = await this.runQuery(alias, esRequest);
    const indexAsOf = this.readIndexAsOf(response.aggregations);

    if (!parsed.includeFacets) {
      return this.resultMapper.map(response, parsed, undefined, indexAsOf);
    }

    const aggs = response.aggregations;
    const categoryAgg = aggs?.category as
      AggWithValues<{ key: string; doc_count: number }> | undefined;
    const rawCategoryBuckets = categoryAgg?.values?.buckets || [];

    const brandAgg = aggs?.brand as
      AggWithValues<{ key: string; doc_count: number }> | undefined;
    const rawBrandBuckets = brandAgg?.values?.buckets || [];

    const priceAgg = aggs?.price as
      | AggWithValues<{ from?: number; to?: number; doc_count: number }>
      | undefined;
    const rawPriceBuckets = priceAgg?.values?.buckets || [];

    const categoryItems = await this.buildCategoryFacet(
      rawCategoryBuckets,
      parsed.filters.categoryId,
    );
    const brandItems = this.buildBrandFacet(rawBrandBuckets);
    const priceItems = this.buildPriceFacet(rawPriceBuckets);
    const attributeFacets = this.buildAttributeFacets(
      aggs,
      Object.keys(parsed.filters.attributes),
    );

    const facets: SearchFacets = {
      category: categoryItems,
      brand: brandItems,
      price: priceItems,
      attributes: attributeFacets,
    };

    return this.resultMapper.map(response, parsed, facets, indexAsOf);
  }

  private async runQuery(
    alias: string,
    esRequest: estypes.SearchRequest,
  ): Promise<estypes.SearchResponse<ProductSearchDocument>> {
    try {
      return await this.esClient.search<ProductSearchDocument>(
        {
          index: alias,
          ...esRequest,
        },
        {
          requestTimeout: ES_REQUEST_TIMEOUT_MS,
        },
      );
    } catch (err: unknown) {
      throw toSearchException(err);
    }
  }

  private readIndexAsOf(
    aggregations?: Record<string, estypes.AggregationsAggregate>,
  ): string | null {
    if (!aggregations?.index_as_of) {
      return null;
    }
    const maxAgg = aggregations.index_as_of as {
      value_as_string?: string;
      value?: number | null;
    };
    if (maxAgg.value_as_string) {
      return maxAgg.value_as_string;
    }
    if (
      typeof maxAgg.value === 'number' &&
      !isNaN(maxAgg.value) &&
      isFinite(maxAgg.value) &&
      maxAgg.value > 0
    ) {
      return new Date(maxAgg.value).toISOString();
    }
    return null;
  }

  private async buildCategoryFacet(
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

  private buildBrandFacet(
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

  private buildPriceFacet(
    rawPriceBuckets: Array<{ doc_count: number }>,
  ): PriceFacetBucket[] {
    return [
      {
        min: 0,
        max: 5000000,
        count: rawPriceBuckets[0]?.doc_count ?? 0,
      },
      {
        min: 5000000,
        max: 20000000,
        count: rawPriceBuckets[1]?.doc_count ?? 0,
      },
      {
        min: 20000000,
        max: null,
        count: rawPriceBuckets[2]?.doc_count ?? 0,
      },
    ];
  }

  private buildAttributeFacets(
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

    // Keeps the 10 keys with the highest total count, <= 30 values each, values sorted by count desc
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

    const top10Keys = keyTotals.slice(0, 10);
    const attributesFacets: Record<string, AttributeFacetItem[]> = {};

    for (const { key } of top10Keys) {
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
      attributesFacets[key] = items.slice(0, 30);
    }

    return attributesFacets;
  }
}
