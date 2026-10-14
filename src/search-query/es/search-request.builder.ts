import { estypes } from '@elastic/elasticsearch';
import { Injectable } from '@nestjs/common';
import { SortOption } from '../../common/enums';
import {
  ERROR_MESSAGES_MAP,
  SearchErrorCode,
} from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { ParsedSearchQuery } from '../query-parser';

function makeFilterAgg(
  filters: estypes.QueryDslQueryContainer[],
): estypes.QueryDslQueryContainer {
  return {
    bool: {
      filter: [{ term: { visibility_status: 'PUBLISHED' } }, ...filters],
    },
  };
}

function buildSort(sort: SortOption): estypes.Sort {
  switch (sort) {
    case SortOption.RELEVANCE:
      return [{ _score: { order: 'desc' } }, { product_id: { order: 'asc' } }];
    case SortOption.NEWEST:
      return [
        { created_at: { order: 'desc' } },
        { product_id: { order: 'asc' } },
      ];
    case SortOption.PRICE_ASC:
      return [{ price: { order: 'asc' } }, { product_id: { order: 'asc' } }];
    case SortOption.PRICE_DESC:
      return [{ price: { order: 'desc' } }, { product_id: { order: 'asc' } }];
    case SortOption.RATING_DESC:
      return [
        { rating_avg: { order: 'desc' } },
        { rating_count: { order: 'desc' } },
        { product_id: { order: 'asc' } },
      ];
  }
}

@Injectable()
export class SearchRequestBuilder {
  build(parsed: ParsedSearchQuery): estypes.SearchRequest {
    return SearchRequestBuilder.build(parsed);
  }

  static build(parsed: ParsedSearchQuery): estypes.SearchRequest {
    if (parsed.page * parsed.size > 10000) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        ['page * size cannot exceed 10000'],
      );
    }

    // 1. Base query: bool.filter PUBLISHED + optional full-text
    const boolQuery: estypes.QueryDslBoolQuery = {
      filter: [{ term: { visibility_status: 'PUBLISHED' } }],
    };

    if (parsed.text) {
      boolQuery.must = [
        {
          multi_match: {
            query: parsed.text,
            fields: ['title^3', 'brand^2', 'description'],
            operator: 'and',
          },
        },
      ];
      boolQuery.should = [
        {
          term: {
            title_exact: {
              value: parsed.text,
              boost: 5,
            },
          },
        },
      ];
    }

    const query: estypes.QueryDslQueryContainer = {
      bool: boolQuery,
    };

    // 2. Build individual filter clauses
    let categoryFilter: estypes.QueryDslQueryContainer | undefined;
    if (parsed.filters.categoryId) {
      categoryFilter = { term: { category_ids: parsed.filters.categoryId } };
    }

    let shopFilter: estypes.QueryDslQueryContainer | undefined;
    if (parsed.filters.shopId) {
      shopFilter = { term: { shop_id: parsed.filters.shopId } };
    }

    let brandFilter: estypes.QueryDslQueryContainer | undefined;
    if (parsed.filters.brands.length === 1) {
      brandFilter = { term: { brand: parsed.filters.brands[0] } };
    } else if (parsed.filters.brands.length > 1) {
      brandFilter = { terms: { brand: parsed.filters.brands } };
    }

    let priceFilter: estypes.QueryDslQueryContainer | undefined;
    if (
      parsed.filters.minPrice !== undefined ||
      parsed.filters.maxPrice !== undefined
    ) {
      const range: Record<string, number> = {};
      if (parsed.filters.minPrice !== undefined) {
        range.gte = parsed.filters.minPrice;
      }
      if (parsed.filters.maxPrice !== undefined) {
        range.lte = parsed.filters.maxPrice;
      }
      priceFilter = { range: { price: range } };
    }

    const attrFilters: Record<string, estypes.QueryDslQueryContainer> = {};
    for (const [k, vals] of Object.entries(parsed.filters.attributes)) {
      if (vals && vals.length > 0) {
        if (vals.length === 1) {
          attrFilters[k] = { term: { attribute_pairs: `${k}=${vals[0]}` } };
        } else {
          attrFilters[k] = {
            terms: { attribute_pairs: vals.map((v) => `${k}=${v}`) },
          };
        }
      }
    }

    // 3. Post filter: combines all selected filters
    const allSelectedFilters: estypes.QueryDslQueryContainer[] = [];
    if (categoryFilter) allSelectedFilters.push(categoryFilter);
    if (shopFilter) allSelectedFilters.push(shopFilter);
    if (brandFilter) allSelectedFilters.push(brandFilter);
    if (priceFilter) allSelectedFilters.push(priceFilter);
    for (const clause of Object.values(attrFilters)) {
      allSelectedFilters.push(clause);
    }

    let postFilter: estypes.QueryDslQueryContainer | undefined;
    if (allSelectedFilters.length === 1) {
      postFilter = allSelectedFilters[0];
    } else if (allSelectedFilters.length > 1) {
      postFilter = {
        bool: {
          filter: allSelectedFilters,
        },
      };
    }

    // 4. Facets aggregations (omitted when includeFacets = false)
    let aggs:
      Record<string, estypes.AggregationsAggregationContainer> | undefined;

    if (parsed.includeFacets) {
      const nonCategoryFilters = [
        shopFilter,
        brandFilter,
        priceFilter,
        ...Object.values(attrFilters),
      ].filter(Boolean) as estypes.QueryDslQueryContainer[];

      const nonBrandFilters = [
        categoryFilter,
        shopFilter,
        priceFilter,
        ...Object.values(attrFilters),
      ].filter(Boolean) as estypes.QueryDslQueryContainer[];

      const nonPriceFilters = [
        categoryFilter,
        shopFilter,
        brandFilter,
        ...Object.values(attrFilters),
      ].filter(Boolean) as estypes.QueryDslQueryContainer[];

      aggs = {
        category: {
          filter: makeFilterAgg(nonCategoryFilters),
          aggs: {
            values: {
              terms: {
                field: 'category_ids',
                size: 200,
              },
            },
          },
        },
        brand: {
          filter: makeFilterAgg(nonBrandFilters),
          aggs: {
            values: {
              terms: {
                field: 'brand',
                size: 50,
              },
            },
          },
        },
        price: {
          filter: makeFilterAgg(nonPriceFilters),
          aggs: {
            values: {
              range: {
                field: 'price',
                ranges: [
                  { to: 5000000 },
                  { from: 5000000, to: 20000000 },
                  { from: 20000000 },
                ],
              },
            },
          },
        },
        index_as_of: {
          max: {
            field: 'indexed_at',
          },
        },
      };

      // Attribute facets
      const selectedKeys = Object.keys(attrFilters);
      for (const selKey of selectedKeys) {
        const otherAttrFilters = Object.entries(attrFilters)
          .filter(([key]) => key !== selKey)
          .map(([, f]) => f);
        const nonKFilters = [
          categoryFilter,
          shopFilter,
          brandFilter,
          priceFilter,
          ...otherAttrFilters,
        ].filter(Boolean) as estypes.QueryDslQueryContainer[];

        aggs[`attr_sel_${selKey}`] = {
          filter: makeFilterAgg(nonKFilters),
          aggs: {
            values: {
              terms: {
                field: 'attribute_pairs',
                include: `^${selKey}=.*`,
                size: 30,
              },
            },
          },
        };
      }

      const excludeRegex =
        selectedKeys.length > 0 ? `^(${selectedKeys.join('|')})=.*` : undefined;

      aggs['attr_rest'] = {
        filter: makeFilterAgg(allSelectedFilters),
        aggs: {
          values: {
            terms: {
              field: 'attribute_pairs',
              ...(excludeRegex ? { exclude: excludeRegex } : {}),
              size: 300,
            },
          },
        },
      };
    }

    const searchRequest: estypes.SearchRequest = {
      query,
      sort: buildSort(parsed.sort),
      from: (parsed.page - 1) * parsed.size,
      size: parsed.size,
      track_total_hits: true,
      _source: [
        'product_id',
        'title',
        'slug',
        'shop',
        'shop_id',
        'shop_name',
        'price',
        'currency',
        'cover_url',
        'rating_avg',
      ],
    };

    if (postFilter) {
      searchRequest.post_filter = postFilter;
    }
    if (aggs) {
      searchRequest.aggs = aggs;
    }

    return searchRequest;
  }
}

export const ElasticsearchQueryBuilder = SearchRequestBuilder;
export type ElasticsearchQueryBuilder = SearchRequestBuilder;
