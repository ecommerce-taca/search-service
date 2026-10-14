import { Client, estypes } from '@elastic/elasticsearch';
import { Inject, Injectable } from '@nestjs/common';
import { ES_REQUEST_TIMEOUT_MS, getProductsAlias } from '../common/constants';
import { toSearchException } from '../common/es';
import { AppConfigService } from '../config/config.service';
import { ProductSearchDocument } from '../product-index/interfaces';
import { SearchRequestBuilder } from './es/search-request.builder';
import { FacetMapper } from './facet.mapper';
import { ParsedSearchQuery } from './query-parser';
import { SearchResult, SearchResultMapper } from './result.mapper';

@Injectable()
export class SearchProductsService {
  constructor(
    @Inject(Client) private readonly esClient: Client,
    private readonly configService: AppConfigService,
    private readonly requestBuilder: SearchRequestBuilder,
    private readonly resultMapper: SearchResultMapper,
    private readonly facetMapper: FacetMapper,
  ) {}

  async search(parsed: ParsedSearchQuery): Promise<SearchResult> {
    const alias = getProductsAlias(this.configService.esIndexPrefix);
    const esRequest = this.requestBuilder.build(parsed);

    const response = await this.runQuery(alias, esRequest);
    const indexAsOf = this.readIndexAsOf(response.aggregations);

    if (!parsed.includeFacets) {
      return this.resultMapper.map(response, parsed, undefined, indexAsOf);
    }

    const facets = await this.facetMapper.mapFacets(
      response.aggregations,
      parsed.filters.categoryId,
      Object.keys(parsed.filters.attributes),
    );

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
    const maxAgg = aggregations.index_as_of as estypes.AggregationsMaxAggregate;
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
}
