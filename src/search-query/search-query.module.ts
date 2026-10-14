import { Module } from '@nestjs/common';
import { SearchRequestBuilder } from './es/search-request.builder';
import { FacetMapper } from './facet.mapper';
import { QueryParser } from './query-parser';
import { SearchResultMapper } from './result.mapper';
import { SearchProductsService } from './search-products.service';
import { SearchController } from './search.controller';

@Module({
  controllers: [SearchController],
  providers: [
    QueryParser,
    SearchRequestBuilder,
    SearchResultMapper,
    FacetMapper,
    SearchProductsService,
  ],
  exports: [
    QueryParser,
    SearchRequestBuilder,
    SearchResultMapper,
    FacetMapper,
    SearchProductsService,
  ],
})
export class SearchQueryModule {}
