import { Controller, Get, Query } from '@nestjs/common';
import { createResponse } from '../common/response.helper';
import { SearchProductsQueryDto } from './dto/search-products.query';
import { QueryParser } from './query-parser';
import { SearchProductsService } from './search-products.service';

@Controller('products')
export class SearchController {
  constructor(
    private readonly searchProductsService: SearchProductsService,
    private readonly queryParser: QueryParser,
  ) {}

  @Get('search')
  async search(@Query() query: SearchProductsQueryDto) {
    const parsed = this.queryParser.parse(query);
    const result = await this.searchProductsService.search(parsed);
    return createResponse(result.data, result.meta);
  }
}
