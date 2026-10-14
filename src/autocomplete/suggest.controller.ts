import { Controller, Get, Logger, Query } from '@nestjs/common';
import { ApiResponse, createResponse } from '../common/response.helper';
import { SuggestQueryDto } from './dto/suggest.query';
import { SuggestService, Suggestion } from './suggest.service';

@Controller('search')
export class SuggestController {
  private readonly logger = new Logger(SuggestController.name);

  constructor(private readonly suggestService: SuggestService) {}

  @Get('suggest')
  async suggest(
    @Query() query: SuggestQueryDto,
  ): Promise<ApiResponse<Suggestion[]>> {
    const prefixLength = query?.prefix ? Array.from(query.prefix).length : 0;

    this.logger.debug({
      event: 'suggest_request',
      prefix_len: prefixLength,
      limit: query?.limit,
    });

    const suggestions = await this.suggestService.suggest(query);
    return createResponse(suggestions);
  }
}
