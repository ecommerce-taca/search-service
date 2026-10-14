import {
  BadRequestException,
  Catch,
  Controller,
  ExceptionFilter,
  Get,
  Logger,
  Query,
  UseFilters,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { ApiResponse, createResponse } from '../common/response.helper';
import { SuggestQueryDto } from './dto/suggest.query';
import { SuggestService, Suggestion } from './suggest.service';

@Catch(BadRequestException)
export class SuggestValidationFilter implements ExceptionFilter {
  catch(exception: BadRequestException) {
    const res = exception.getResponse();
    const rawMessages =
      typeof res === 'object' && res && 'message' in res
        ? (res as { message: unknown }).message
        : res;

    const messages = Array.isArray(rawMessages)
      ? rawMessages
      : [String(rawMessages)];

    const isMissing = messages.some(
      (m: unknown) => typeof m === 'string' && m.includes('prefix is required'),
    );
    if (isMissing) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        undefined,
        "Query parameter 'prefix' is required.",
      );
    }

    const isEmpty = messages.some(
      (m: unknown) =>
        typeof m === 'string' && m.includes('prefix cannot be empty'),
    );
    if (isEmpty) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        undefined,
        "Query parameter 'prefix' cannot be empty.",
      );
    }

    const isLimit = messages.some(
      (m: unknown) =>
        typeof m === 'string' && m.includes('limit must not exceed 10'),
    );
    if (isLimit) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        undefined,
        'Limit cannot exceed 10.',
      );
    }

    const isTooLong = messages.some(
      (m: unknown) =>
        typeof m === 'string' &&
        (m.includes('exceeds max length of 100 characters') ||
          m.includes('shorter than or equal to 100')),
    );
    if (isTooLong) {
      throw new SearchException(
        SearchErrorCode.SEARCH_QUERY_TOO_LONG,
        undefined,
        "Query parameter 'prefix' exceeds 100 characters.",
      );
    }

    throw new SearchException(
      SearchErrorCode.SEARCH_INVALID_INPUT,
      undefined,
      messages.join(', '),
    );
  }
}

@Controller('search')
@UseFilters(SuggestValidationFilter)
export class SuggestController {
  private readonly logger = new Logger(SuggestController.name);

  constructor(private readonly suggestService: SuggestService) {}

  @Get('suggest')
  @UsePipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
      forbidNonWhitelisted: true,
    }),
  )
  async suggest(
    @Query() query: SuggestQueryDto,
  ): Promise<ApiResponse<Suggestion[]>> {
    const prefixLength = query?.prefix ? Array.from(query.prefix).length : 0;

    this.logger.debug({
      event: 'suggest_request',
      prefix_len: prefixLength,
      limit: query?.limit,
      category_id: query?.category_id,
    });

    const suggestions = await this.suggestService.suggest(query);
    return createResponse(suggestions);
  }
}
