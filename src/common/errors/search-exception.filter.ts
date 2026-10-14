import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  NotFoundException,
} from '@nestjs/common';
import { Response } from 'express';
import { RequestContext } from '../context/request-context';
import { ERROR_MESSAGES_MAP, SearchErrorCode } from './error-codes';
import { SearchException } from './search.exception';

export interface ErrorEnvelope {
  error: {
    code: string;
    message: string;
    details: unknown;
    trace_id: string;
  };
}

@Catch()
export class SearchExceptionFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost) {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();

    const traceId = RequestContext.getTraceId() || 'unknown';

    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let code: string = SearchErrorCode.SEARCH_INTERNAL_ERROR;
    let message: string =
      ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INTERNAL_ERROR];
    let details: unknown = null;

    if (exception instanceof SearchException) {
      status = exception.getStatus();
      code = exception.code;
      message = exception.message;
      details = exception.details;
    } else if (exception instanceof NotFoundException) {
      status = HttpStatus.NOT_FOUND;
      code = 'NOT_FOUND';
      const res = exception.getResponse();
      message =
        typeof res === 'object' && res && 'message' in res
          ? String((res as { message: unknown }).message)
          : 'Not Found';
      details = null;
    } else if (exception instanceof HttpException) {
      status = exception.getStatus();
      const res = exception.getResponse();

      if (status === HttpStatus.BAD_REQUEST) {
        code = SearchErrorCode.SEARCH_INVALID_INPUT;
        message = ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT];
        if (typeof res === 'object' && res && 'message' in res) {
          details = (res as { message: unknown }).message;
        }
      } else if (status === HttpStatus.FORBIDDEN) {
        code = SearchErrorCode.SEARCH_ADMIN_FORBIDDEN;
        message = ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_ADMIN_FORBIDDEN];
      } else if (status === HttpStatus.CONFLICT) {
        code = SearchErrorCode.SEARCH_REINDEX_CONFLICT;
        message = ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_REINDEX_CONFLICT];
      } else {
        if (typeof res === 'object' && res && 'code' in res) {
          code = String((res as { code: unknown }).code);
        }
        if (typeof res === 'object' && res && 'message' in res) {
          message = String((res as { message: unknown }).message);
        } else {
          message = exception.message;
        }
      }
    } else {
      // Unknown error -> SEARCH_INTERNAL_ERROR 500, never leak stack/DSL/token
      status = HttpStatus.INTERNAL_SERVER_ERROR;
      code = SearchErrorCode.SEARCH_INTERNAL_ERROR;
      message = ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INTERNAL_ERROR];
      details = null;
    }

    const payload: ErrorEnvelope = {
      error: {
        code,
        message,
        details,
        trace_id: traceId,
      },
    };

    response.status(status).json(payload);
  }
}
