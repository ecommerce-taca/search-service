import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { Request, Response } from 'express';
import { RequestContext } from '../context/request-context';
import { resolveRouteTemplate } from '../../observability/route-template.helper';
import {
  ERROR_CODE_NOT_FOUND,
  ERROR_MESSAGES_MAP,
  SearchErrorCode,
} from './error-codes';
import { SearchException } from './search.exception';

export interface ErrorEnvelope {
  error: {
    code: string;
    message: string;
    details: unknown;
    trace_id: string;
  };
}

function isEsOrKafkaError(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const candidate = err as {
    name?: string;
    meta?: { body?: unknown; statusCode?: number };
    [key: string]: unknown;
  };
  if (
    candidate.name === 'ResponseError' ||
    candidate.name === 'ConnectionError' ||
    candidate.name === 'RequestAbortedError' ||
    candidate.name === 'ProductNotSupportedError' ||
    Boolean(candidate.meta?.body) ||
    (typeof candidate.name === 'string' &&
      candidate.name.toLowerCase().includes('kafka')) ||
    (typeof candidate.name === 'string' &&
      candidate.name.toLowerCase().includes('elastic'))
  ) {
    return true;
  }
  return false;
}

@Catch()
export class SearchExceptionFilter implements ExceptionFilter {
  private readonly logger: Logger;

  constructor(logger?: Logger) {
    this.logger = logger || new Logger(SearchExceptionFilter.name);
  }

  catch(exception: unknown, host: ArgumentsHost) {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();

    const traceId = RequestContext.getTraceId() || 'unknown';
    const requestId =
      RequestContext.getRequestId() ||
      (request?.headers?.['x-request-id'] as string) ||
      'unknown';
    const route = resolveRouteTemplate(request);

    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let code: string = SearchErrorCode.SEARCH_INTERNAL_ERROR;
    let message: string =
      ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INTERNAL_ERROR];
    let details: unknown = null;
    let isUnknownError = false;

    if (exception instanceof SearchException) {
      status = exception.getStatus();
      code = exception.code;
      message = exception.message;
      details = exception.details;
    } else if (exception instanceof NotFoundException) {
      status = HttpStatus.NOT_FOUND;
      code = ERROR_CODE_NOT_FOUND;
      message = 'Không tìm thấy tài nguyên.';
      details = null;
    } else if (exception instanceof HttpException) {
      status = exception.getStatus();
      const res = exception.getResponse();

      if (status >= HttpStatus.INTERNAL_SERVER_ERROR) {
        code = SearchErrorCode.SEARCH_INTERNAL_ERROR;
        message = ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INTERNAL_ERROR];
        details = null;
      } else if (status === HttpStatus.BAD_REQUEST) {
        code = SearchErrorCode.SEARCH_INVALID_INPUT;
        message = ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT];
        if (typeof res === 'object' && res && 'message' in res) {
          details = (res as { message: unknown }).message;
        }
      } else if (status === HttpStatus.FORBIDDEN) {
        code = SearchErrorCode.SEARCH_ADMIN_FORBIDDEN;
        message = ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_ADMIN_FORBIDDEN];
        details = null;
      } else if (status === HttpStatus.NOT_FOUND) {
        code = ERROR_CODE_NOT_FOUND;
        message = 'Không tìm thấy tài nguyên.';
        details = null;
      } else if (status === HttpStatus.CONFLICT) {
        code = SearchErrorCode.SEARCH_REINDEX_CONFLICT;
        message = ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_REINDEX_CONFLICT];
        details = null;
      } else {
        // Other 4xx
        if (typeof res === 'object' && res && 'code' in res) {
          code = String((res as { code: unknown }).code);
        } else if (exception.name && exception.name !== 'HttpException') {
          code = exception.name.replace(/Exception$/, '').toUpperCase();
        } else {
          code = HttpStatus[status] || 'HTTP_ERROR';
        }

        let candidateMsg = '';
        if (typeof res === 'object' && res && 'message' in res) {
          const rawMsg = (res as { message: unknown }).message;
          candidateMsg = Array.isArray(rawMsg)
            ? rawMsg.join(', ')
            : String(rawMsg);
        } else if (typeof res === 'string') {
          candidateMsg = res;
        }

        const rawPath = request?.path || request?.url || '';
        if (candidateMsg && (!rawPath || !candidateMsg.includes(rawPath))) {
          message = candidateMsg;
        } else {
          message = 'Yêu cầu không hợp lệ.';
        }
        details = null;
      }
    } else {
      // Unknown error -> SEARCH_INTERNAL_ERROR 500, never leak stack/DSL/token
      status = HttpStatus.INTERNAL_SERVER_ERROR;
      code = SearchErrorCode.SEARCH_INTERNAL_ERROR;
      message = ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INTERNAL_ERROR];
      details = null;
      isUnknownError = true;
    }

    // Logging: 5xx as error, 4xx as warn
    const logPayload: Record<string, unknown> = {
      event: 'http_exception',
      code,
      status_code: status,
      trace_id: traceId,
      request_id: requestId,
      route,
    };

    if (isUnknownError) {
      const errorClassName =
        (exception as object)?.constructor?.name || typeof exception;
      logPayload.error_class = errorClassName;
      if (
        !isEsOrKafkaError(exception) &&
        exception instanceof Error &&
        exception.message
      ) {
        logPayload.error_message = exception.message;
      }
    } else if (
      !isEsOrKafkaError(exception) &&
      exception instanceof Error &&
      exception.message
    ) {
      logPayload.error_message = exception.message;
    }

    if (status >= HttpStatus.INTERNAL_SERVER_ERROR) {
      this.logger.error(logPayload);
    } else if (status >= HttpStatus.BAD_REQUEST) {
      this.logger.warn(logPayload);
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
