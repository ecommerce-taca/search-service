import { HttpException, HttpStatus } from '@nestjs/common';
import {
  ERROR_HTTP_STATUS_MAP,
  ERROR_MESSAGES_MAP,
  SearchErrorCode,
} from './error-codes';

export class SearchException extends HttpException {
  public readonly code: SearchErrorCode;
  public readonly details: unknown;

  constructor(
    code: SearchErrorCode,
    message?: string,
    details: unknown = null,
    statusCode?: number,
  ) {
    const status =
      statusCode ??
      ERROR_HTTP_STATUS_MAP[code] ??
      HttpStatus.INTERNAL_SERVER_ERROR;
    const msg = message ?? ERROR_MESSAGES_MAP[code] ?? 'Internal Error';
    super(
      {
        code,
        message: msg,
        details,
      },
      status,
    );
    this.code = code;
    this.details = details;
  }
}
