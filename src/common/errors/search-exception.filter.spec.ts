import {
  ArgumentsHost,
  BadRequestException,
  HttpException,
  HttpStatus,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { Request, Response } from 'express';
import { RequestContext } from '../context/request-context';
import {
  ERROR_CODE_NOT_FOUND,
  ERROR_MESSAGES_MAP,
  SearchErrorCode,
} from './error-codes';
import { SearchException } from './search.exception';
import { SearchExceptionFilter } from './search-exception.filter';

describe('SearchExceptionFilter', () => {
  let filter: SearchExceptionFilter;
  let mockResponse: Partial<Response>;
  let mockRequest: {
    path?: string;
    url?: string;
    headers?: Record<string, string>;
  };
  let mockHost: ArgumentsHost;
  let responseJson: jest.Mock;
  let responseStatus: jest.Mock;
  let loggerErrorSpy: jest.SpyInstance;
  let loggerWarnSpy: jest.SpyInstance;

  beforeEach(() => {
    filter = new SearchExceptionFilter();
    loggerErrorSpy = jest
      .spyOn(Logger.prototype, 'error')
      .mockImplementation(() => {});
    loggerWarnSpy = jest
      .spyOn(Logger.prototype, 'warn')
      .mockImplementation(() => {});

    responseJson = jest.fn();
    responseStatus = jest.fn().mockReturnValue({ json: responseJson });
    mockResponse = {
      status: responseStatus,
    };
    mockRequest = {
      path: '/api/v1/test',
      url: '/api/v1/test',
      headers: {
        'x-request-id': 'req-header-123',
      },
    };
    mockHost = {
      switchToHttp: () => ({
        getResponse: () => mockResponse as Response,
        getRequest: () => mockRequest as unknown as Request,
        getNext: () => ({}) as unknown as () => void,
      }),
    } as unknown as ArgumentsHost;
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('should format SearchException properly with exact message and status, and log warn', () => {
    RequestContext.run(
      {
        requestId: 'req-123',
        traceId: 'trace-456',
        spanId: 'span-789',
        traceparent: '00-trace-456-span-789-01',
        actor: { roles: [], permissions: [], mfaStepUp: false },
      },
      () => {
        const exception = new SearchException(
          SearchErrorCode.SEARCH_ADMIN_FORBIDDEN,
        );
        filter.catch(exception, mockHost);

        expect(responseStatus).toHaveBeenCalledWith(403);
        expect(responseJson).toHaveBeenCalledWith({
          error: {
            code: SearchErrorCode.SEARCH_ADMIN_FORBIDDEN,
            message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_ADMIN_FORBIDDEN],
            details: null,
            trace_id: 'trace-456',
          },
        });

        expect(loggerWarnSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            event: 'http_exception',
            code: SearchErrorCode.SEARCH_ADMIN_FORBIDDEN,
            status_code: 403,
            trace_id: 'trace-456',
            request_id: 'req-123',
            route: 'unmatched',
          }),
        );
      },
    );
  });

  it('should map BadRequestException to SEARCH_INVALID_INPUT 400', () => {
    RequestContext.run(
      {
        requestId: 'req-123',
        traceId: 'trace-abc',
        spanId: 'span-def',
        traceparent: '00-trace-abc-span-def-01',
        actor: { roles: [], permissions: [], mfaStepUp: false },
      },
      () => {
        const exception = new BadRequestException(['field is required']);
        filter.catch(exception, mockHost);

        expect(responseStatus).toHaveBeenCalledWith(400);
        expect(responseJson).toHaveBeenCalledWith({
          error: {
            code: SearchErrorCode.SEARCH_INVALID_INPUT,
            message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
            details: ['field is required'],
            trace_id: 'trace-abc',
          },
        });

        expect(loggerWarnSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            event: 'http_exception',
            code: SearchErrorCode.SEARCH_INVALID_INPUT,
            status_code: 400,
          }),
        );
      },
    );
  });

  it('should handle NotFoundException with status 404, code NOT_FOUND, fixed message, and never echo raw path', () => {
    RequestContext.run(
      {
        requestId: 'req-123',
        traceId: 'trace-notfound',
        spanId: 'span-1',
        traceparent: '00-trace-notfound-span-1-01',
        actor: { roles: [], permissions: [], mfaStepUp: false },
      },
      () => {
        const rawPath = '/api/v1/unknown-secret-path';
        mockRequest.path = rawPath;
        const exception = new NotFoundException(`Cannot GET ${rawPath}`);
        filter.catch(exception, mockHost);

        expect(responseStatus).toHaveBeenCalledWith(404);
        expect(responseJson).toHaveBeenCalledWith({
          error: {
            code: ERROR_CODE_NOT_FOUND,
            message: 'Không tìm thấy tài nguyên.',
            details: null,
            trace_id: 'trace-notfound',
          },
        });

        expect(responseJson.mock.calls[0][0].error.message).not.toContain(
          rawPath,
        );
      },
    );
  });

  it('should map generic HttpException >= 500 to SEARCH_INTERNAL_ERROR message and code', () => {
    RequestContext.run(
      {
        requestId: 'req-502',
        traceId: 'trace-502',
        spanId: 'span-502',
        traceparent: '00-trace-502-span-502-01',
        actor: { roles: [], permissions: [], mfaStepUp: false },
      },
      () => {
        const exception = new HttpException(
          'Internal upstream server error at http://internal-cluster:9200',
          HttpStatus.BAD_GATEWAY,
        );
        filter.catch(exception, mockHost);

        expect(responseStatus).toHaveBeenCalledWith(502);
        expect(responseJson).toHaveBeenCalledWith({
          error: {
            code: SearchErrorCode.SEARCH_INTERNAL_ERROR,
            message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INTERNAL_ERROR],
            details: null,
            trace_id: 'trace-502',
          },
        });
        expect(loggerErrorSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            event: 'http_exception',
            status_code: 502,
            code: SearchErrorCode.SEARCH_INTERNAL_ERROR,
          }),
        );
      },
    );
  });

  it('should sanitize generic HttpException 4xx if message contains request path', () => {
    RequestContext.run(
      {
        requestId: 'req-400',
        traceId: 'trace-400',
        spanId: 'span-400',
        traceparent: '00-trace-400-span-400-01',
        actor: { roles: [], permissions: [], mfaStepUp: false },
      },
      () => {
        mockRequest.path = '/api/v1/custom-path';
        const exception = new HttpException(
          'Invalid operation on /api/v1/custom-path',
          HttpStatus.UNPROCESSABLE_ENTITY,
        );
        filter.catch(exception, mockHost);

        expect(responseStatus).toHaveBeenCalledWith(422);
        expect(responseJson).toHaveBeenCalledWith({
          error: {
            code: 'UNPROCESSABLE_ENTITY',
            message: 'Yêu cầu không hợp lệ.',
            details: null,
            trace_id: 'trace-400',
          },
        });
      },
    );
  });

  it('should map unknown error to SEARCH_INTERNAL_ERROR 500 with error class name in log and no message in response', () => {
    RequestContext.run(
      {
        requestId: 'req-123',
        traceId: 'trace-unknown',
        spanId: 'span-2',
        traceparent: '00-trace-unknown-span-2-01',
        actor: { roles: [], permissions: [], mfaStepUp: false },
      },
      () => {
        const exception = new TypeError(
          'Database connection secret password leaked',
        );
        filter.catch(exception, mockHost);

        expect(responseStatus).toHaveBeenCalledWith(500);
        expect(responseJson).toHaveBeenCalledWith({
          error: {
            code: SearchErrorCode.SEARCH_INTERNAL_ERROR,
            message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INTERNAL_ERROR],
            details: null,
            trace_id: 'trace-unknown',
          },
        });

        expect(loggerErrorSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            event: 'http_exception',
            code: SearchErrorCode.SEARCH_INTERNAL_ERROR,
            status_code: 500,
            trace_id: 'trace-unknown',
            request_id: 'req-123',
            route: 'unmatched',
            error_class: 'TypeError',
            error_message: 'Database connection secret password leaked',
          }),
        );
      },
    );
  });

  it('should not leak Elasticsearch error body message into log', () => {
    RequestContext.run(
      {
        requestId: 'req-es',
        traceId: 'trace-es',
        spanId: 'span-es',
        traceparent: '00-trace-es-span-es-01',
        actor: { roles: [], permissions: [], mfaStepUp: false },
      },
      () => {
        const esError = new Error(
          'ES cluster connection refused at 192.168.100.23:30920',
        );
        esError.name = 'ResponseError';
        (esError as unknown as { meta: { body: object } }).meta = {
          body: { error: { reason: 'shard failure' } },
        };

        filter.catch(esError, mockHost);

        expect(loggerErrorSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            event: 'http_exception',
            status_code: 500,
            error_class: 'Error',
          }),
        );
        // Error message should NOT be logged for ES error body
        const loggedObj = loggerErrorSpy.mock.calls[0][0];
        expect(loggedObj.error_message).toBeUndefined();
      },
    );
  });
});
