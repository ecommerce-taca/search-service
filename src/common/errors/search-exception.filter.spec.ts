import {
  ArgumentsHost,
  BadRequestException,
  NotFoundException,
} from '@nestjs/common';
import { Request, Response } from 'express';
import { RequestContext } from '../context/request-context';
import { ERROR_MESSAGES_MAP, SearchErrorCode } from './error-codes';
import { SearchException } from './search.exception';
import { SearchExceptionFilter } from './search-exception.filter';

describe('SearchExceptionFilter', () => {
  let filter: SearchExceptionFilter;
  let mockResponse: Partial<Response>;
  let mockHost: ArgumentsHost;
  let responseJson: jest.Mock;
  let responseStatus: jest.Mock;

  beforeEach(() => {
    filter = new SearchExceptionFilter();
    responseJson = jest.fn();
    responseStatus = jest.fn().mockReturnValue({ json: responseJson });
    mockResponse = {
      status: responseStatus,
    };
    mockHost = {
      switchToHttp: () => ({
        getResponse: () => mockResponse as Response,
        getRequest: () => ({}) as unknown as Request,
        getNext: () => ({}) as unknown as () => void,
      }),
    } as unknown as ArgumentsHost;
  });

  it('should format SearchException properly with exact message and status', () => {
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
      },
    );
  });

  it('should handle NotFoundException with status 404 and envelope', () => {
    RequestContext.run(
      {
        requestId: 'req-123',
        traceId: 'trace-notfound',
        spanId: 'span-1',
        traceparent: '00-trace-notfound-span-1-01',
        actor: { roles: [], permissions: [], mfaStepUp: false },
      },
      () => {
        const exception = new NotFoundException('Cannot GET /api/v1/unknown');
        filter.catch(exception, mockHost);

        expect(responseStatus).toHaveBeenCalledWith(404);
        expect(responseJson).toHaveBeenCalledWith({
          error: {
            code: 'NOT_FOUND',
            message: 'Cannot GET /api/v1/unknown',
            details: null,
            trace_id: 'trace-notfound',
          },
        });
      },
    );
  });

  it('should map unknown error to SEARCH_INTERNAL_ERROR 500 without leaking stack or secrets', () => {
    RequestContext.run(
      {
        requestId: 'req-123',
        traceId: 'trace-unknown',
        spanId: 'span-2',
        traceparent: '00-trace-unknown-span-2-01',
        actor: { roles: [], permissions: [], mfaStepUp: false },
      },
      () => {
        const exception = new Error(
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
      },
    );
  });
});
