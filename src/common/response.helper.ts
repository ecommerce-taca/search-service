import { RequestContext } from './context/request-context';

export interface ResponseMeta {
  request_id: string;
  [key: string]: unknown;
}

export interface ApiResponse<T> {
  data: T;
  meta: ResponseMeta;
}

export function createResponse<T>(
  data: T,
  extraMeta: Record<string, unknown> = {},
): ApiResponse<T> {
  const requestId = RequestContext.getRequestId() || '';
  return {
    data,
    meta: {
      request_id: requestId,
      ...extraMeta,
    },
  };
}
