import { Injectable, NestMiddleware } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';
import {
  parseActor,
  parseOrCreateRequestId,
  parseOrCreateTrace,
  RequestContext,
  RequestContextData,
} from './request-context';

@Injectable()
export class RequestContextMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction): void {
    const requestId = parseOrCreateRequestId(req.headers['x-request-id']);
    const trace = parseOrCreateTrace(
      req.headers['traceparent'],
      req.headers['tracestate'],
    );
    const actor = parseActor(req.headers);

    const contextData: RequestContextData = {
      requestId,
      traceId: trace.traceId,
      spanId: trace.spanId,
      parentSpanId: trace.parentSpanId,
      parent_span_id: trace.parent_span_id,
      traceparent: trace.traceparent,
      tracestate: trace.tracestate,
      actor,
    };

    // Echo X-Request-ID response header
    res.setHeader('X-Request-ID', requestId);

    RequestContext.run(contextData, () => {
      next();
    });
  }
}
