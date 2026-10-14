import { AsyncLocalStorage } from 'async_hooks';
import { randomBytes, randomUUID } from 'crypto';

export interface ActorContext {
  userId?: string;
  roles: string[];
  permissions: string[];
  shopScope?: string;
  mfaStepUp: boolean;
}

export interface RequestContextData {
  requestId: string;
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  parent_span_id?: string;
  traceparent: string;
  tracestate?: string;
  actor: ActorContext;
}

const storage = new AsyncLocalStorage<RequestContextData>();

const TRACEPARENT_REGEX = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/i;

export function parseOrCreateTrace(
  traceparentHeader?: string | string[],
  tracestateHeader?: string | string[],
): {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  parent_span_id?: string;
  traceparent: string;
  tracestate?: string;
} {
  const rawTraceparent = Array.isArray(traceparentHeader)
    ? traceparentHeader[0]
    : traceparentHeader;
  const rawTracestate = Array.isArray(tracestateHeader)
    ? tracestateHeader[0]
    : tracestateHeader;

  let traceId: string;
  let parentSpanId: string | undefined;
  let flags = '01';

  if (rawTraceparent && TRACEPARENT_REGEX.test(rawTraceparent)) {
    const match = rawTraceparent.match(TRACEPARENT_REGEX);
    if (match) {
      traceId = match[1].toLowerCase();
      parentSpanId = match[2].toLowerCase();
      flags = match[3].toLowerCase();
    } else {
      traceId = randomBytes(16).toString('hex');
    }
  } else {
    traceId = randomBytes(16).toString('hex');
  }

  // Service must start its OWN span
  const spanId = randomBytes(8).toString('hex');
  const traceparent = `00-${traceId}-${spanId}-${flags}`;

  return {
    traceId,
    spanId,
    parentSpanId,
    parent_span_id: parentSpanId,
    traceparent,
    tracestate: rawTracestate,
  };
}

export function parseOrCreateRequestId(header?: string | string[]): string {
  const raw = Array.isArray(header) ? header[0] : header;
  if (raw && typeof raw === 'string') {
    const trimmed = raw.trim();
    if (trimmed.length > 0 && trimmed.length <= 64) {
      return trimmed;
    }
  }
  return randomUUID();
}

export function parseActor(
  headers: Record<string, string | string[] | undefined>,
): ActorContext {
  const userId =
    typeof headers['x-user-id'] === 'string'
      ? headers['x-user-id'].trim()
      : undefined;

  const rawRoles = headers['x-user-roles'];
  const rolesString = Array.isArray(rawRoles)
    ? rawRoles.join(',')
    : rawRoles || '';
  const roles = rolesString
    .split(',')
    .map((r) => r.trim())
    .filter((r) => r.length > 0);

  const rawPerms = headers['x-user-permissions'];
  const permsString = Array.isArray(rawPerms)
    ? rawPerms.join(',')
    : rawPerms || '';
  const permissions = permsString
    .split(',')
    .map((p) => p.trim())
    .filter((p) => p.length > 0);

  const shopScope =
    typeof headers['x-user-shop-scope'] === 'string'
      ? headers['x-user-shop-scope'].trim()
      : undefined;

  const rawMfa = headers['x-mfa-step-up'];
  const mfaString = Array.isArray(rawMfa) ? rawMfa[0] : rawMfa;
  const mfaStepUp = mfaString === 'true' || mfaString === '1';

  return {
    userId,
    roles,
    permissions,
    shopScope,
    mfaStepUp,
  };
}

export function buildKafkaHeaders(
  context?: Partial<RequestContextData>,
): Record<string, string> {
  const ctx = context || RequestContext.current();
  const headers: Record<string, string> = {};

  if (ctx?.traceparent) {
    headers['traceparent'] = ctx.traceparent;
  }
  if (ctx?.tracestate) {
    headers['tracestate'] = ctx.tracestate;
  }
  if (ctx?.requestId) {
    headers['x-request-id'] = ctx.requestId;
  }

  return headers;
}

export class RequestContext {
  static run<R>(data: RequestContextData, fn: () => R): R {
    return storage.run(data, fn);
  }

  static current(): RequestContextData | undefined {
    return storage.getStore();
  }

  static getRequestId(): string | undefined {
    return storage.getStore()?.requestId;
  }

  static getTraceId(): string | undefined {
    return storage.getStore()?.traceId;
  }

  static getSpanId(): string | undefined {
    return storage.getStore()?.spanId;
  }

  static getParentSpanId(): string | undefined {
    return (
      storage.getStore()?.parentSpanId || storage.getStore()?.parent_span_id
    );
  }

  static getActor(): ActorContext | undefined {
    return storage.getStore()?.actor;
  }
}
