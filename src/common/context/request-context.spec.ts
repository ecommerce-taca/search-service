import {
  buildKafkaHeaders,
  parseActor,
  parseOrCreateRequestId,
  parseOrCreateTrace,
  RequestContext,
} from './request-context';

describe('RequestContext', () => {
  describe('parseOrCreateRequestId', () => {
    it('should keep request id if <= 64 chars', () => {
      const id = 'my-custom-request-id-12345';
      expect(parseOrCreateRequestId(id)).toBe(id);
    });

    it('should generate uuid if header is missing', () => {
      const generated = parseOrCreateRequestId(undefined);
      expect(generated).toBeDefined();
      expect(generated.length).toBeGreaterThan(10);
    });

    it('should generate uuid if header exceeds 64 characters', () => {
      const longId = 'a'.repeat(65);
      const generated = parseOrCreateRequestId(longId);
      expect(generated).not.toBe(longId);
      expect(generated.length).toBeLessThanOrEqual(64);
    });
  });

  describe('parseOrCreateTrace', () => {
    it('should parse valid W3C traceparent and keep tracestate', () => {
      const validTraceparent =
        '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
      const tracestate = 'rojo=1,congo=2';

      const result = parseOrCreateTrace(validTraceparent, tracestate);
      expect(result.traceId).toBe('4bf92f3577b34da6a3ce929d0e0e4736');
      expect(result.spanId).toBe('00f067aa0ba902b7');
      expect(result.traceparent).toBe(validTraceparent);
      expect(result.tracestate).toBe(tracestate);
    });

    it('should generate valid 32-hex trace_id and 16-hex span_id if invalid or absent', () => {
      const result = parseOrCreateTrace(undefined, undefined);
      expect(result.traceId).toMatch(/^[0-9a-f]{32}$/);
      expect(result.spanId).toMatch(/^[0-9a-f]{16}$/);
      expect(result.traceparent).toBe(
        `00-${result.traceId}-${result.spanId}-01`,
      );
    });
  });

  describe('parseActor', () => {
    it('should parse actor headers with roles, permissions, shop scope, and mfa', () => {
      const headers = {
        'x-user-id': 'usr-100',
        'x-user-roles': 'SEARCH_ADMIN, SELLER ',
        'x-user-permissions': 'read:products, write:products',
        'x-user-shop-scope': 'shop-99',
        'x-mfa-step-up': 'true',
      };

      const actor = parseActor(headers);
      expect(actor.userId).toBe('usr-100');
      expect(actor.roles).toEqual(['SEARCH_ADMIN', 'SELLER']);
      expect(actor.permissions).toEqual(['read:products', 'write:products']);
      expect(actor.shopScope).toBe('shop-99');
      expect(actor.mfaStepUp).toBe(true);
    });
  });

  describe('buildKafkaHeaders', () => {
    it('should format Kafka headers with traceparent, tracestate, and x-request-id', () => {
      const ctx = {
        requestId: 'req-999',
        traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
        tracestate: 'vendor=value',
      };

      const headers = buildKafkaHeaders(ctx);
      expect(headers['traceparent']).toBe(ctx.traceparent);
      expect(headers['tracestate']).toBe(ctx.tracestate);
      expect(headers['x-request-id']).toBe(ctx.requestId);
    });

    it('should read from active RequestContext when no context passed', () => {
      RequestContext.run(
        {
          requestId: 'ctx-req',
          traceId: 'tid',
          spanId: 'sid',
          traceparent: '00-tid-sid-01',
          actor: { roles: [], permissions: [], mfaStepUp: false },
        },
        () => {
          const headers = buildKafkaHeaders();
          expect(headers['x-request-id']).toBe('ctx-req');
          expect(headers['traceparent']).toBe('00-tid-sid-01');
        },
      );
    });
  });
});
