import { Request } from 'express';
import { resolveRouteTemplate } from './route-template.helper';

describe('resolveRouteTemplate', () => {
  it('should return combined baseUrl and route.path when route is matched', () => {
    const req = {
      baseUrl: '/api/v1',
      route: { path: '/search' },
      path: '/api/v1/search',
    } as unknown as Request;
    expect(resolveRouteTemplate(req)).toBe('/api/v1/search');
  });

  it('should return route.path when baseUrl is not set', () => {
    const req = {
      route: { path: '/health/live' },
      path: '/health/live',
    } as unknown as Request;
    expect(resolveRouteTemplate(req)).toBe('/health/live');
  });

  it('should return "unmatched" and never leak req.path or req.url when req.route is undefined', () => {
    const req = {
      baseUrl: '/api/v1',
      path: '/api/v1/products/sensitive-sku-id-12345',
      url: '/api/v1/products/sensitive-sku-id-12345?query=secret',
    } as unknown as Request;
    expect(resolveRouteTemplate(req)).toBe('unmatched');
  });

  it('should return "unmatched" when req is null or undefined', () => {
    expect(resolveRouteTemplate(null)).toBe('unmatched');
    expect(resolveRouteTemplate(undefined)).toBe('unmatched');
  });

  it('should return "unmatched" when req.route has no path', () => {
    const req = {
      route: {},
      path: '/unknown',
    } as unknown as Request;
    expect(resolveRouteTemplate(req)).toBe('unmatched');
  });
});
