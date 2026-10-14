import { Request } from 'express';

export function resolveRouteTemplate(
  req?:
    Partial<Request> | { baseUrl?: string; route?: { path?: string } } | null,
): string {
  if (req?.route?.path) {
    return `${req.baseUrl || ''}${req.route.path}`;
  }
  return 'unmatched';
}
