import { Injectable, NestMiddleware } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';
import { MetricsService } from './metrics.service';
import { resolveRouteTemplate } from './route-template.helper';

@Injectable()
export class MetricsMiddleware implements NestMiddleware {
  constructor(private readonly metricsService: MetricsService) {}

  use(req: Request, res: Response, next: NextFunction): void {
    const startTime = process.hrtime();

    res.on('finish', () => {
      const diff = process.hrtime(startTime);
      const durationSeconds = diff[0] + diff[1] / 1e9;

      // Extract route template rather than raw URL or query
      const routeTemplate = resolveRouteTemplate(req);

      this.metricsService.recordHttpRequestDuration(
        routeTemplate,
        req.method,
        res.statusCode,
        durationSeconds,
      );
    });

    next();
  }
}
