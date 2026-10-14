import { Injectable, OnModuleInit } from '@nestjs/common';
import { collectDefaultMetrics, Histogram, Registry } from 'prom-client';

@Injectable()
export class MetricsService implements OnModuleInit {
  private readonly registry: Registry;
  private readonly httpRequestDuration: Histogram<string>;

  constructor() {
    this.registry = new Registry();
    this.httpRequestDuration = new Histogram({
      name: 'http_request_duration_seconds',
      help: 'Duration of HTTP requests in seconds',
      labelNames: ['route', 'method', 'status'],
      registers: [this.registry],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
    });
  }

  onModuleInit() {
    collectDefaultMetrics({ register: this.registry });
  }

  recordHttpRequestDuration(
    route: string,
    method: string,
    status: string | number,
    durationSeconds: number,
  ): void {
    this.httpRequestDuration
      .labels(route, method, String(status))
      .observe(durationSeconds);
  }

  getContentType(): string {
    return this.registry.contentType;
  }

  async getMetrics(): Promise<string> {
    return this.registry.metrics();
  }

  getRegistry(): Registry {
    return this.registry;
  }
}
