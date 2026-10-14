import { Global, MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { Request } from 'express';
import { LoggerModule } from 'nestjs-pino';
import { RequestContext } from '../common/context/request-context';
import { AppConfigModule } from '../config/config.module';
import { AppConfigService } from '../config/config.service';
import { MetricsController } from './metrics.controller';
import { MetricsMiddleware } from './metrics.middleware';
import { MetricsService } from './metrics.service';

@Global()
@Module({
  imports: [
    LoggerModule.forRootAsync({
      imports: [AppConfigModule],
      inject: [AppConfigService],
      useFactory: (config: AppConfigService) => ({
        pinoHttp: {
          level: config.appEnv === 'test' ? 'silent' : 'info',
          timestamp: () => `,"timestamp":"${new Date().toISOString()}"`,
          formatters: {
            level(label: string) {
              return { level: label };
            },
            bindings() {
              return {};
            },
          },
          messageKey: 'event',
          customProps: () => {
            const ctx = RequestContext.current();
            return {
              service: config.serviceName,
              env: config.appEnv,
              version: config.appVersion,
              trace_id: ctx?.traceId || 'unknown',
              span_id: ctx?.spanId || 'unknown',
              request_id: ctx?.requestId || 'unknown',
            };
          },
          customSuccessObject: (req, res, val: { responseTime: number }) => {
            const expressReq = req as unknown as Request;
            const routeTemplate = expressReq.route?.path
              ? `${expressReq.baseUrl || ''}${expressReq.route.path}`
              : expressReq.baseUrl || expressReq.path || 'unknown';
            const ctx = RequestContext.current();
            return {
              service: config.serviceName,
              env: config.appEnv,
              version: config.appVersion,
              event: 'http_request',
              trace_id: ctx?.traceId || 'unknown',
              span_id: ctx?.spanId || 'unknown',
              request_id: ctx?.requestId || 'unknown',
              route: routeTemplate,
              method: req.method,
              status_code: res.statusCode,
              duration_ms: Math.round(val.responseTime),
            };
          },
          customErrorObject: (
            req,
            res,
            _error,
            val: { responseTime: number },
          ) => {
            const expressReq = req as unknown as Request;
            const routeTemplate = expressReq.route?.path
              ? `${expressReq.baseUrl || ''}${expressReq.route.path}`
              : expressReq.baseUrl || expressReq.path || 'unknown';
            const ctx = RequestContext.current();
            return {
              service: config.serviceName,
              env: config.appEnv,
              version: config.appVersion,
              event: 'http_error',
              trace_id: ctx?.traceId || 'unknown',
              span_id: ctx?.spanId || 'unknown',
              request_id: ctx?.requestId || 'unknown',
              route: routeTemplate,
              method: req.method,
              status_code: res.statusCode,
              duration_ms: Math.round(val.responseTime),
            };
          },
          customSuccessMessage: () => 'http_request',
          customErrorMessage: () => 'http_error',
          serializers: {
            req: () => undefined,
            res: () => undefined,
            err: () => undefined,
          },
          redact: {
            paths: [
              'req.headers.authorization',
              'req.headers.cookie',
              'headers.authorization',
              'headers.cookie',
              'authorization',
              'cookie',
            ],
            remove: true,
          },
        },
      }),
    }),
  ],
  controllers: [MetricsController],
  providers: [MetricsService],
  exports: [MetricsService, LoggerModule],
})
export class ObservabilityModule implements NestModule {
  configure(consumer: MiddlewareConsumer) {
    consumer.apply(MetricsMiddleware).forRoutes('*');
  }
}
