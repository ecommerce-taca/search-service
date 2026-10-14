import { Client, errors } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { Counter } from 'prom-client';
import {
  EVENT_RETRY_BACKOFF_MS,
  EVENT_RETRY_COUNT,
  getConsumerStateIndex,
  getKafkaTopic,
  KAFKA_TOPICS,
} from '../../common/constants';
import {
  parseOrCreateRequestId,
  parseOrCreateTrace,
  RequestContext,
  RequestContextData,
} from '../../common/context/request-context';
import { AppConfigService } from '../../config/config.service';
import { MetricsService } from '../../observability/metrics.service';
import { ELASTICSEARCH_CLIENT } from '../../product-index/elasticsearch.module';
import {
  CatalogHandler,
  CategoryHandler,
  ProductHandler,
  RatingHandler,
  SkuHandler,
} from '../handlers';
import { DomainEvent, KafkaMessageInput } from '../interfaces';
import { DlqPublisher } from './dlq-publisher';

@Injectable()
export class EventProcessor {
  private readonly logger = new Logger(EventProcessor.name);
  private eventsProcessedTotal?: Counter<string>;

  constructor(
    private readonly configService: AppConfigService,
    @Inject(ELASTICSEARCH_CLIENT) private readonly esClient: Client,
    private readonly metricsService: MetricsService,
    private readonly dlqPublisher: DlqPublisher,
    private readonly productHandler: ProductHandler,
    private readonly skuHandler: SkuHandler,
    private readonly categoryHandler: CategoryHandler,
    private readonly catalogHandler: CatalogHandler,
    private readonly ratingHandler: RatingHandler,
  ) {
    const reg = this.metricsService?.getRegistry();
    if (reg) {
      this.eventsProcessedTotal =
        (reg.getSingleMetric('events_processed_total') as Counter<string>) ||
        new Counter({
          name: 'events_processed_total',
          help: 'Total processed domain events by topic and outcome',
          labelNames: ['topic', 'outcome'],
          registers: [reg],
        });
    }
  }

  async processMessage(
    topic: string,
    message: KafkaMessageInput,
  ): Promise<string | null> {
    const ctx = this.buildContext(this.extractHeaders(message.headers));
    return RequestContext.run(ctx, async () => {
      let evt: DomainEvent<unknown>;
      try {
        const val = message.value;
        const raw = Buffer.isBuffer(val)
          ? val.toString('utf-8')
          : String(val || '{}');
        evt = JSON.parse(raw) as DomainEvent<unknown>;
      } catch (parseErr) {
        this.logger.error(`Failed parsing event JSON on ${topic}: ${parseErr}`);
        this.eventsProcessedTotal?.labels(topic, 'failed').inc();
        return null;
      }

      if (!evt?.event_id) {
        this.logger.warn(`Skipping event missing event_id on ${topic}`);
        this.eventsProcessedTotal?.labels(topic, 'skipped').inc();
        return null;
      }

      if ((await this.checkDeduplication(evt, topic)) !== 'DUPLICATE') {
        await this.executeWithRetry(topic, evt, ctx);
      }
      return evt.occurred_at || null;
    });
  }

  async checkDeduplication(
    evt: DomainEvent<unknown> | { event_id: string; [key: string]: unknown },
    topic?: string,
  ): Promise<'PROCESSED' | 'DUPLICATE'> {
    const index = getConsumerStateIndex(
      this.configService?.esIndexPrefix || '',
    );
    try {
      await this.esClient.create({
        index,
        id: evt.event_id,
        document: {
          event_id: evt.event_id,
          aggregate_id: (evt as DomainEvent<unknown>).aggregate_id || '',
          source_version: (evt as DomainEvent<unknown>).version ?? 0,
          processed_at: new Date().toISOString(),
          status: 'PROCESSED',
          error_code: null,
        },
      });
      return 'PROCESSED';
    } catch (err: unknown) {
      const e = err as {
        statusCode?: number;
        meta?: { statusCode?: number };
        message?: string;
      };
      const is409 =
        (err instanceof errors.ResponseError && err.statusCode === 409) ||
        e?.statusCode === 409 ||
        e?.meta?.statusCode === 409 ||
        String(e?.message || '')
          .toLowerCase()
          .includes('conflict');
      if (is409) {
        if (topic) this.eventsProcessedTotal?.labels(topic, 'duplicate').inc();
        this.logger.debug(
          `Duplicate ${evt.event_id} on ${topic || 'topic'} skipped.`,
        );
        return 'DUPLICATE';
      }
      this.logger.warn(`Dedupe write failed for ${evt.event_id}: ${err}`);
      return 'PROCESSED';
    }
  }

  async executeWithRetry(
    topic: string,
    evt: DomainEvent<unknown>,
    ctx: RequestContextData,
    handler?: (e: DomainEvent<unknown>) => Promise<{ status: string } | void>,
  ): Promise<void> {
    let lastError: unknown = null;
    let success = false;
    for (let attempt = 1; attempt <= EVENT_RETRY_COUNT; attempt++) {
      try {
        if (handler) await handler(evt);
        else await this.dispatchToHandler(topic, evt);
        success = true;
        break;
      } catch (err) {
        lastError = err;
        this.logger.warn(
          `Attempt ${attempt}/${EVENT_RETRY_COUNT} failed for ${evt.event_id}: ${err}`,
        );
        if (attempt < EVENT_RETRY_COUNT) {
          const ms =
            process.env.NODE_ENV === 'test' ? 10 : EVENT_RETRY_BACKOFF_MS;
          await new Promise((r) => setTimeout(r, ms));
        }
      }
    }

    if (success) {
      this.eventsProcessedTotal?.labels(topic, 'success').inc();
    } else {
      this.eventsProcessedTotal?.labels(topic, 'failed').inc();
      await this.dlqPublisher.publish(topic, evt, lastError, ctx);
    }
  }

  async processEvent(
    evt: DomainEvent<unknown> | { event_id: string; [key: string]: unknown },
    headers?: Record<string, unknown>,
  ): Promise<{ status: string; reason?: string }> {
    void headers;
    if (!evt?.event_id) {
      return { status: 'SKIPPED', reason: 'MISSING_EVENT_ID' };
    }
    return (await this.checkDeduplication(evt)) === 'DUPLICATE'
      ? { status: 'SKIPPED', reason: 'DUPLICATE' }
      : { status: 'PROCESSED' };
  }

  async processWithRetry(
    evt: DomainEvent<unknown> | { event_id: string; [key: string]: unknown },
    headers?: Record<string, unknown>,
    handler?: (evt: DomainEvent<unknown>) => Promise<{ status: string } | void>,
  ): Promise<{ status: string }> {
    const ctx = this.buildContext(this.extractHeaders(headers));
    return RequestContext.run(ctx, async () => {
      const topic = getKafkaTopic(
        KAFKA_TOPICS.PRODUCT_EVENTS,
        this.configService?.kafkaTopicPrefix || '',
      );
      try {
        await this.executeWithRetry(
          topic,
          evt as DomainEvent<unknown>,
          ctx,
          handler,
        );
        return { status: 'PROCESSED' };
      } catch {
        return { status: 'FAILED' };
      }
    });
  }

  private async dispatchToHandler(
    topic: string,
    evt: DomainEvent<unknown>,
  ): Promise<void> {
    const p = this.configService?.kafkaTopicPrefix || '';
    const h = {
      [getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, p)]: this.productHandler,
      [getKafkaTopic(KAFKA_TOPICS.SKU_EVENTS, p)]: this.skuHandler,
      [getKafkaTopic(KAFKA_TOPICS.CATEGORY_EVENTS, p)]: this.categoryHandler,
      [getKafkaTopic(KAFKA_TOPICS.CATALOG_EVENTS, p)]: this.catalogHandler,
      [getKafkaTopic(KAFKA_TOPICS.RATING_EVENTS, p)]: this.ratingHandler,
    }[topic];
    if (h) return h.handle(evt);
    this.logger.warn(`Unknown topic received: ${topic}`);
  }

  private extractHeaders(
    headers?: Record<string, unknown>,
  ): Record<string, string> {
    const res: Record<string, string> = {};
    for (const [k, v] of Object.entries(headers || {})) {
      if (v != null) {
        const val = Array.isArray(v) ? v[0] : v;
        res[k.toLowerCase()] = Buffer.isBuffer(val)
          ? val.toString('utf-8')
          : String(val);
      }
    }
    return res;
  }

  private buildContext(raw: Record<string, string>): RequestContextData {
    return {
      ...parseOrCreateTrace(raw['traceparent'], raw['tracestate']),
      requestId: parseOrCreateRequestId(raw['x-request-id']),
      actor: { roles: [], permissions: [], mfaStepUp: false },
    };
  }
}
