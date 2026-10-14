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

const SKUS_UPDATED_EVENT = 'product.skus_updated';

@Injectable()
export class EventProcessor {
  private readonly logger = new Logger(EventProcessor.name);
  private readonly eventsProcessedTotal: Counter<string>;

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
    const reg = this.metricsService.getRegistry();
    this.eventsProcessedTotal =
      (reg.getSingleMetric('events_processed_total') as Counter<string>) ||
      new Counter({
        name: 'events_processed_total',
        help: 'Total processed domain events by topic and outcome',
        labelNames: ['topic', 'outcome'],
        registers: [reg],
      });
  }

  toIsoTimestamp(value: string | number): string {
    if (typeof value === 'number') return new Date(value).toISOString();
    const str = String(value).trim();
    const num = /^\d+$/.test(str) ? Number(str) : NaN;
    const d = !isNaN(num) ? new Date(num) : new Date(str);
    return isNaN(d.getTime()) ? new Date().toISOString() : d.toISOString();
  }

  async processMessage(
    topic: string,
    message: KafkaMessageInput,
    partition = 0,
  ): Promise<string | null> {
    const ctx = this.buildContext(message.headers);
    return RequestContext.run(ctx, async () => {
      const sendInvalid = async (logMsg: string) => {
        this.logger.warn(logMsg);
        this.eventsProcessedTotal.labels(topic, 'failed').inc();
        await this.dlqPublisher.publishInvalidMessage(
          topic,
          partition,
          message.offset,
          ctx,
        );
        return null;
      };

      let evt: DomainEvent<unknown>;
      try {
        const raw = Buffer.isBuffer(message.value)
          ? message.value.toString('utf-8')
          : String(message.value || '');
        if (!message.value || !raw.trim()) throw new Error('Empty message');
        evt = JSON.parse(raw) as DomainEvent<unknown>;
        if (!evt || typeof evt !== 'object') throw new Error('Invalid JSON');
      } catch (parseErr) {
        return sendInvalid(
          `Failed parsing JSON on ${topic}: ${parseErr instanceof Error ? parseErr.name : 'UnknownError'}`,
        );
      }

      if (!evt.event_id) {
        return sendInvalid(`Skipping event missing event_id on ${topic}`);
      }

      if (evt.occurred_at != null) {
        evt.occurred_at = this.toIsoTimestamp(evt.occurred_at);
      }
      if (evt.version != null) {
        evt.version = +evt.version || 0;
      }
      if (typeof evt.payload === 'string') {
        // Debezium's outbox router emits the payload as a JSON string unless
        // collection.expand.json.payload=true.
        try {
          evt.payload = JSON.parse(evt.payload) as unknown;
        } catch (payloadErr) {
          return sendInvalid(
            `Unparseable payload on ${topic}: ${payloadErr instanceof Error ? payloadErr.name : 'UnknownError'}`,
          );
        }
      }

      if ((await this.checkDeduplication(evt, topic)) !== 'DUPLICATE') {
        await this.executeWithRetry(topic, evt, ctx);
      }
      return (evt.occurred_at as string) || null;
    });
  }

  async checkDeduplication(
    evt: DomainEvent<unknown> | { event_id: string; [key: string]: unknown },
    topic?: string,
  ): Promise<'PROCESSED' | 'DUPLICATE'> {
    const index = getConsumerStateIndex(this.configService.esIndexPrefix);
    try {
      const existing = await this.esClient.get({ index, id: evt.event_id });
      if (existing?.found) {
        if (topic) this.eventsProcessedTotal.labels(topic, 'duplicate').inc();
        this.logger.debug(
          `Duplicate ${evt.event_id} on ${topic || 'topic'} skipped.`,
        );
        return 'DUPLICATE';
      }
    } catch (err: unknown) {
      if (!(err instanceof errors.ResponseError && err.statusCode === 404)) {
        this.logger.warn(
          `Dedupe check failed for ${evt.event_id}: ${err instanceof Error ? err.name : 'UnknownError'}`,
        );
      }
    }
    return 'PROCESSED';
  }

  async executeWithRetry(
    topic: string,
    evt: DomainEvent<unknown>,
    ctx: RequestContextData,
  ): Promise<void> {
    let lastError: unknown = null;
    let success = false;
    for (let attempt = 1; attempt <= EVENT_RETRY_COUNT; attempt++) {
      try {
        await this.dispatchToHandler(topic, evt);
        success = true;
        break;
      } catch (err) {
        lastError = err;
        this.logger.warn(
          `Attempt ${attempt}/${EVENT_RETRY_COUNT} failed for ${evt.event_id}: ${err instanceof Error ? err.name : 'UnknownError'}`,
        );
        if (attempt < EVENT_RETRY_COUNT) {
          const ms =
            process.env.NODE_ENV === 'test' ? 10 : EVENT_RETRY_BACKOFF_MS;
          await new Promise((r) => setTimeout(r, ms));
        }
      }
    }

    if (success) {
      this.eventsProcessedTotal.labels(topic, 'success').inc();
      await this.dlqPublisher.recordConsumerState(evt, 'PROCESSED');
    } else {
      this.eventsProcessedTotal.labels(topic, 'failed').inc();
      await this.dlqPublisher.publish(topic, evt, lastError, ctx);
    }
  }

  private async dispatchToHandler(topic: string, evt: DomainEvent<unknown>) {
    if (evt.event_type === SKUS_UPDATED_EVENT) {
      // Catalog ships the full SKU list of a product on the product topic.
      return this.skuHandler.handleSkusUpdated(evt);
    }
    const p = this.configService.kafkaTopicPrefix;
    const match = [
      [KAFKA_TOPICS.PRODUCT_EVENTS, this.productHandler],
      [KAFKA_TOPICS.SKU_EVENTS, this.skuHandler],
      [KAFKA_TOPICS.CATEGORY_EVENTS, this.categoryHandler],
      [KAFKA_TOPICS.CATALOG_EVENTS, this.catalogHandler],
      [KAFKA_TOPICS.RATING_EVENTS, this.ratingHandler],
    ].find(([t]) => getKafkaTopic(t as string, p) === topic);
    if (match) return (match[1] as ProductHandler).handle(evt);
    this.logger.warn(`Unknown topic received: ${topic}`);
  }

  private buildContext(headers?: Record<string, unknown>): RequestContextData {
    const raw: Record<string, string> = {};
    for (const [k, v] of Object.entries(headers || {})) {
      if (v != null) {
        raw[k.toLowerCase()] = Buffer.isBuffer(v)
          ? v.toString('utf-8')
          : String(Array.isArray(v) ? v[0] : v);
      }
    }
    return {
      ...parseOrCreateTrace(raw['traceparent'], raw['tracestate']),
      requestId: parseOrCreateRequestId(raw['x-request-id']),
      actor: { roles: [], permissions: [], mfaStepUp: false },
    };
  }
}
