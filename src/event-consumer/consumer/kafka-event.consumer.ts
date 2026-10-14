import { Client, errors } from '@elastic/elasticsearch';
import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  Optional,
} from '@nestjs/common';
import { Consumer, Kafka, Producer } from 'kafkajs';
import { Counter, Gauge } from 'prom-client';
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
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
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
import { DomainEvent } from '../interfaces';
import { KAFKA_CLIENT } from '../kafka.module';
import { ConsumerStatusService } from '../status/consumer-status.service';

@Injectable()
export class KafkaEventConsumer
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(KafkaEventConsumer.name);
  private consumer?: Consumer;
  private producer?: Producer;
  private isRunning = false;

  private eventsProcessedTotal: Counter<string>;
  private eventLagSeconds: Gauge<string>;

  constructor(
    private readonly configService: AppConfigService,
    @Inject(ELASTICSEARCH_CLIENT) private readonly esClient: Client,
    private readonly metricsService: MetricsService,
    private readonly productHandler: ProductHandler,
    private readonly skuHandler: SkuHandler,
    private readonly categoryHandler: CategoryHandler,
    private readonly catalogHandler: CatalogHandler,
    private readonly ratingHandler: RatingHandler,
    private readonly consumerStatusService: ConsumerStatusService,
    @Optional() @Inject(KAFKA_CLIENT) private readonly kafka?: Kafka,
  ) {
    const registry = this.metricsService.getRegistry();

    this.eventsProcessedTotal = new Counter({
      name: 'events_processed_total',
      help: 'Total processed domain events by topic and outcome',
      labelNames: ['topic', 'outcome'],
      registers: [registry],
    });

    this.eventLagSeconds = new Gauge({
      name: 'event_lag_seconds',
      help: 'Event lag in seconds between occurred_at and processing time',
      labelNames: ['topic'],
      registers: [registry],
    });
  }

  async onApplicationBootstrap(): Promise<void> {
    if (!this.configService.kafkaConsumerEnabled) {
      this.logger.log('Kafka consumer is disabled via config.');
      this.consumerStatusService.setConsumerState(false);
      return;
    }

    if (!this.kafka) {
      this.logger.warn(
        'Kafka client is not configured; skipping consumer start.',
      );
      this.consumerStatusService.setConsumerState(false);
      return;
    }

    await this.start();
  }

  async onApplicationShutdown(): Promise<void> {
    this.isRunning = false;
    this.consumerStatusService.setConsumerState(false);
    if (this.consumer) {
      try {
        await this.consumer.disconnect();
      } catch (err) {
        this.logger.error(`Error disconnecting Kafka consumer: ${err}`);
      }
    }
    if (this.producer) {
      try {
        await this.producer.disconnect();
      } catch (err) {
        this.logger.error(`Error disconnecting Kafka producer: ${err}`);
      }
    }
  }

  async start(): Promise<void> {
    if (!this.kafka) {
      return;
    }

    const prefix = this.configService.kafkaTopicPrefix;
    const topics = [
      getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.SKU_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.CATEGORY_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.CATALOG_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.RATING_EVENTS, prefix),
    ];

    this.consumer = this.kafka.consumer({
      groupId: this.configService.kafkaGroupId,
    });

    this.producer = this.kafka.producer();

    await this.consumer.connect();
    await this.producer.connect();

    for (const topic of topics) {
      await this.consumer.subscribe({ topic, fromBeginning: false });
    }

    this.isRunning = true;
    this.consumerStatusService.setConsumerState(true);

    await this.consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
        isRunning,
        isStale,
      }) => {
        let lastOccurredAt: string | null = null;
        let lastLagSeconds = 0;

        for (const message of batch.messages) {
          if (!isRunning() || isStale()) {
            break;
          }

          const occurredAt = await this.processMessage(batch.topic, message);
          if (occurredAt) {
            lastOccurredAt = occurredAt;
            const parsedTime = new Date(occurredAt).getTime();
            if (!isNaN(parsedTime)) {
              lastLagSeconds = Math.max(
                0,
                Math.floor((Date.now() - parsedTime) / 1000),
              );
            }
          }

          resolveOffset(message.offset);
          await heartbeat();
        }

        await commitOffsetsIfNecessary();

        if (lastOccurredAt) {
          this.eventLagSeconds.labels(batch.topic).set(lastLagSeconds);
          this.consumerStatusService.recordEventProcessed(
            batch.topic,
            lastOccurredAt,
            lastLagSeconds,
          );
        }
      },
    });
  }

  /**
   * Processes an individual Kafka message:
   * 1. Extracts traceparent / request_id headers and enters RequestContext
   * 2. Deserializes JSON domain event
   * 3. Performs deduplication check via search-consumer-state
   * 4. Retries 3x with 2s backoff on handler dispatch
   * 5. If retry exhausted, produces redacted envelope to DLQ topic
   */
  async processMessage(
    topic: string,
    message: {
      headers?: Record<
        string,
        Buffer | string | undefined | (Buffer | string)[]
      >;
      value: Buffer | string | null;
      offset: string;
    },
  ): Promise<string | null> {
    const rawHeaders: Record<string, string> = {};
    if (message.headers) {
      for (const [k, v] of Object.entries(message.headers)) {
        if (v) {
          if (Array.isArray(v)) {
            const first = v[0];
            rawHeaders[k.toLowerCase()] = Buffer.isBuffer(first)
              ? first.toString('utf-8')
              : String(first);
          } else {
            rawHeaders[k.toLowerCase()] = Buffer.isBuffer(v)
              ? v.toString('utf-8')
              : String(v);
          }
        }
      }
    }

    const traceInfo = parseOrCreateTrace(
      rawHeaders['traceparent'],
      rawHeaders['tracestate'],
    );
    const requestId = parseOrCreateRequestId(rawHeaders['x-request-id']);

    const ctx: RequestContextData = {
      requestId,
      traceId: traceInfo.traceId,
      spanId: traceInfo.spanId,
      parentSpanId: traceInfo.parentSpanId,
      traceparent: traceInfo.traceparent,
      tracestate: traceInfo.tracestate,
      actor: { roles: [], permissions: [], mfaStepUp: false },
    };

    return RequestContext.run(ctx, async () => {
      let evt: DomainEvent<unknown>;
      try {
        const rawValue = Buffer.isBuffer(message.value)
          ? message.value.toString('utf-8')
          : String(message.value || '{}');
        evt = JSON.parse(rawValue) as DomainEvent<unknown>;
      } catch (parseErr) {
        this.logger.error(
          `Failed to parse event JSON on topic ${topic}: ${parseErr}`,
        );
        this.eventsProcessedTotal.labels(topic, 'failed').inc();
        return null;
      }

      if (!evt || !evt.event_id) {
        this.logger.warn(
          `Skipping event with missing event_id on topic ${topic}`,
        );
        this.eventsProcessedTotal.labels(topic, 'skipped').inc();
        return null;
      }

      // Dedupe check: op_type create on search-consumer-state
      const consumerStateIndex = getConsumerStateIndex(
        this.configService.esIndexPrefix,
      );

      try {
        await this.esClient.create({
          index: consumerStateIndex,
          id: evt.event_id,
          document: {
            event_id: evt.event_id,
            aggregate_id: evt.aggregate_id || '',
            source_version: evt.version ?? 0,
            processed_at: new Date().toISOString(),
            status: 'PROCESSED',
            error_code: null,
          },
        });
      } catch (err: unknown) {
        if (
          (err instanceof errors.ResponseError ||
            (err as { name?: string })?.name === 'ResponseError') &&
          ((err as { statusCode?: number })?.statusCode === 409 ||
            (err as { meta?: { statusCode?: number } })?.meta?.statusCode ===
              409)
        ) {
          // Already recorded => duplicate
          this.eventsProcessedTotal.labels(topic, 'duplicate').inc();
          this.logger.debug(
            `Duplicate event ${evt.event_id} on topic ${topic} skipped.`,
          );
          return evt.occurred_at || null;
        }
        this.logger.warn(
          `Failed writing consumer-state dedupe record for ${evt.event_id}: ${err}`,
        );
      }

      // Retry 3 times with 2s backoff
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
            `Attempt ${attempt}/${EVENT_RETRY_COUNT} failed for event ${evt.event_id} on ${topic}: ${err}`,
          );
          if (attempt < EVENT_RETRY_COUNT) {
            await this.delay(EVENT_RETRY_BACKOFF_MS);
          }
        }
      }

      if (success) {
        this.eventsProcessedTotal.labels(topic, 'success').inc();
      } else {
        this.eventsProcessedTotal.labels(topic, 'failed').inc();
        await this.handleDeadLetter(topic, evt, lastError, ctx);
      }

      return evt.occurred_at || null;
    });
  }

  private async dispatchToHandler(
    topic: string,
    evt: DomainEvent<unknown>,
  ): Promise<void> {
    const prefix = this.configService.kafkaTopicPrefix;

    if (topic === getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, prefix)) {
      await this.productHandler.handle(evt);
    } else if (topic === getKafkaTopic(KAFKA_TOPICS.SKU_EVENTS, prefix)) {
      await this.skuHandler.handle(evt);
    } else if (topic === getKafkaTopic(KAFKA_TOPICS.CATEGORY_EVENTS, prefix)) {
      await this.categoryHandler.handle(evt);
    } else if (topic === getKafkaTopic(KAFKA_TOPICS.CATALOG_EVENTS, prefix)) {
      await this.catalogHandler.handle(evt);
    } else if (topic === getKafkaTopic(KAFKA_TOPICS.RATING_EVENTS, prefix)) {
      await this.ratingHandler.handle(evt);
    } else {
      this.logger.warn(`Unknown topic received: ${topic}`);
    }
  }

  private async handleDeadLetter(
    topic: string,
    evt: DomainEvent<unknown>,
    error: unknown,
    ctx: RequestContextData,
  ): Promise<void> {
    const errorCode =
      error instanceof SearchException
        ? error.code
        : typeof error === 'object' &&
            error !== null &&
            'code' in error &&
            typeof (error as Record<string, unknown>).code === 'string'
          ? ((error as Record<string, unknown>).code as string)
          : SearchErrorCode.SEARCH_INTERNAL_ERROR;

    const consumerStateIndex = getConsumerStateIndex(
      this.configService.esIndexPrefix,
    );

    // Update state to FAILED
    try {
      await this.esClient.index({
        index: consumerStateIndex,
        id: evt.event_id,
        document: {
          event_id: evt.event_id,
          aggregate_id: evt.aggregate_id || '',
          source_version: evt.version ?? 0,
          processed_at: new Date().toISOString(),
          status: 'FAILED',
          error_code: String(errorCode),
        },
      });
    } catch (err) {
      this.logger.error(
        `Failed to record FAILED state for ${evt.event_id}: ${err}`,
      );
    }

    // Produce redacted envelope to DLQ topic
    if (this.producer) {
      const dlqTopic = getKafkaTopic(
        KAFKA_TOPICS.DLQ,
        this.configService.kafkaTopicPrefix,
      );

      const redactedEnvelope = {
        event_id: evt.event_id,
        event_type: evt.event_type,
        aggregate_id: evt.aggregate_id,
        version: evt.version,
        error_code: String(errorCode),
      };

      try {
        await this.producer.send({
          topic: dlqTopic,
          messages: [
            {
              key: evt.aggregate_id || evt.event_id,
              value: JSON.stringify(redactedEnvelope),
              headers: {
                traceparent: ctx.traceparent,
                'x-request-id': ctx.requestId,
              },
            },
          ],
        });
        this.logger.log(
          `Produced redacted event ${evt.event_id} to DLQ topic ${dlqTopic}`,
        );
      } catch (err) {
        this.logger.error(`Failed producing to DLQ topic ${dlqTopic}: ${err}`);
      }
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
