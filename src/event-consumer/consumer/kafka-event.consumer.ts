import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  Optional,
} from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import { Gauge } from 'prom-client';
import { getKafkaTopic, KAFKA_TOPICS } from '../../common/constants';
import { AppConfigService } from '../../config/config.service';
import { MetricsService } from '../../observability/metrics.service';
import {
  CatalogHandler,
  CategoryHandler,
  ProductHandler,
  RatingHandler,
  SkuHandler,
} from '../handlers';
import { DomainEvent, KafkaMessageInput } from '../interfaces';
import { KAFKA_CLIENT } from '../kafka.module';
import { ConsumerStatusService } from '../status/consumer-status.service';
import { DlqPublisher } from './dlq-publisher';
import { EventProcessor } from './event-processor';

@Injectable()
export class KafkaEventConsumer
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(KafkaEventConsumer.name);
  private consumer?: Consumer;
  private isRunning = false;
  private eventLagSeconds?: Gauge<string>;

  constructor(
    private readonly configService: AppConfigService,
    @Inject(KAFKA_CLIENT) private readonly kafka: Kafka,
    private readonly metricsService: MetricsService,
    private readonly productHandler: ProductHandler,
    private readonly skuHandler: SkuHandler,
    private readonly categoryHandler: CategoryHandler,
    private readonly catalogHandler: CatalogHandler,
    private readonly ratingHandler: RatingHandler,
    private readonly consumerStatusService: ConsumerStatusService,
    @Optional() private readonly eventProcessor?: EventProcessor,
    @Optional() private readonly dlqPublisher?: DlqPublisher,
  ) {
    if (this.metricsService) {
      const registry = this.metricsService.getRegistry();
      const existing = registry.getSingleMetric(
        'event_lag_seconds',
      ) as Gauge<string>;
      this.eventLagSeconds =
        existing ||
        new Gauge({
          name: 'event_lag_seconds',
          help: 'Event lag in seconds between occurred_at and processing time',
          labelNames: ['topic'],
          registers: [registry],
        });
    }
  }

  async onApplicationBootstrap(): Promise<void> {
    if (!this.configService?.kafkaConsumerEnabled) {
      this.logger.log('Kafka consumer is disabled via config.');
      this.consumerStatusService?.setConsumerState(false);
      return;
    }

    if (!this.kafka) {
      this.logger.warn(
        'Kafka client is not configured; skipping consumer start.',
      );
      this.consumerStatusService?.setConsumerState(false);
      return;
    }

    await this.start();
  }

  async onApplicationShutdown(): Promise<void> {
    this.isRunning = false;
    this.consumerStatusService?.setConsumerState(false);
    if (this.consumer) {
      try {
        await this.consumer.disconnect();
      } catch (err: unknown) {
        this.logger.error(`Error disconnecting Kafka consumer: ${err}`);
      }
    }
    if (this.dlqPublisher) {
      try {
        await this.dlqPublisher.disconnect();
      } catch (err: unknown) {
        this.logger.error(`Error disconnecting DLQ publisher: ${err}`);
      }
    }
  }

  async start(): Promise<void> {
    if (!this.kafka) {
      return;
    }

    const prefix = this.configService?.kafkaTopicPrefix || '';
    const topics = [
      getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.SKU_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.CATEGORY_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.CATALOG_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.RATING_EVENTS, prefix),
    ];

    this.consumer = this.kafka.consumer({
      groupId: this.configService?.kafkaGroupId || 'search-service-group',
    });

    if (this.dlqPublisher) {
      await this.dlqPublisher.connect();
    }
    await this.consumer.connect();

    await this.consumer.subscribe({
      topics,
      fromBeginning: false,
    });

    this.isRunning = true;
    this.consumerStatusService?.setConsumerState(true);

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

          // If processMessage fails (e.g. DLQ send failure), error propagates,
          // resolveOffset is NOT called, and kafkajs re-delivers from last resolved offset.
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
          this.eventLagSeconds?.labels(batch.topic).set(lastLagSeconds);
          this.consumerStatusService?.recordEventProcessed(
            batch.topic,
            lastOccurredAt,
            lastLagSeconds,
          );
        }
      },
    });
  }

  async processMessage(
    topic: string,
    message: KafkaMessageInput,
  ): Promise<string | null> {
    if (!this.eventProcessor) {
      throw new Error('EventProcessor is not configured');
    }
    return this.eventProcessor.processMessage(topic, message);
  }

  async processEvent(
    evt: DomainEvent<unknown> | { event_id: string; [key: string]: unknown },
    headers?: Record<string, unknown>,
  ): Promise<{ status: string; reason?: string }> {
    if (!this.eventProcessor) {
      throw new Error('EventProcessor is not configured');
    }
    return this.eventProcessor.processEvent(evt, headers);
  }

  async processWithRetry(
    evt: DomainEvent<unknown> | { event_id: string; [key: string]: unknown },
    headers?: Record<string, unknown>,
    handler?: (evt: DomainEvent<unknown>) => Promise<{ status: string } | void>,
  ): Promise<{ status: string }> {
    if (!this.eventProcessor) {
      throw new Error('EventProcessor is not configured');
    }
    return this.eventProcessor.processWithRetry(evt, headers, handler);
  }
}
