import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import { Gauge } from 'prom-client';
import { getKafkaTopic, KAFKA_TOPICS } from '../../common/constants';
import { AppConfigService } from '../../config/config.service';
import { MetricsService } from '../../observability/metrics.service';
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
  private readonly eventLagSeconds: Gauge<string>;

  constructor(
    private readonly configService: AppConfigService,
    @Inject(KAFKA_CLIENT) private readonly kafka: Kafka,
    private readonly metricsService: MetricsService,
    private readonly consumerStatusService: ConsumerStatusService,
    private readonly eventProcessor: EventProcessor,
    private readonly dlqPublisher: DlqPublisher,
  ) {
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
      } catch (err: unknown) {
        this.logger.error(
          `Error disconnecting Kafka consumer: ${err instanceof Error ? err.name : 'UnknownError'}`,
        );
      }
    }
    try {
      await this.dlqPublisher.disconnect();
    } catch (err: unknown) {
      this.logger.error(
        `Error disconnecting DLQ publisher: ${err instanceof Error ? err.name : 'UnknownError'}`,
      );
    }
  }

  async start(): Promise<void> {
    if (!this.kafka) {
      return;
    }

    const prefix = this.configService.kafkaTopicPrefix || '';
    const topics = [
      getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.SKU_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.CATEGORY_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.CATALOG_EVENTS, prefix),
      getKafkaTopic(KAFKA_TOPICS.RATING_EVENTS, prefix),
    ];

    this.consumer = this.kafka.consumer({
      groupId: this.configService.kafkaGroupId || 'search-service-group',
    });

    await this.dlqPublisher.connect();
    await this.consumer.connect();

    await this.consumer.subscribe({
      topics,
      fromBeginning: false,
    });

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

          // If processMessage fails (e.g. DLQ send failure), error propagates,
          // resolveOffset is NOT called, and kafkajs re-delivers from last resolved offset.
          const occurredAt = await this.eventProcessor.processMessage(
            batch.topic,
            message,
            batch.partition,
          );
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
          await this.consumer?.commitOffsets([
            {
              topic: batch.topic,
              partition: batch.partition,
              offset: (BigInt(message.offset) + 1n).toString(),
            },
          ]);
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
}
