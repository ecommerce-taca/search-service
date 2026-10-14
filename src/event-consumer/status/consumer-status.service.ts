import { Client, errors } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { Kafka } from 'kafkajs';
import {
  EVENT_MAX_LAG_ALERT_S,
  getConsumerStateIndex,
  getKafkaTopic,
  KAFKA_TOPICS,
} from '../../common/constants';
import { ConsumerState } from '../../common/enums';
import { AppConfigService } from '../../config/config.service';
import { ELASTICSEARCH_CLIENT } from '../../product-index/elasticsearch.module';
import { KAFKA_CLIENT } from '../kafka.module';
import {
  ConsumerStatus,
  CONSUMER_STATUS_PROVIDER,
  IConsumerStatusProvider,
} from '../interfaces';

@Injectable()
export class ConsumerStatusService implements IConsumerStatusProvider {
  private readonly logger = new Logger(ConsumerStatusService.name);

  private cachedStatus: ConsumerStatus | null = null;
  private lastFetchedTime = 0;
  private readonly CACHE_TTL_MS = 5000;

  // Tracked live by consumer
  private isRunning = false;
  private lastEventAt: string | null = null;
  private liveLagByTopic: Record<string, number> = {};
  private lastEventOccurredEpochMs = 0;

  constructor(
    @Inject(ELASTICSEARCH_CLIENT) private readonly client: Client,
    private readonly configService: AppConfigService,
    @Optional() @Inject(KAFKA_CLIENT) private readonly kafka?: Kafka,
  ) {
    this.isRunning = this.configService.kafkaConsumerEnabled;
  }

  setConsumerState(running: boolean): void {
    this.isRunning = running;
  }

  recordEventProcessed(
    topic: string,
    occurredAt: string | null,
    lag?: number,
  ): void {
    if (occurredAt) {
      this.lastEventAt = occurredAt;
      const parsedEpoch = new Date(occurredAt).getTime();
      if (!isNaN(parsedEpoch)) {
        this.lastEventOccurredEpochMs = parsedEpoch;
      }
    }
    if (typeof lag === 'number') {
      this.liveLagByTopic[topic] = Math.max(0, lag);
    }
  }

  setTopicLag(topic: string, lag: number): void {
    this.liveLagByTopic[topic] = Math.max(0, lag);
  }

  async getStatus(): Promise<ConsumerStatus> {
    const now = Date.now();
    if (
      this.cachedStatus &&
      now - this.lastFetchedTime < this.CACHE_TTL_MS
    ) {
      return this.cachedStatus;
    }

    const dlqCount = await this.getDlqCount();
    const lagByTopic = await this.computeLagByTopic();

    const totalLag = Object.values(lagByTopic).reduce(
      (acc, val) => acc + val,
      0,
    );

    let maxLagSeconds = 0;
    if (totalLag > 0 && this.lastEventOccurredEpochMs > 0) {
      maxLagSeconds = Math.max(
        0,
        Math.floor((now - this.lastEventOccurredEpochMs) / 1000),
      );
    }

    let state: ConsumerState;
    if (!this.configService.kafkaConsumerEnabled || !this.isRunning) {
      state = ConsumerState.PAUSED;
    } else if (dlqCount > 0) {
      state = ConsumerState.DLQ;
    } else if (
      totalLag > 0 ||
      maxLagSeconds > EVENT_MAX_LAG_ALERT_S
    ) {
      state = ConsumerState.DEGRADED;
    } else {
      state = ConsumerState.RUNNING;
    }

    const status: ConsumerStatus = {
      state,
      lagByTopic,
      maxLagSeconds,
      lastEventAt: this.lastEventAt,
      dlqCount,
    };

    this.cachedStatus = status;
    this.lastFetchedTime = now;
    return status;
  }

  private async getDlqCount(): Promise<number> {
    const consumerStateIndex = getConsumerStateIndex(
      this.configService.esIndexPrefix,
    );
    try {
      const response = await this.client.count({
        index: consumerStateIndex,
        query: {
          term: { status: 'FAILED' },
        },
      });
      return response.count ?? 0;
    } catch (err: unknown) {
      if (
        (err instanceof errors.ResponseError ||
          (err as { name?: string })?.name === 'ResponseError') &&
        ((err as { statusCode?: number })?.statusCode === 404 ||
          (err as { meta?: { statusCode?: number } })?.meta?.statusCode === 404)
      ) {
        return 0;
      }
      this.logger.debug(`Could not count DLQ consumer-state docs: ${err}`);
      return 0;
    }
  }

  private async computeLagByTopic(): Promise<Record<string, number>> {
    const topics = [
      KAFKA_TOPICS.PRODUCT_EVENTS,
      KAFKA_TOPICS.SKU_EVENTS,
      KAFKA_TOPICS.CATEGORY_EVENTS,
      KAFKA_TOPICS.CATALOG_EVENTS,
      KAFKA_TOPICS.RATING_EVENTS,
    ];

    const result: Record<string, number> = {};
    for (const topic of topics) {
      const prefixed = getKafkaTopic(
        topic,
        this.configService.kafkaTopicPrefix,
      );
      result[prefixed] = this.liveLagByTopic[prefixed] ?? 0;
    }

    if (!this.kafka || !this.configService.kafkaConsumerEnabled) {
      return result;
    }

    try {
      const admin = this.kafka.admin();
      await admin.connect();
      try {
        const prefixedTopics = Object.keys(result);
        for (const topic of prefixedTopics) {
          try {
            const topicOffsets = await admin.fetchTopicOffsets(topic);
            const consumerOffsets = await admin.fetchOffsets({
              groupId: this.configService.kafkaGroupId,
              topics: [topic],
            });

            let topicLag = 0;
            const consumerTopic = consumerOffsets.find((t) => t.topic === topic);

            for (const partitionOffset of topicOffsets) {
              const partitionId = partitionOffset.partition;
              const high = parseInt(partitionOffset.high, 10);
              const consumerPartition = consumerTopic?.partitions.find(
                (p) => p.partition === partitionId,
              );
              const committed = consumerPartition
                ? parseInt(consumerPartition.offset, 10)
                : 0;

              if (!isNaN(high) && !isNaN(committed) && high > committed) {
                topicLag += high - committed;
              }
            }
            result[topic] = topicLag;
            this.liveLagByTopic[topic] = topicLag;
          } catch {
            // keep fallback
          }
        }
      } finally {
        await admin.disconnect();
      }
    } catch {
      // Fallback to tracked liveLagByTopic
    }

    return result;
  }
}
