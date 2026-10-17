import { Client, errors } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';
import {
  getConsumerStateIndex,
  getKafkaTopic,
  KAFKA_TOPICS,
} from '../../common/constants';
import { RequestContextData } from '../../common/context/request-context';
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { AppConfigService } from '../../config/config.service';
import { ELASTICSEARCH_CLIENT } from '../../product-index/elasticsearch.module';
import { DlqEnvelope, DlqMetadata, DomainEvent } from '../interfaces';
import { KAFKA_CLIENT } from '../kafka.module';

@Injectable()
export class DlqPublisher {
  private readonly logger = new Logger(DlqPublisher.name);
  private producer?: Producer;

  constructor(
    private readonly configService: AppConfigService,
    @Inject(ELASTICSEARCH_CLIENT) private readonly esClient: Client,
    @Inject(KAFKA_CLIENT) private readonly kafka: Kafka,
  ) {}

  async connect(): Promise<void> {
    if (!this.producer) this.producer = this.kafka.producer();
    await this.producer.connect();
  }

  async disconnect(): Promise<void> {
    if (this.producer) await this.producer.disconnect();
  }

  async recordConsumerState(
    evt: DomainEvent<unknown>,
    status: string,
    errorCode: string | null = null,
  ): Promise<void> {
    const index = getConsumerStateIndex(this.configService.esIndexPrefix);
    try {
      await this.esClient.index({
        index,
        id: evt.event_id,
        document: {
          event_id: evt.event_id,
          aggregate_id: evt.aggregate_id || '',
          source_version: evt.version ?? 0,
          processed_at: new Date().toISOString(),
          status,
          error_code: errorCode,
          ...(evt.replay_of_event_id
            ? { replay_of_event_id: evt.replay_of_event_id }
            : {}),
        },
      });
    } catch (err: unknown) {
      if (err instanceof errors.ResponseError && err.statusCode === 409) {
        this.logger.debug(`409 recording state for ${evt.event_id}`);
        return;
      }
      this.logger.warn(
        `Failed recording state for ${evt.event_id}: ${err instanceof Error ? err.name : 'UnknownError'}`,
      );
    }
  }

  /**
   * Publishes redacted dead letter message to DLQ and records DLQ state in consumer-state.
   * Rethrows on publish failure so consumer does not commit offset.
   */
  async publish(
    topic: string,
    evt: DomainEvent<unknown>,
    error: unknown,
    ctx: RequestContextData,
    meta: DlqMetadata = {},
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

    if (!this.producer) {
      this.producer = this.kafka.producer();
      await this.producer.connect();
    }

    const dlqTopic = getKafkaTopic(
      KAFKA_TOPICS.DLQ,
      this.configService.kafkaTopicPrefix,
    );

    const redactedEnvelope: DlqEnvelope = {
      event_id: evt.event_id,
      event_type: evt.event_type,
      aggregate_id: evt.aggregate_id,
      version: evt.version,
      error_code: String(errorCode),
      source_topic: topic,
      source_partition: meta.partition ?? 0,
      source_offset: meta.offset ?? '',
      attempts: meta.attempts ?? 1,
      failed_at: meta.failed_at ?? new Date().toISOString(),
    };

    try {
      await this.producer.send({
        topic: dlqTopic,
        messages: [
          {
            key: evt.aggregate_id || evt.event_id,
            value: JSON.stringify(redactedEnvelope),
            headers: {
              ...(ctx.traceparent ? { traceparent: ctx.traceparent } : {}),
              ...(ctx.requestId ? { 'x-request-id': ctx.requestId } : {}),
            },
          },
        ],
      });
      this.logger.log(`Produced redacted event ${evt.event_id} to ${dlqTopic}`);
    } catch (err: unknown) {
      this.logger.error(
        `Failed producing to DLQ topic ${dlqTopic}: ${err instanceof Error ? err.name : 'UnknownError'}`,
      );
      throw err;
    }

    await this.recordConsumerState(evt, 'DLQ', String(errorCode));
  }

  /**
   * Publishes unparseable or missing event_id messages to DLQ without payload.
   */
  async publishInvalidMessage(
    topic: string,
    partition: number,
    offset: string,
    ctx: RequestContextData,
  ): Promise<void> {
    if (!this.producer) {
      this.producer = this.kafka.producer();
      await this.producer.connect();
    }

    const dlqTopic = getKafkaTopic(
      KAFKA_TOPICS.DLQ,
      this.configService.kafkaTopicPrefix,
    );

    const redactedEnvelope = {
      topic,
      partition,
      offset,
      error_code: 'INVALID_EVENT',
    };

    try {
      await this.producer.send({
        topic: dlqTopic,
        messages: [
          {
            key: `${topic}-${partition}-${offset}`,
            value: JSON.stringify(redactedEnvelope),
            headers: {
              ...(ctx.traceparent ? { traceparent: ctx.traceparent } : {}),
              ...(ctx.requestId ? { 'x-request-id': ctx.requestId } : {}),
            },
          },
        ],
      });
      this.logger.log(
        `Produced invalid event (offset ${offset}) to ${dlqTopic}`,
      );
    } catch (err: unknown) {
      this.logger.error(
        `Failed producing invalid event to ${dlqTopic}: ${err instanceof Error ? err.name : 'UnknownError'}`,
      );
      throw err;
    }
  }
}
