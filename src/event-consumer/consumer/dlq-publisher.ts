import { Client } from '@elastic/elasticsearch';
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
import { DomainEvent } from '../interfaces';
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
    if (!this.producer) {
      this.producer = this.kafka.producer();
    }
    await this.producer.connect();
  }

  async disconnect(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
    }
  }

  /**
   * Publishes a redacted dead letter message to the DLQ topic and records
   * the FAILED state in consumer-state.
   *
   * If DLQ write fails, the error is NOT swallowed: it is rethrown so the consumer
   * will not commit the message offset and KafkaJS will re-deliver the batch.
   */
  async publish(
    topic: string,
    evt: DomainEvent<unknown>,
    error: unknown,
    ctx: RequestContextData,
  ): Promise<void> {
    void topic;
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
      this.configService?.esIndexPrefix || '',
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
    } catch (err: unknown) {
      // Justified: recording failed status in consumer-state is best-effort before sending to DLQ
      this.logger.error(
        `Failed to record FAILED state for ${evt.event_id}: ${err}`,
      );
    }

    if (!this.producer) {
      this.producer = this.kafka.producer();
      await this.producer.connect();
    }

    const dlqTopic = getKafkaTopic(
      KAFKA_TOPICS.DLQ,
      this.configService?.kafkaTopicPrefix || '',
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
              ...(ctx.traceparent ? { traceparent: ctx.traceparent } : {}),
              ...(ctx.requestId ? { 'x-request-id': ctx.requestId } : {}),
            },
          },
        ],
      });
      this.logger.log(
        `Produced redacted event ${evt.event_id} to DLQ topic ${dlqTopic}`,
      );
    } catch (err: unknown) {
      this.logger.error(`Failed producing to DLQ topic ${dlqTopic}: ${err}`);
      // Re-throw so kafkajs does not commit and re-delivers the batch from the last resolved offset
      throw err;
    }
  }
}
