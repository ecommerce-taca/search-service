import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { Kafka } from 'kafkajs';
import { getKafkaTopic, KAFKA_TOPICS } from '../common/constants';
import { ReindexState } from '../common/enums';
import { AppConfigService } from '../config/config.service';
import {
  IIndexWriter,
  INDEX_WRITER,
  IProductDocumentBuilder,
  PRODUCT_DOCUMENT_BUILDER,
} from '../product-index/interfaces';
import { ReindexErrorCode } from './enums';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';
import { ReindexSource } from './reindex-source.interface';
import { decodeDebeziumScalar } from '../event-consumer/consumer/debezium';

@Injectable()
export class KafkaReplaySource implements ReindexSource {
  private readonly logger = new Logger(KafkaReplaySource.name);

  constructor(
    private readonly configService: AppConfigService,
    private readonly jobRepo: ReindexJobRepository,
    @Optional()
    @Inject(PRODUCT_DOCUMENT_BUILDER)
    private readonly documentBuilder?: IProductDocumentBuilder,
    @Optional()
    @Inject(INDEX_WRITER)
    private readonly indexWriter?: IIndexWriter,
    @Optional()
    @Inject(Kafka)
    private readonly kafka?: Kafka,
  ) {}

  async copy(
    job: ReindexJobDocument,
    targetIndex?: string,
    signal?: AbortSignal,
  ): Promise<void> {
    const destIndex = targetIndex || job.target_index;
    if (!this.documentBuilder || !this.indexWriter || !this.kafka) {
      job.state = ReindexState.FAILED;
      job.error_code = ReindexErrorCode.REINDEX_SOURCE_UNAVAILABLE;
      job.finished_at = new Date().toISOString();
      job.updated_at = new Date().toISOString();
      await this.jobRepo.update(job);
      return;
    }

    await this.replayKafkaEvents(job, destIndex, signal);
  }

  async resolveReplayStartingOffsets(
    job: ReindexJobDocument,
  ): Promise<Record<string, Record<number, string>>> {
    if (!job.checkpoint) return {};
    try {
      return JSON.parse(job.checkpoint);
    } catch {
      return {};
    }
  }

  async replayKafkaEvents(
    job: ReindexJobDocument,
    targetIndex: string = job.target_index,
    signal?: AbortSignal,
  ): Promise<void> {
    if (!this.kafka || !this.documentBuilder || !this.indexWriter) {
      job.state = ReindexState.FAILED;
      job.error_code = ReindexErrorCode.REINDEX_SOURCE_UNAVAILABLE;
      job.finished_at = new Date().toISOString();
      job.updated_at = new Date().toISOString();
      await this.jobRepo.update(job);
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

    const admin = this.kafka.admin();
    await admin.connect();

    try {
      const topicOffsets = await Promise.all(
        topics.map((t) => admin.fetchTopicOffsets(t).catch(() => [])),
      );

      const highWatermarks = new Map<string, Map<number, bigint>>();
      topics.forEach((t, i) => {
        const offsets = topicOffsets[i];
        const partMap = new Map<number, bigint>();
        for (const p of offsets) {
          partMap.set(p.partition, BigInt(p.high));
        }
        highWatermarks.set(t, partMap);
      });

      const groupId = `search-reindex-${job.job_id}`;
      const consumer = this.kafka.consumer({ groupId });
      await consumer.connect();

      try {
        await consumer.subscribe({ topics, fromBeginning: true });

        const committedOffsets: Record<string, Record<number, string>> = {};

        await consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            if (
              signal?.aborted ||
              (job.state as ReindexState) === ReindexState.CANCELLED
            ) {
              await consumer.stop();
              return;
            }

            if (!committedOffsets[topic]) {
              committedOffsets[topic] = {};
            }
            committedOffsets[topic][partition] = message.offset;
            job.checkpoint = JSON.stringify(committedOffsets);

            if (message.value) {
              try {
                const event = JSON.parse(message.value.toString());
                // Cùng contract Debezium như consumer chính: id ở header, key = aggregate_id.
                if (!event.event_id) {
                  const id = message.headers?.id;
                  event.event_id = decodeDebeziumScalar(
                    Array.isArray(id) ? id[0] : id,
                  );
                }
                if (!event.aggregate_id) {
                  event.aggregate_id = decodeDebeziumScalar(message.key);
                }
                if (typeof event.payload === 'string') {
                  event.payload = JSON.parse(event.payload);
                }
                const change = this.documentBuilder!.build(event, null);
                if (change.kind === 'UPSERT') {
                  await this.indexWriter!.bulkUpsert(targetIndex, [change.doc]);
                  job.processed_count++;
                } else if (change.kind === 'PATCH') {
                  await this.indexWriter!.bulkPartialUpdate(targetIndex, [
                    { id: change.id, doc: change.doc },
                  ]);
                  job.processed_count++;
                }
              } catch {
                job.failed_count++;
              }
            }

            job.updated_at = new Date().toISOString();
            await this.jobRepo.update(job);

            let allDone = true;
            for (const [top, partMap] of highWatermarks.entries()) {
              for (const [part, high] of partMap.entries()) {
                const current = committedOffsets[top]?.[part];
                if (!current || BigInt(current) + 1n < high) {
                  allDone = false;
                  break;
                }
              }
              if (!allDone) break;
            }

            if (allDone) {
              await consumer.stop();
            }
          },
        });
      } finally {
        await consumer.disconnect().catch(() => {});
      }
    } finally {
      await admin.disconnect().catch(() => {});
    }
  }
}
