import { Client } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { Kafka } from 'kafkajs';
import {
  getProductsAlias,
  KAFKA_TOPICS,
  getKafkaTopic,
} from '../common/constants';
import { ReindexState } from '../common/enums';
import { AppConfigService } from '../config/config.service';
import {
  IIndexWriter,
  INDEX_WRITER,
  IProductDocumentBuilder,
  PRODUCT_DOCUMENT_BUILDER,
  ProductSearchDocument,
} from '../product-index/interfaces';
import { DomainEvent } from '../event-consumer/interfaces';
import { ReindexErrorCode, ReindexSource } from './enums';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';
import { assertTransition } from './state-machine';
import { ValidationGate } from './validation-gate';

@Injectable()
export class ReindexRunner {
  private readonly logger = new Logger(ReindexRunner.name);

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
    private readonly jobRepo: ReindexJobRepository,
    private readonly validationGate: ValidationGate,
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

  private isAbortedOrCancelled(
    job: ReindexJobDocument,
    signal?: AbortSignal,
  ): boolean {
    return Boolean(
      signal?.aborted || (job.state as ReindexState) === ReindexState.CANCELLED,
    );
  }

  async executeJob(
    job: ReindexJobDocument,
    source: ReindexSource,
    activeIndex: string | null,
    signal?: AbortSignal,
  ): Promise<void> {
    try {
      if (this.isAbortedOrCancelled(job, signal)) return;

      // 1. Transition to RUNNING
      assertTransition(job.state, ReindexState.RUNNING);
      job.state = ReindexState.RUNNING;
      job.updated_at = new Date().toISOString();
      await this.jobRepo.update(job);

      // 2. Execute backfill based on source
      if (source === ReindexSource.INDEX_COPY) {
        if (!activeIndex) {
          job.processed_count = 0;
          job.failed_count = 0;
          job.updated_at = new Date().toISOString();
          await this.jobRepo.update(job);
        } else {
          await this.copyStep(job, activeIndex, signal);
        }
      } else if (source === ReindexSource.KAFKA_REPLAY) {
        if (!this.documentBuilder || !this.indexWriter) {
          job.state = ReindexState.FAILED;
          job.error_code = ReindexErrorCode.REINDEX_SOURCE_UNAVAILABLE;
          job.finished_at = new Date().toISOString();
          job.updated_at = new Date().toISOString();
          await this.jobRepo.update(job);
          return;
        }
        await this.replayKafkaEvents(job, signal);
      }

      if (this.isAbortedOrCancelled(job, signal)) return;

      // 3. Transition to VALIDATING
      assertTransition(job.state, ReindexState.VALIDATING);
      job.state = ReindexState.VALIDATING;
      job.updated_at = new Date().toISOString();
      await this.jobRepo.update(job);

      // 4. Run validation gates
      await this.gateStep(job, activeIndex);

      if (this.isAbortedOrCancelled(job, signal)) return;

      // 5. Swap alias
      await this.swapStep(job, activeIndex);
    } catch (err: unknown) {
      if (this.isAbortedOrCancelled(job, signal)) {
        return;
      }

      const errorClass =
        (err as object)?.constructor?.name || typeof err || 'UnknownError';
      this.logger.error(`Reindex runner caught error [${errorClass}]`);

      if (!job.error_code) {
        job.error_code = ReindexErrorCode.REINDEX_WORKER_ERROR;
      }
      job.state = ReindexState.FAILED;
      job.finished_at = new Date().toISOString();
      job.updated_at = new Date().toISOString();

      try {
        await this.jobRepo.update(job);
      } catch (repoErr) {
        this.logger.warn(`Failed to persist failed job state: ${repoErr}`);
      }
    } finally {
      await this.jobRepo.releaseLock(job.job_id);
    }
  }

  async copyStep(
    job: ReindexJobDocument,
    activeIndex: string,
    signal?: AbortSignal,
  ): Promise<void> {
    const reindexTask = await this.client.reindex({
      wait_for_completion: false,
      conflicts: 'proceed',
      source: { index: activeIndex },
      dest: { index: job.target_index },
    });

    const rawTaskId = reindexTask.task;
    const taskId = rawTaskId != null ? String(rawTaskId) : null;
    job.checkpoint = taskId;
    job.updated_at = new Date().toISOString();
    await this.jobRepo.update(job);

    if (!taskId) return;

    while (!this.isAbortedOrCancelled(job, signal)) {
      await new Promise((resolve) => setTimeout(resolve, 2000));
      if (this.isAbortedOrCancelled(job, signal)) break;

      const taskInfo = (await this.client.tasks.get({
        task_id: taskId,
      })) as {
        completed: boolean;
        task?: {
          status?: {
            total?: number;
            created?: number;
            updated?: number;
            version_conflicts?: number;
          };
        };
        response?: {
          created?: number;
          updated?: number;
          version_conflicts?: number;
          failures?: unknown[];
        };
        error?: unknown;
      };

      const status = taskInfo.task?.status;
      const response = taskInfo.response;
      job.processed_count =
        (response?.created ?? status?.created ?? 0) +
        (response?.updated ?? status?.updated ?? 0);
      job.failed_count =
        response?.version_conflicts ?? status?.version_conflicts ?? 0;
      job.updated_at = new Date().toISOString();
      await this.jobRepo.update(job);

      if (taskInfo.completed) {
        if (
          taskInfo.error ||
          (response?.failures && response.failures.length > 0)
        ) {
          job.error_code = ReindexErrorCode.REINDEX_WORKER_ERROR;
          throw new Error(`Reindex task failed: ${taskId}`);
        }
        break;
      }
    }
  }

  async gateStep(
    job: ReindexJobDocument,
    activeIndex: string | null,
  ): Promise<void> {
    // 1. Refresh target
    await this.client.indices.refresh({ index: job.target_index });

    // 2. Count gate
    const targetCountRes = await this.client.count({ index: job.target_index });
    const targetCount = targetCountRes.count;
    let activeCount = job.total_estimate ?? 0;
    if (activeIndex && job.total_estimate == null) {
      const activeCountRes = await this.client.count({ index: activeIndex });
      activeCount = activeCountRes.count;
    }

    const countGate = this.validationGate.checkDocumentCount(
      targetCount,
      activeCount,
    );
    if (!countGate.pass) {
      job.error_code = countGate.reason!;
      throw new Error(`Count gate failed: ${countGate.reason}`);
    }

    // 3. Version regression gate
    if (activeIndex && activeCount > 0) {
      const versionGate = await this.validationGate.checkSourceVersions(
        job.target_index,
        activeIndex,
      );
      if (!versionGate.pass) {
        job.error_code = versionGate.reason!;
        throw new Error(`Version gate failed: ${versionGate.reason}`);
      }
    }

    // 4. Smoke queries gate
    const smokeGate = await this.validationGate.runSmokeQueries(
      job.target_index,
    );
    if (!smokeGate.pass) {
      job.error_code = smokeGate.reason!;
      throw new Error(`Smoke queries gate failed: ${smokeGate.reason}`);
    }
  }

  async swapStep(
    job: ReindexJobDocument,
    activeIndex: string | null,
  ): Promise<void> {
    const alias = getProductsAlias(this.configService.esIndexPrefix);
    const actions: Array<{
      remove?: { index: string; alias: string };
      add?: { index: string; alias: string };
    }> = [];

    if (activeIndex) {
      actions.push({ remove: { index: activeIndex, alias } });
    }
    actions.push({ add: { index: job.target_index, alias } });

    await this.client.indices.updateAliases({ actions });
    this.logger.log(
      `Swapped alias ${alias} to ${job.target_index}. Old index ${activeIndex ?? 'none'} retained.`,
    );

    assertTransition(job.state, ReindexState.SWAPPED);
    job.state = ReindexState.SWAPPED;
    job.finished_at = new Date().toISOString();
    job.updated_at = new Date().toISOString();
    job.error_code = null;
    await this.jobRepo.update(job);
  }

  async resumeBackfill(job: ReindexJobDocument): Promise<void> {
    this.logger.log(
      `Resuming backfill for job ${job.job_id} from checkpoint ${job.checkpoint}`,
    );
  }

  private async replayKafkaEvents(
    job: ReindexJobDocument,
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
      // Capture high watermarks
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
            if (this.isAbortedOrCancelled(job, signal)) {
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
                const event = JSON.parse(
                  message.value.toString(),
                ) as DomainEvent<unknown>;
                const change = this.documentBuilder!.build(event, null);

                if (change.kind === 'UPSERT') {
                  await this.indexWriter!.bulkUpsert(job.target_index, [
                    change.doc as ProductSearchDocument,
                  ]);
                  job.processed_count++;
                } else if (change.kind === 'PATCH') {
                  await this.indexWriter!.bulkPartialUpdate(job.target_index, [
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

            // Check if all partitions reached high watermarks
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
