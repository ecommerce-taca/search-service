import { Client, estypes } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { Kafka } from 'kafkajs';
import {
  getKafkaTopic,
  getProductsAlias,
  KAFKA_TOPICS,
} from '../common/constants';
import { ReindexState } from '../common/enums';
import { AppConfigService } from '../config/config.service';
import {
  IIndexWriter,
  INDEX_WRITER,
  IProductDocumentBuilder,
  PRODUCT_DOCUMENT_BUILDER,
} from '../product-index/interfaces';
import { ReindexErrorCode, ReindexSource } from './enums';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';
import { assertTransition } from './state-machine';
import { ValidationGate } from './validation-gate';

@Injectable()
export class ReindexRunner {
  private readonly logger = new Logger(ReindexRunner.name);
  private readonly abortControllers = new Map<string, AbortController>();
  private readonly abortedJobIds = new Set<string>();

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
    private readonly jobRepo: ReindexJobRepository,
    @Optional()
    private readonly validationGate?: ValidationGate,
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

  abort(jobId: string): void {
    this.abortedJobIds.add(jobId);
    const controller = this.abortControllers.get(jobId);
    if (controller) {
      controller.abort();
    }
  }

  private isAbortedOrCancelled(
    job: ReindexJobDocument,
    signal?: AbortSignal,
  ): boolean {
    const internalSignal = this.abortControllers.get(job.job_id)?.signal;
    return Boolean(
      this.abortedJobIds.has(job.job_id) ||
      signal?.aborted ||
      internalSignal?.aborted ||
      (job.state as ReindexState) === ReindexState.CANCELLED,
    );
  }

  async run(job: ReindexJobDocument, signal?: AbortSignal): Promise<void> {
    const controller = new AbortController();
    this.abortControllers.set(job.job_id, controller);
    if (this.abortedJobIds.has(job.job_id)) {
      controller.abort();
    }
    if (signal) {
      if (signal.aborted) {
        controller.abort();
      } else {
        signal.addEventListener('abort', () => controller.abort());
      }
    }

    try {
      const alias = getProductsAlias(this.configService.esIndexPrefix);
      let activeIndex: string | null = null;
      try {
        const aliasMap = await this.client.indices.getAlias({ name: alias });
        const indices = Object.keys(aliasMap).sort();
        if (indices.length > 0) {
          activeIndex = indices[0];
        }
      } catch {
        // Alias does not exist
      }

      const source = (job.source as ReindexSource) || ReindexSource.INDEX_COPY;
      await this.executeJob(job, source, activeIndex, controller.signal);
    } finally {
      this.abortControllers.delete(job.job_id);
      this.abortedJobIds.delete(job.job_id);
    }
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
      if (this.isAbortedOrCancelled(job, signal)) return;
      await this.jobRepo.update(job);

      // 2. Execute backfill based on source
      if (source === ReindexSource.INDEX_COPY) {
        if (!activeIndex) {
          job.processed_count = 0;
          job.failed_count = 0;
          job.updated_at = new Date().toISOString();
          if (this.isAbortedOrCancelled(job, signal)) return;
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
          if (this.isAbortedOrCancelled(job, signal)) return;
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
      if (this.isAbortedOrCancelled(job, signal)) return;
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
      if (!this.isAbortedOrCancelled(job, signal)) {
        await this.jobRepo.releaseLock(job.job_id);
      }
    }
  }

  async executeCopyStep(
    job: ReindexJobDocument,
    signal?: AbortSignal,
  ): Promise<void> {
    const alias = getProductsAlias(this.configService.esIndexPrefix);
    let activeIndex: string | null = null;
    try {
      const aliasMap = await this.client.indices.getAlias({ name: alias });
      const indices = Object.keys(aliasMap).sort();
      if (indices.length > 0) activeIndex = indices[0];
    } catch {
      // no active index
    }

    if (!activeIndex) {
      job.processed_count = 0;
      job.failed_count = 0;
      job.updated_at = new Date().toISOString();
      await this.jobRepo.update(job);
      return;
    }

    await this.copyStep(job, activeIndex, signal);
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

    const rawTaskId = (reindexTask as { task?: string | number }).task;
    const taskId = rawTaskId != null ? String(rawTaskId) : null;
    job.checkpoint = taskId;
    job.updated_at = new Date().toISOString();
    await this.jobRepo.updateProgress(job.job_id, {
      checkpoint: taskId,
      processed_count: job.processed_count,
      failed_count: job.failed_count,
    });

    if (!taskId) return;

    while (!this.isAbortedOrCancelled(job, signal)) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      if (this.isAbortedOrCancelled(job, signal)) break;

      const taskInfo: estypes.TasksGetResponse = await this.client.tasks.get({
        task_id: taskId,
      });

      if (this.isAbortedOrCancelled(job, signal)) break;

      const status = taskInfo.task?.status as
        | {
            total?: number;
            created?: number;
            updated?: number;
            version_conflicts?: number;
          }
        | undefined;
      const response = taskInfo.response as
        | {
            created?: number;
            updated?: number;
            version_conflicts?: number;
            failures?: unknown[];
          }
        | undefined;

      job.processed_count =
        (response?.created ?? status?.created ?? 0) +
        (response?.updated ?? status?.updated ?? 0);
      job.failed_count =
        response?.version_conflicts ?? status?.version_conflicts ?? 0;
      job.updated_at = new Date().toISOString();

      if (this.isAbortedOrCancelled(job, signal)) break;

      await this.jobRepo.updateProgress(job.job_id, {
        checkpoint: taskId,
        processed_count: job.processed_count,
        failed_count: job.failed_count,
      });

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

  async checkDocumentCountGate(
    sourceIndex: string,
    targetIndex: string,
  ): Promise<{
    passed: boolean;
    pass: boolean;
    status: 'PASS' | 'FAIL';
    diffPercent?: number;
    errorCode?: ReindexErrorCode;
    reason?: ReindexErrorCode;
  }> {
    let sourceCount = 0;
    let targetCount = 0;

    try {
      const s = await this.client.count({ index: sourceIndex });
      sourceCount = s.count;
    } catch {
      // empty
    }

    try {
      const t = await this.client.count({ index: targetIndex });
      targetCount = t.count;
    } catch {
      // empty
    }

    if (sourceCount === 0) {
      return {
        passed: true,
        pass: true,
        status: 'PASS',
        diffPercent: 0,
      };
    }

    if (targetCount === 0 && sourceCount > 0) {
      return {
        passed: false,
        pass: false,
        status: 'FAIL',
        diffPercent: 100,
        errorCode: ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
        reason: ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
      };
    }

    const diffPercent =
      (Math.abs(targetCount - sourceCount) / sourceCount) * 100;
    const passed = diffPercent <= 0.1;

    return {
      passed,
      pass: passed,
      status: passed ? 'PASS' : 'FAIL',
      diffPercent,
      errorCode: passed
        ? undefined
        : ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
      reason: passed ? undefined : ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
    };
  }

  async checkSourceVersionGate(
    sourceIndex: string,
    targetIndex: string,
  ): Promise<{
    passed: boolean;
    pass: boolean;
    status: 'PASS' | 'FAIL';
    errorCode?: ReindexErrorCode;
    reason?: ReindexErrorCode;
  }> {
    try {
      const sourceRes = await this.client.search<{
        product_id: string;
        source_version: number;
      }>({
        index: sourceIndex,
        size: 200,
        _source: ['product_id', 'source_version'],
      });

      const sourceHits = sourceRes.hits?.hits ?? [];
      if (sourceHits.length === 0) {
        return { passed: true, pass: true, status: 'PASS' };
      }

      const targetRes = await this.client.search<{
        product_id: string;
        source_version: number;
      }>({
        index: targetIndex,
        size: 200,
        _source: ['product_id', 'source_version'],
      });

      const targetHits = targetRes.hits?.hits ?? [];
      const targetMap = new Map<string, number>();
      for (const h of targetHits) {
        const id = h._id || h._source?.product_id;
        if (id && h._source) {
          targetMap.set(id, h._source.source_version);
        }
      }

      for (const s of sourceHits) {
        const id = s._id || s._source?.product_id;
        const sVersion = s._source?.source_version ?? 0;
        const tVersion = id ? targetMap.get(id) : undefined;
        if (tVersion === undefined || tVersion < sVersion) {
          return {
            passed: false,
            pass: false,
            status: 'FAIL',
            errorCode: ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION,
            reason: ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION,
          };
        }
      }

      return { passed: true, pass: true, status: 'PASS' };
    } catch {
      return {
        passed: false,
        pass: false,
        status: 'FAIL',
        errorCode: ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION,
        reason: ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION,
      };
    }
  }

  async checkSmokeQueriesGate(targetIndex: string): Promise<{
    passed: boolean;
    pass: boolean;
    status: 'PASS' | 'FAIL';
    errorCode?: ReindexErrorCode;
    reason?: ReindexErrorCode;
  }> {
    try {
      const smokeRes = await this.client.search<{
        visibility_status?: string;
      }>({
        index: targetIndex,
        query: {
          bool: {
            filter: [{ term: { visibility_status: 'PUBLISHED' } }],
          },
        },
        aggs: {
          category_ids: { terms: { field: 'category_ids', size: 10 } },
          brand: { terms: { field: 'brand', size: 10 } },
          attribute_pairs: { terms: { field: 'attribute_pairs', size: 10 } },
        },
      });

      if (smokeRes._shards?.failed && smokeRes._shards.failed > 0) {
        return {
          passed: false,
          pass: false,
          status: 'FAIL',
          errorCode: ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
          reason: ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
        };
      }

      const totalValue =
        typeof smokeRes.hits?.total === 'number'
          ? smokeRes.hits.total
          : (smokeRes.hits?.total?.value ?? 0);

      if (totalValue === 0) {
        return {
          passed: false,
          pass: false,
          status: 'FAIL',
          errorCode: ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
          reason: ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
        };
      }

      const aggs = (smokeRes.aggregations ?? {}) as Record<
        string,
        { buckets?: unknown[] }
      >;
      if (
        (aggs.category_ids && (aggs.category_ids.buckets?.length ?? 0) === 0) ||
        (aggs.brand && (aggs.brand.buckets?.length ?? 0) === 0) ||
        (aggs.attribute_pairs &&
          (aggs.attribute_pairs.buckets?.length ?? 0) === 0)
      ) {
        return {
          passed: false,
          pass: false,
          status: 'FAIL',
          errorCode: ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
          reason: ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
        };
      }

      const hits = smokeRes.hits?.hits ?? [];
      for (const hit of hits) {
        if (
          hit._source?.visibility_status &&
          hit._source.visibility_status !== 'PUBLISHED'
        ) {
          return {
            passed: false,
            pass: false,
            status: 'FAIL',
            errorCode: ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
            reason: ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
          };
        }
      }

      return { passed: true, pass: true, status: 'PASS' };
    } catch {
      return {
        passed: false,
        pass: false,
        status: 'FAIL',
        errorCode: ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
        reason: ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
      };
    }
  }

  async gateStep(
    job: ReindexJobDocument,
    activeIndex: string | null,
  ): Promise<void> {
    await this.client.indices.refresh({ index: job.target_index });

    if (activeIndex) {
      const countGate = await this.checkDocumentCountGate(
        activeIndex,
        job.target_index,
      );
      if (!countGate.passed) {
        job.error_code = countGate.errorCode!;
        throw new Error(`Count gate failed: ${countGate.errorCode}`);
      }
    }

    if (activeIndex) {
      const versionGate = await this.checkSourceVersionGate(
        activeIndex,
        job.target_index,
      );
      if (!versionGate.passed) {
        job.error_code = versionGate.errorCode!;
        throw new Error(`Version gate failed: ${versionGate.errorCode}`);
      }
    }

    const smokeGate = await this.checkSmokeQueriesGate(job.target_index);
    if (!smokeGate.passed) {
      job.error_code = smokeGate.errorCode!;
      throw new Error(`Smoke queries gate failed: ${smokeGate.errorCode}`);
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

    await this.client.indices.updateAliases({
      body: { actions },
    });
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

  logSafeProgress(data: Record<string, unknown>): void {
    const sensitiveKeys = [
      'token',
      'jwt',
      'secret',
      'raw_query',
      'password',
      'authorization',
    ];
    const sanitized: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(data)) {
      const lower = k.toLowerCase();
      if (!sensitiveKeys.some((s) => lower.includes(s))) {
        sanitized[k] = v;
      }
    }
    console.log(JSON.stringify(sanitized));
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
                const event = JSON.parse(message.value.toString());
                const change = this.documentBuilder!.build(event, null);
                if (change.kind === 'UPSERT') {
                  await this.indexWriter!.bulkUpsert(job.target_index, [
                    change.doc,
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
