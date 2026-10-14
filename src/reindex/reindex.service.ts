import { Client } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { randomUUID } from 'crypto';
import {
  formatUtcTimestamp,
  getProductsAlias,
  getVersionedIndexName,
  MAPPING_VERSION,
} from '../common/constants';
import { RequestContext } from '../common/context/request-context';
import { ReindexState } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';
import {
  CONSUMER_STATUS_PROVIDER,
  IConsumerStatusProvider,
} from '../event-consumer/interfaces';
import {
  PRODUCTS_INDEX_MAPPING_V1,
  PRODUCTS_INDEX_SETTINGS_V1,
} from '../product-index/mappings';
import {
  AdminSearchStatusQueryDto,
  AdminSearchStatusResponseData,
  ReindexJobDetailsResponseData,
  ReindexRequestDto,
  ReindexStartResponseData,
} from './dto';
import {
  ALLOWED_REINDEX_ERROR_CODES,
  ReindexErrorCode,
  ReindexSource,
} from './enums';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';
import { ReindexRunner } from './reindex.runner';
import { assertTransition } from './state-machine';

const VALID_JOB_ID_REGEX = /^job-[a-zA-Z0-9_-]+$/;

@Injectable()
export class ReindexService {
  private readonly logger = new Logger(ReindexService.name);
  private readonly abortControllers = new Map<string, AbortController>();

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
    private readonly jobRepo: ReindexJobRepository,
    private readonly runner: ReindexRunner,
    @Optional()
    @Inject(CONSUMER_STATUS_PROVIDER)
    private readonly consumerStatusProvider?: IConsumerStatusProvider,
  ) {}

  static generateTargetIndexName(
    mappingVersion: number = MAPPING_VERSION,
    dateOrTimestamp?: Date | string,
    prefix = '',
  ): string {
    let ts: string | undefined;
    if (dateOrTimestamp instanceof Date) {
      ts = formatUtcTimestamp(dateOrTimestamp);
    } else if (typeof dateOrTimestamp === 'string') {
      if (dateOrTimestamp.includes('-') || dateOrTimestamp.includes(':')) {
        const parsed = new Date(dateOrTimestamp);
        ts = !isNaN(parsed.getTime())
          ? formatUtcTimestamp(parsed)
          : dateOrTimestamp;
      } else {
        ts = dateOrTimestamp;
      }
    }
    return getVersionedIndexName(mappingVersion, ts, prefix);
  }

  generateTargetIndexName(
    mappingVersion: number = MAPPING_VERSION,
    dateOrTimestamp?: Date | string,
    prefix = this.configService.esIndexPrefix,
  ): string {
    return ReindexService.generateTargetIndexName(
      mappingVersion,
      dateOrTimestamp,
      prefix,
    );
  }

  serializeJob(job: ReindexJobDocument): ReindexJobDetailsResponseData {
    let errorCode: string | null = null;
    if (job.state === ReindexState.FAILED) {
      if (job.error_code && ALLOWED_REINDEX_ERROR_CODES.has(job.error_code)) {
        errorCode = job.error_code;
      } else {
        errorCode = ReindexErrorCode.REINDEX_WORKER_ERROR;
      }
    }

    return {
      job_id: job.job_id,
      state: job.state,
      target_index: job.target_index,
      mapping_version: job.mapping_version,
      processed_count: job.processed_count,
      failed_count: job.failed_count,
      total_estimate: job.total_estimate ?? null,
      checkpoint: job.checkpoint ?? null,
      started_at: job.started_at,
      updated_at: job.updated_at,
      error_code: errorCode,
    };
  }

  async start(dto: ReindexRequestDto): Promise<ReindexStartResponseData> {
    if (dto.mapping_version !== MAPPING_VERSION) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        `Mapping version ${dto.mapping_version} is not supported. Current version is ${MAPPING_VERSION}.`,
      );
    }

    // Check active job across system
    const activeJob = await this.jobRepo.findActiveJob();
    if (activeJob) {
      throw new SearchException(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
    }

    const jobId = `job-${randomUUID()}`;
    await this.jobRepo.acquireLock(jobId);

    const prefix = this.configService.esIndexPrefix;
    const alias = getProductsAlias(prefix);
    let activeIndex: string | null = null;
    let totalEstimate: number | null = null;

    try {
      const aliasMap = await this.client.indices.getAlias({ name: alias });
      const indices = Object.keys(aliasMap).sort();
      if (indices.length > 0) {
        activeIndex = indices[0];
        const countRes = await this.client.count({ index: alias });
        totalEstimate = countRes.count;
      }
    } catch {
      // Alias does not exist yet
    }

    const targetIndex = this.generateTargetIndexName(
      dto.mapping_version,
      undefined,
      prefix,
    );

    try {
      await this.client.indices.create({
        index: targetIndex,
        mappings: PRODUCTS_INDEX_MAPPING_V1,
        settings: PRODUCTS_INDEX_SETTINGS_V1,
      });
    } catch (err: unknown) {
      await this.jobRepo.releaseLock(jobId);
      throw err;
    }

    const now = new Date().toISOString();
    const traceId = RequestContext.getTraceId();
    const requestId = RequestContext.getRequestId();

    const jobDoc: ReindexJobDocument = {
      job_id: jobId,
      target_index: targetIndex,
      mapping_version: dto.mapping_version,
      state: ReindexState.REQUESTED,
      checkpoint: null,
      total_estimate: totalEstimate,
      processed_count: 0,
      failed_count: 0,
      started_at: now,
      finished_at: null,
      updated_at: now,
      error_code: null,
      trace_id: traceId,
      request_id: requestId,
    };

    await this.jobRepo.create(jobDoc);

    const abortController = new AbortController();
    this.abortControllers.set(jobId, abortController);

    // Launch runner asynchronously without blocking the response
    const source = dto.source || ReindexSource.INDEX_COPY;
    void this.runner
      .executeJob(jobDoc, source, activeIndex, abortController.signal)
      .finally(() => {
        this.abortControllers.delete(jobId);
      });

    return {
      job_id: jobId,
      state: ReindexState.REQUESTED,
      target_index: targetIndex,
    };
  }

  async cancel(jobId: string): Promise<ReindexJobDetailsResponseData> {
    if (!jobId || !VALID_JOB_ID_REGEX.test(jobId)) {
      throw new SearchException(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
    }

    const record = await this.jobRepo.get(jobId);
    const job = record.doc;

    if (
      job.state !== ReindexState.REQUESTED &&
      job.state !== ReindexState.RUNNING &&
      job.state !== ReindexState.VALIDATING
    ) {
      throw new SearchException(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
    }

    const controller = this.abortControllers.get(jobId);
    if (controller) {
      controller.abort();
      this.abortControllers.delete(jobId);
    }

    try {
      await this.client.indices.delete({ index: job.target_index });
    } catch (err: unknown) {
      this.logger.debug(
        `Target index ${job.target_index} not deleted during cancel: ${err}`,
      );
    }

    assertTransition(job.state, ReindexState.CANCELLED);
    job.state = ReindexState.CANCELLED;
    job.finished_at = new Date().toISOString();
    job.updated_at = new Date().toISOString();
    job.error_code = null;

    await this.jobRepo.update(job, record._seq_no, record._primary_term);
    await this.jobRepo.releaseLock(jobId);

    return this.serializeJob(job);
  }

  async getJob(jobId: string): Promise<ReindexJobDetailsResponseData> {
    if (!jobId || !VALID_JOB_ID_REGEX.test(jobId)) {
      throw new SearchException(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
    }

    const record = await this.jobRepo.get(jobId);
    const job = record.doc;

    if (job.state === ReindexState.RUNNING) {
      const updatedTime = new Date(job.updated_at).getTime();
      if (Date.now() - updatedTime > 15 * 60 * 1000) {
        job.state = ReindexState.FAILED;
        job.error_code = ReindexErrorCode.REINDEX_TIMEOUT;
        job.finished_at = new Date().toISOString();
        job.updated_at = new Date().toISOString();
        await this.jobRepo.update(job, record._seq_no, record._primary_term);
        await this.jobRepo.releaseLock(job.job_id);
      }
    }

    return this.serializeJob(job);
  }

  async getStatus(
    query?: AdminSearchStatusQueryDto,
  ): Promise<AdminSearchStatusResponseData> {
    if (query?.index || query?.consumer_group) {
      this.logger.debug(
        `Status check requested with optional filter: index=${query.index}, group=${query.consumer_group}`,
      );
    }
    const prefix = this.configService.esIndexPrefix;
    const alias = getProductsAlias(prefix);

    let activeIndex: string | null = null;
    let documentCount = 0;

    try {
      const aliasMap = await this.client.indices.getAlias({ name: alias });
      const indices = Object.keys(aliasMap).sort();
      if (indices.length > 0) {
        activeIndex = indices[0];
        const countRes = await this.client.count({ index: alias });
        documentCount = countRes.count;
      }
    } catch {
      // Alias does not exist
    }

    let lastEventAt: string | null = null;
    let consumerLag: Record<string, number> = {};
    let dlqCount = 0;

    if (this.consumerStatusProvider) {
      try {
        const cs = await this.consumerStatusProvider.getStatus();
        lastEventAt = cs.lastEventAt ?? null;
        consumerLag = cs.lagByTopic ?? {};
        dlqCount = cs.dlqCount ?? 0;
      } catch (err: unknown) {
        this.logger.warn(`Failed to retrieve consumer status: ${err}`);
      }
    }

    let reindexState = 'IDLE';
    const latestRecord = await this.jobRepo.latest();

    if (latestRecord) {
      const latestJob = latestRecord.doc;
      if (latestJob.state === ReindexState.RUNNING) {
        const updatedTime = new Date(latestJob.updated_at).getTime();
        if (Date.now() - updatedTime > 15 * 60 * 1000) {
          latestJob.state = ReindexState.FAILED;
          latestJob.error_code = ReindexErrorCode.REINDEX_TIMEOUT;
          latestJob.finished_at = new Date().toISOString();
          latestJob.updated_at = new Date().toISOString();
          await this.jobRepo.update(
            latestJob,
            latestRecord._seq_no,
            latestRecord._primary_term,
          );
          await this.jobRepo.releaseLock(latestJob.job_id);
        }
      }
      reindexState = latestJob.state;
    }

    return {
      alias,
      active_index: activeIndex,
      mapping_version: MAPPING_VERSION,
      document_count: documentCount,
      last_event_at: lastEventAt,
      consumer_lag: consumerLag,
      dlq_count: dlqCount,
      reindex_state: reindexState,
    };
  }

  async executeAliasSwap(
    oldIndex: string | null,
    newIndex: string,
    alias: string,
  ): Promise<void> {
    const actions: Array<{
      remove?: { index: string; alias: string };
      add?: { index: string; alias: string };
    }> = [];

    if (oldIndex) {
      actions.push({ remove: { index: oldIndex, alias } });
    }
    actions.push({ add: { index: newIndex, alias } });

    await this.client.indices.updateAliases({ actions });
  }

  async rollbackAliasSwap(
    currentActiveIndex: string,
    previousIndex: string,
    alias: string,
  ): Promise<void> {
    const actions = [
      { remove: { index: currentActiveIndex, alias } },
      { add: { index: previousIndex, alias } },
    ];
    await this.client.indices.updateAliases({ actions });
  }

  async resumeBackfill(job: ReindexJobDocument): Promise<void> {
    return this.runner.resumeBackfill(job);
  }
}
