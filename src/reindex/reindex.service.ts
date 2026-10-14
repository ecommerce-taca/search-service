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
import {
  ReindexJobDocument,
  ReindexJobRecord,
  ReindexJobRepository,
} from './job.repository';
import { ReindexRunner } from './reindex.runner';
import { assertTransition } from './state-machine';

export { ReindexErrorCode };
export const REINDEX_ERROR_CODES = ReindexErrorCode;
export const REINDEX_STALE_AFTER_MS = 15 * 60 * 1000;

const VALID_JOB_ID_REGEX =
  /^job-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

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
    prefix = '',
  ): string {
    return ReindexService.generateTargetIndexName(
      mappingVersion,
      dateOrTimestamp,
      prefix,
    );
  }

  formatErrorCode(
    state: ReindexState,
    errorCode: string | null,
  ): string | null {
    if (state !== ReindexState.FAILED) {
      return null;
    }
    return errorCode;
  }

  mapExceptionToErrorCode(error: unknown): ReindexErrorCode {
    if (
      error &&
      typeof error === 'object' &&
      'errorCode' in error &&
      typeof (error as { errorCode: string }).errorCode === 'string' &&
      Object.values(ReindexErrorCode).includes(
        (error as { errorCode: ReindexErrorCode }).errorCode,
      )
    ) {
      return (error as { errorCode: ReindexErrorCode }).errorCode;
    }
    return ReindexErrorCode.REINDEX_WORKER_ERROR;
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

    const details: ReindexJobDetailsResponseData = {
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
    if (job.finished_at) {
      details.finished_at = job.finished_at;
    }
    return details;
  }

  private async expireIfStale(
    record: ReindexJobRecord,
  ): Promise<ReindexJobDocument> {
    const job = record.doc ?? record;
    if (
      job.state === ReindexState.RUNNING ||
      job.state === ReindexState.VALIDATING
    ) {
      const updatedTime = new Date(job.updated_at).getTime();
      if (Date.now() - updatedTime > REINDEX_STALE_AFTER_MS) {
        job.state = ReindexState.FAILED;
        job.error_code = ReindexErrorCode.REINDEX_TIMEOUT;
        const now = new Date().toISOString();
        job.finished_at = now;
        job.updated_at = now;
        if (
          record._seq_no !== undefined &&
          record._primary_term !== undefined
        ) {
          await this.jobRepo.update(job, record._seq_no, record._primary_term);
        } else {
          await this.jobRepo.update(job);
        }
        await this.jobRepo.releaseLock(job.job_id);
      }
    }
    return job;
  }

  async start(dto: ReindexRequestDto): Promise<ReindexStartResponseData> {
    if (dto.mapping_version !== MAPPING_VERSION) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        `Mapping version ${dto.mapping_version} is not supported. Current version is ${MAPPING_VERSION}.`,
      );
    }

    const hasActive = await this.jobRepo.hasActiveJob();
    if (hasActive) {
      throw new SearchException(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
    }

    const jobId = `job-${randomUUID()}`;
    await this.jobRepo.acquireLock(jobId);

    const prefix = this.configService.esIndexPrefix;
    const alias = getProductsAlias(prefix);
    let totalEstimate: number | null = null;

    try {
      let aliasExists = true;
      if (typeof this.client.indices?.existsAlias === 'function') {
        aliasExists = Boolean(
          await this.client.indices.existsAlias({ name: alias }),
        );
      }
      if (aliasExists) {
        const aliasMap = await this.client.indices.getAlias({ name: alias });
        const indices = Object.keys(aliasMap).sort();
        if (indices.length > 0) {
          const countRes = await this.client.count({ index: alias });
          totalEstimate = countRes.count;
        }
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
      source: dto.source || ReindexSource.INDEX_COPY,
      reason: dto.reason,
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
    void this.runner.run(jobDoc).finally(() => {
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

    const maxRetries = 5;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const record = await this.jobRepo.get(jobId);
      if (!record) {
        throw new SearchException(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
      }
      const job = record.doc ?? record;

      if (job.state === ReindexState.CANCELLED) {
        return this.serializeJob(job);
      }

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
      this.runner.abort(jobId);

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

      try {
        if (
          record._seq_no !== undefined &&
          record._primary_term !== undefined
        ) {
          await this.jobRepo.update(job, record._seq_no, record._primary_term);
        } else {
          await this.jobRepo.update(job);
        }
        await this.jobRepo.releaseLock(jobId);

        return this.serializeJob(job);
      } catch (err: unknown) {
        if (
          err instanceof SearchException &&
          err.code === SearchErrorCode.SEARCH_REINDEX_CONFLICT &&
          attempt < maxRetries - 1
        ) {
          continue;
        }
        throw err;
      }
    }

    throw new SearchException(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
  }

  async getJob(jobId: string): Promise<ReindexJobDetailsResponseData> {
    if (!jobId || !VALID_JOB_ID_REGEX.test(jobId)) {
      throw new SearchException(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
    }

    const record = await this.jobRepo.get(jobId);
    if (!record) {
      throw new SearchException(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
    }

    const job = await this.expireIfStale(record);
    return this.serializeJob(job);
  }

  async getStatus(
    query?: AdminSearchStatusQueryDto,
  ): Promise<AdminSearchStatusResponseData> {
    void query;
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
      const latestJob = await this.expireIfStale(latestRecord);
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
}
