import { Client } from '@elastic/elasticsearch';
import { Injectable, Logger } from '@nestjs/common';
import { getReindexJobsIndex, MAPPING_VERSION } from '../common/constants';
import { ReindexState } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';

export interface ReindexJobDocument {
  job_id: string;
  target_index: string;
  mapping_version: number;
  state: ReindexState;
  checkpoint: string | null;
  total_estimate?: number | null;
  processed_count: number;
  failed_count: number;
  started_at: string;
  finished_at: string | null;
  updated_at: string;
  error_code: string | null;
  trace_id?: string;
  request_id?: string;
}

export interface ReindexJobRecord {
  doc: ReindexJobDocument;
  _seq_no?: number;
  _primary_term?: number;
}

const ACTIVE_LOCK_ID = 'active-lock';
const VALID_JOB_ID_REGEX = /^job-[a-zA-Z0-9_-]+$/;

@Injectable()
export class ReindexJobRepository {
  private readonly logger = new Logger(ReindexJobRepository.name);
  private mappingChecked = false;

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
  ) {}

  get indexName(): string {
    return getReindexJobsIndex(this.configService.esIndexPrefix);
  }

  async ensureMapping(): Promise<void> {
    if (this.mappingChecked) return;
    try {
      await this.client.indices.putMapping({
        index: this.indexName,
        properties: {
          total_estimate: { type: 'long' },
        },
      });
      this.mappingChecked = true;
    } catch {
      // Ignore if index doesn't exist yet or already has the property
    }
  }

  async acquireLock(jobId: string): Promise<void> {
    const lockDoc = {
      job_id: jobId,
      target_index: ACTIVE_LOCK_ID,
      mapping_version: MAPPING_VERSION,
      state: ReindexState.RUNNING,
      updated_at: new Date().toISOString(),
    };

    try {
      await this.client.index({
        index: this.indexName,
        id: ACTIVE_LOCK_ID,
        document: lockDoc,
        op_type: 'create',
        refresh: 'wait_for',
      });
    } catch (err: unknown) {
      if (this.isConflictError(err)) {
        throw new SearchException(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
      }
      throw err;
    }
  }

  async releaseLock(jobId: string): Promise<boolean> {
    try {
      const res = await this.client.get<{ job_id: string }>({
        index: this.indexName,
        id: ACTIVE_LOCK_ID,
      });

      if (res.found && res._source?.job_id === jobId) {
        await this.client.delete({
          index: this.indexName,
          id: ACTIVE_LOCK_ID,
          refresh: 'wait_for',
        });
        return true;
      }
      return false;
    } catch (err: unknown) {
      if (this.isNotFoundError(err)) {
        return false;
      }
      this.logger.warn(`Failed to release lock for ${jobId}: ${err}`);
      return false;
    }
  }

  async create(job: ReindexJobDocument): Promise<void> {
    await this.ensureMapping();

    await this.client.index({
      index: this.indexName,
      id: job.job_id,
      document: job,
      op_type: 'create',
      refresh: 'wait_for',
    });
  }

  async get(jobId: string): Promise<ReindexJobRecord> {
    if (!jobId || !VALID_JOB_ID_REGEX.test(jobId) || jobId === ACTIVE_LOCK_ID) {
      throw new SearchException(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
    }

    try {
      const res = await this.client.get<ReindexJobDocument>({
        index: this.indexName,
        id: jobId,
      });

      if (!res.found || !res._source || res._id === ACTIVE_LOCK_ID) {
        throw new SearchException(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
      }

      return {
        doc: res._source,
        _seq_no: res._seq_no,
        _primary_term: res._primary_term,
      };
    } catch (err: unknown) {
      if (this.isNotFoundError(err)) {
        throw new SearchException(SearchErrorCode.SEARCH_REINDEX_NOT_FOUND);
      }
      throw err;
    }
  }

  async update(
    job: ReindexJobDocument,
    seqNo?: number,
    primaryTerm?: number,
  ): Promise<ReindexJobRecord> {
    job.updated_at = new Date().toISOString();

    const params: {
      index: string;
      id: string;
      document: ReindexJobDocument;
      refresh: 'wait_for';
      if_seq_no?: number;
      if_primary_term?: number;
    } = {
      index: this.indexName,
      id: job.job_id,
      document: job,
      refresh: 'wait_for',
    };

    if (seqNo !== undefined && primaryTerm !== undefined) {
      params.if_seq_no = seqNo;
      params.if_primary_term = primaryTerm;
    }

    try {
      const res = await this.client.index(params);
      return {
        doc: job,
        _seq_no: res._seq_no,
        _primary_term: res._primary_term,
      };
    } catch (err: unknown) {
      if (this.isConflictError(err)) {
        throw new SearchException(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
      }
      throw err;
    }
  }

  async latest(): Promise<ReindexJobRecord | null> {
    try {
      const res = await this.client.search<ReindexJobDocument>({
        index: this.indexName,
        query: {
          bool: {
            must_not: [{ ids: { values: [ACTIVE_LOCK_ID] } }],
          },
        },
        sort: [{ started_at: { order: 'desc' } }],
        size: 1,
      });

      const hit = res.hits.hits[0];
      if (!hit || !hit._source) return null;

      return {
        doc: hit._source,
        _seq_no: hit._seq_no,
        _primary_term: hit._primary_term,
      };
    } catch (err: unknown) {
      if (this.isNotFoundError(err)) {
        return null;
      }
      this.logger.warn(`Failed to query latest reindex job: ${err}`);
      return null;
    }
  }

  async findActiveJob(): Promise<ReindexJobDocument | null> {
    try {
      const res = await this.client.search<ReindexJobDocument>({
        index: this.indexName,
        query: {
          bool: {
            must_not: [{ ids: { values: [ACTIVE_LOCK_ID] } }],
            filter: [
              {
                terms: {
                  state: [
                    ReindexState.REQUESTED,
                    ReindexState.RUNNING,
                    ReindexState.VALIDATING,
                  ],
                },
              },
            ],
          },
        },
        size: 1,
      });

      return res.hits.hits[0]?._source ?? null;
    } catch (err: unknown) {
      if (this.isNotFoundError(err)) {
        return null;
      }
      this.logger.warn(`Failed to query active reindex job: ${err}`);
      return null;
    }
  }

  private isConflictError(err: unknown): boolean {
    if (!err || typeof err !== 'object') return false;
    const candidate = err as {
      meta?: { statusCode?: number; body?: { error?: { type?: string } } };
      statusCode?: number;
      message?: string;
    };
    return (
      candidate.meta?.statusCode === 409 ||
      candidate.statusCode === 409 ||
      candidate.meta?.body?.error?.type ===
        'version_conflict_engine_exception' ||
      Boolean(
        candidate.message &&
        candidate.message.includes('version_conflict_engine_exception'),
      )
    );
  }

  private isNotFoundError(err: unknown): boolean {
    if (!err || typeof err !== 'object') return false;
    const candidate = err as {
      meta?: {
        statusCode?: number;
        body?: { error?: { type?: string; reason?: string } };
      };
      statusCode?: number;
      message?: string;
    };
    return (
      candidate.meta?.statusCode === 404 ||
      candidate.statusCode === 404 ||
      candidate.meta?.body?.error?.type === 'index_not_found_exception' ||
      Boolean(
        candidate.message &&
        candidate.message.includes('index_not_found_exception'),
      )
    );
  }
}
