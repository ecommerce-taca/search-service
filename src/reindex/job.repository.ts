import { Client } from '@elastic/elasticsearch';
import { Injectable, Logger } from '@nestjs/common';
import { getReindexJobsIndex, MAPPING_VERSION } from '../common/constants';
import { ReindexState } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { toSearchException } from '../common/es/es-error.mapper';
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
  source?: string;
  reason?: string;
}

export type ReindexJobRecord = ReindexJobDocument & {
  _seq_no?: number;
  _primary_term?: number;
  doc?: ReindexJobDocument;
};

const ACTIVE_LOCK_ID = 'active-lock';

@Injectable()
export class ReindexJobRepository {
  private readonly logger = new Logger(ReindexJobRepository.name);

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
  ) {}

  get indexName(): string {
    return getReindexJobsIndex(this.configService.esIndexPrefix);
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
      throw toSearchException(err);
    }
  }

  async releaseLock(jobId: string): Promise<boolean> {
    try {
      const res = await this.client.get<{ job_id: string }>({
        index: this.indexName,
        id: ACTIVE_LOCK_ID,
      });

      if (res && res._source?.job_id === jobId) {
        await this.client.delete({
          index: this.indexName,
          id: ACTIVE_LOCK_ID,
        });
        return true;
      }
      return false;
    } catch (err: unknown) {
      if (this.isNotFoundError(err)) {
        return false;
      }
      const errClass =
        (err as object)?.constructor?.name || typeof err || 'UnknownError';
      this.logger.warn(`Failed to release lock for ${jobId}: [${errClass}]`);
      throw toSearchException(err);
    }
  }

  async create(job: ReindexJobDocument): Promise<void> {
    const document: Partial<ReindexJobDocument> = { ...job };
    delete document.source;
    delete document.reason;
    try {
      await this.client.index({
        index: this.indexName,
        id: job.job_id,
        document,
        refresh: 'wait_for',
      });
    } catch (err: unknown) {
      throw toSearchException(err);
    }
  }

  async get(jobId: string): Promise<ReindexJobRecord | null> {
    if (!jobId || jobId === ACTIVE_LOCK_ID) {
      return null;
    }

    try {
      const res = await this.client.get<ReindexJobDocument>({
        index: this.indexName,
        id: jobId,
      });

      if (!res || !res._source || res._id === ACTIVE_LOCK_ID) {
        return null;
      }

      const doc = res._source;
      return {
        ...doc,
        doc,
        _seq_no: res._seq_no,
        _primary_term: res._primary_term,
      };
    } catch (err: unknown) {
      if (this.isNotFoundError(err)) {
        return null;
      }
      throw toSearchException(err);
    }
  }

  async updateProgress(
    jobId: string,
    progress: {
      checkpoint: string | null;
      processed_count: number;
      failed_count?: number;
    },
  ): Promise<void> {
    const updatedAt = new Date().toISOString();
    try {
      await this.client.update({
        index: this.indexName,
        id: jobId,
        doc: {
          checkpoint: progress.checkpoint,
          processed_count: progress.processed_count,
          failed_count: progress.failed_count ?? 0,
          updated_at: updatedAt,
        },
        doc_as_upsert: true,
        refresh: 'wait_for',
      });
    } catch (err: unknown) {
      throw toSearchException(err);
    }
  }

  async update(
    job: ReindexJobDocument,
    seqNoOrOptions?: number | { ifSeqNo?: number; ifPrimaryTerm?: number },
    primaryTermArg?: number,
  ): Promise<ReindexJobRecord> {
    job.updated_at = new Date().toISOString();

    let seqNo: number | undefined;
    let primaryTerm: number | undefined;

    if (seqNoOrOptions !== undefined) {
      if (typeof seqNoOrOptions === 'object') {
        seqNo = seqNoOrOptions.ifSeqNo;
        primaryTerm = seqNoOrOptions.ifPrimaryTerm;
      } else {
        seqNo = seqNoOrOptions;
        primaryTerm = primaryTermArg;
      }
    }

    const document: Partial<ReindexJobDocument> = { ...job };
    delete document.source;
    delete document.reason;
    const params: {
      index: string;
      id: string;
      document: Partial<ReindexJobDocument>;
      refresh: 'wait_for';
      if_seq_no?: number;
      if_primary_term?: number;
    } = {
      index: this.indexName,
      id: job.job_id,
      document,
      refresh: 'wait_for',
    };

    if (seqNo !== undefined && primaryTerm !== undefined) {
      params.if_seq_no = seqNo;
      params.if_primary_term = primaryTerm;
    }

    try {
      const res = await this.client.index(params);
      return {
        ...job,
        doc: job,
        _seq_no: res._seq_no,
        _primary_term: res._primary_term,
      };
    } catch (err: unknown) {
      if (this.isConflictError(err)) {
        throw new SearchException(SearchErrorCode.SEARCH_REINDEX_CONFLICT);
      }
      throw toSearchException(err);
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

      const hit = res.hits?.hits?.[0];
      if (!hit || !hit._source) return null;

      const doc = hit._source;
      return {
        ...doc,
        doc,
        _seq_no: hit._seq_no,
        _primary_term: hit._primary_term,
      };
    } catch (err: unknown) {
      if (this.isNotFoundError(err)) {
        return null;
      }
      throw toSearchException(err);
    }
  }

  async findLatestJob(): Promise<ReindexJobRecord | null> {
    return this.latest();
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

      return res.hits?.hits?.[0]?._source ?? null;
    } catch (err: unknown) {
      if (this.isNotFoundError(err)) {
        return null;
      }
      throw toSearchException(err);
    }
  }

  async hasActiveJob(): Promise<boolean> {
    try {
      const res = await this.client.search<ReindexJobDocument>({
        index: this.indexName,
        query: {
          bool: {
            must: [
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
            must_not: [{ ids: { values: [ACTIVE_LOCK_ID] } }],
          },
        },
        size: 1,
      });

      return (res.hits?.hits?.length ?? 0) > 0;
    } catch (err: unknown) {
      if (this.isNotFoundError(err)) {
        return false;
      }
      throw toSearchException(err);
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
