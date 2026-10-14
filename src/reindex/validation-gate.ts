import { Client } from '@elastic/elasticsearch';
import { Injectable, Logger } from '@nestjs/common';
import { ReindexErrorCode } from './enums';

export interface GateResult {
  status: 'PASS' | 'FAIL';
  pass: boolean;
  passed: boolean;
  reason?: ReindexErrorCode;
  errorCode?: ReindexErrorCode;
  diffPercent?: number;
}

export function passResult(diffPercent = 0): GateResult {
  return { status: 'PASS', pass: true, passed: true, diffPercent };
}

export function failResult(
  reason: ReindexErrorCode,
  diffPercent?: number,
): GateResult {
  return {
    status: 'FAIL',
    pass: false,
    passed: false,
    reason,
    errorCode: reason,
    diffPercent,
  };
}

@Injectable()
export class ValidationGate {
  private readonly logger = new Logger(ValidationGate.name);

  constructor(private readonly client: Client) {}

  private formatError(err: unknown): string {
    const errorClass =
      (err as object)?.constructor?.name || typeof err || 'UnknownError';
    return `[${errorClass}]`;
  }

  async checkDocumentCount(
    targetIndex: string,
    activeIndex: string | null,
  ): Promise<GateResult> {
    try {
      let targetCount = 0;
      let activeCount = 0;

      try {
        const t = await this.client.count({ index: targetIndex });
        targetCount = t.count;
      } catch {
        // target index count 0
      }

      if (activeIndex) {
        try {
          const a = await this.client.count({ index: activeIndex });
          activeCount = a.count;
        } catch {
          // active index count 0
        }
      }

      if (activeCount === 0) {
        return passResult(0);
      }

      if (targetCount === 0 && activeCount > 0) {
        return failResult(ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH, 100);
      }

      const diffPercent =
        (Math.abs(targetCount - activeCount) / activeCount) * 100;
      // Threshold is 0.1%
      if (diffPercent > 0.10001) {
        return failResult(
          ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
          diffPercent,
        );
      }

      return passResult(diffPercent);
    } catch (err: unknown) {
      this.logger.warn(
        `Error during checkDocumentCount: ${this.formatError(err)}`,
      );
      return failResult(ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH);
    }
  }

  async checkDocumentCountGate(
    arg1: string,
    arg2: string | null,
  ): Promise<GateResult> {
    return this.checkDocumentCount(arg1, arg2);
  }

  async checkSourceVersion(
    targetIndex: string,
    activeIndex: string | null,
    sampleSize = 200,
  ): Promise<GateResult> {
    if (!activeIndex) {
      return passResult();
    }

    try {
      const activeRes = await this.client.search<{
        product_id: string;
        source_version: number;
      }>({
        index: activeIndex,
        size: sampleSize,
        _source: ['product_id', 'source_version'],
      });

      const activeHits = activeRes.hits?.hits ?? [];
      if (activeHits.length === 0) {
        return passResult();
      }

      const targetRes = await this.client.search<{
        product_id: string;
        source_version: number;
      }>({
        index: targetIndex,
        size: sampleSize,
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

      for (const s of activeHits) {
        const id = s._id || s._source?.product_id;
        const sVersion = s._source?.source_version ?? 0;
        const tVersion = id ? targetMap.get(id) : undefined;
        if (tVersion === undefined || tVersion < sVersion) {
          return failResult(ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION);
        }
      }

      return passResult();
    } catch (err: unknown) {
      this.logger.warn(
        `Error during checkSourceVersion: ${this.formatError(err)}`,
      );
      return failResult(ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION);
    }
  }

  async checkSourceVersionGate(
    sourceIndex: string,
    targetIndex: string,
  ): Promise<GateResult> {
    return this.checkSourceVersion(targetIndex, sourceIndex);
  }

  async checkSourceVersions(
    targetIndex: string,
    activeIndex: string | null,
    sampleSize = 200,
  ): Promise<GateResult> {
    return this.checkSourceVersion(targetIndex, activeIndex, sampleSize);
  }

  async checkSmokeQueries(targetIndex: string): Promise<GateResult> {
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
        suggest: {
          smoke_suggest: {
            prefix: 'a',
            completion: {
              field: 'suggest',
              size: 5,
              skip_duplicates: true,
            },
          },
        },
      });

      if (smokeRes._shards?.failed && smokeRes._shards.failed > 0) {
        return failResult(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
      }

      const totalValue =
        typeof smokeRes.hits?.total === 'number'
          ? smokeRes.hits.total
          : (smokeRes.hits?.total?.value ?? 0);

      if (totalValue === 0) {
        return failResult(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
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
        return failResult(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
      }

      const hits = smokeRes.hits?.hits ?? [];
      for (const hit of hits) {
        if (
          hit._source?.visibility_status &&
          hit._source.visibility_status !== 'PUBLISHED'
        ) {
          return failResult(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
        }
      }

      return passResult();
    } catch (err: unknown) {
      this.logger.warn(
        `Error during checkSmokeQueries: ${this.formatError(err)}`,
      );
      return failResult(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
    }
  }

  async checkSmokeQueriesGate(targetIndex: string): Promise<GateResult> {
    return this.checkSmokeQueries(targetIndex);
  }

  async runSmokeQueries(targetIndex: string): Promise<GateResult> {
    return this.checkSmokeQueries(targetIndex);
  }

  async run(
    targetIndex: string,
    activeIndex: string | null,
  ): Promise<GateResult> {
    try {
      await this.client.indices.refresh({ index: targetIndex });

      const countResult = await this.checkDocumentCount(
        targetIndex,
        activeIndex,
      );
      if (!countResult.pass) {
        return countResult;
      }

      if (activeIndex) {
        const versionResult = await this.checkSourceVersion(
          targetIndex,
          activeIndex,
        );
        if (!versionResult.pass) {
          return versionResult;
        }
      }

      const smokeResult = await this.checkSmokeQueries(targetIndex);
      if (!smokeResult.pass) {
        return smokeResult;
      }

      return passResult();
    } catch (err: unknown) {
      this.logger.warn(
        `Error during validation gate run: ${this.formatError(err)}`,
      );
      return failResult(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
    }
  }
}
