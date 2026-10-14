import { Client } from '@elastic/elasticsearch';
import { Injectable, Logger } from '@nestjs/common';
import { ReindexErrorCode } from './enums';

export interface GateResult {
  status: 'PASS' | 'FAIL';
  pass: boolean;
  reason?: ReindexErrorCode;
}

export function passResult(): GateResult {
  return { status: 'PASS', pass: true };
}

export function failResult(reason: ReindexErrorCode): GateResult {
  return { status: 'FAIL', pass: false, reason };
}

@Injectable()
export class ValidationGate {
  private readonly logger = new Logger(ValidationGate.name);

  constructor(private readonly client: Client) {}

  checkDocumentCount(target: number, active: number | null): GateResult;
  checkDocumentCount(
    target: string,
    active: string | null,
  ): Promise<GateResult>;
  checkDocumentCount(
    target: number | string,
    active: number | string | null,
  ): GateResult | Promise<GateResult> {
    if (typeof target === 'number') {
      const activeCount = typeof active === 'number' ? active : 0;
      return this.evaluateDocumentCount(target, activeCount);
    }

    return this.checkDocumentCountAsync(target, active as string | null);
  }

  private evaluateDocumentCount(
    targetCount: number,
    activeCount: number,
  ): GateResult {
    if (activeCount === 0) {
      return passResult();
    }

    if (targetCount === 0 && activeCount > 0) {
      return failResult(ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH);
    }

    const diff = Math.abs(targetCount - activeCount) / activeCount;
    // Threshold is 0.1% (0.001)
    if (diff > 0.0010001) {
      return failResult(ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH);
    }

    return passResult();
  }

  private async checkDocumentCountAsync(
    targetIndex: string,
    activeIndex: string | null,
  ): Promise<GateResult> {
    try {
      await this.client.indices.refresh({ index: targetIndex });
      const targetCountRes = await this.client.count({ index: targetIndex });
      const targetCount = targetCountRes.count;

      let activeCount = 0;
      if (activeIndex) {
        const activeCountRes = await this.client.count({ index: activeIndex });
        activeCount = activeCountRes.count;
      }

      return this.evaluateDocumentCount(targetCount, activeCount);
    } catch (err: unknown) {
      this.logger.warn(`Error during checkDocumentCount: ${err}`);
      return failResult(ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH);
    }
  }

  checkSourceVersions(
    target: Array<{ product_id?: string; source_version: number }>,
    active: Array<{ product_id?: string; source_version: number }>,
    sampleSize?: number,
  ): GateResult;
  checkSourceVersions(
    target: string,
    active: string,
    sampleSize?: number,
  ): Promise<GateResult>;
  checkSourceVersions(
    target: string | Array<{ product_id?: string; source_version: number }>,
    active: string | Array<{ product_id?: string; source_version: number }>,
    sampleSize = 200,
  ): GateResult | Promise<GateResult> {
    if (Array.isArray(target) && Array.isArray(active)) {
      const targetMap = new Map<string, number>();
      target.forEach((doc, idx) => {
        const id = doc.product_id || `idx-${idx}`;
        targetMap.set(id, doc.source_version);
      });

      for (let i = 0; i < active.length; i++) {
        const activeDoc = active[i];
        const id = activeDoc.product_id || `idx-${i}`;
        const targetVersion = targetMap.get(id);

        if (
          targetVersion === undefined ||
          targetVersion < activeDoc.source_version
        ) {
          return failResult(ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION);
        }
      }

      return passResult();
    }

    if (typeof target !== 'string' || typeof active !== 'string') {
      return Promise.resolve(
        failResult(ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION),
      );
    }

    return this.checkSourceVersionsAsync(target, active, sampleSize);
  }

  private async checkSourceVersionsAsync(
    target: string,
    active: string,
    sampleSize = 200,
  ): Promise<GateResult> {
    try {
      const sampleRes = await this.client.search<{
        product_id: string;
        source_version: number;
      }>({
        index: active,
        size: sampleSize,
        _source: ['product_id', 'source_version'],
      });

      const activeHits = sampleRes.hits.hits;
      if (activeHits.length === 0) {
        return passResult();
      }

      const ids = activeHits
        .map((h) => h._id || h._source?.product_id)
        .filter(Boolean) as string[];
      if (ids.length === 0) {
        return passResult();
      }

      const mgetRes = await this.client.mget<{
        product_id: string;
        source_version: number;
      }>({
        index: target,
        ids,
      });

      const targetVersionMap = new Map<string, number>();
      for (const doc of mgetRes.docs) {
        if ('found' in doc && doc.found && doc._source) {
          targetVersionMap.set(doc._id, doc._source.source_version);
        }
      }

      for (const hit of activeHits) {
        const id = hit._id || hit._source?.product_id;
        const activeVersion = hit._source?.source_version ?? 0;
        const targetVersion = targetVersionMap.get(id!);

        if (targetVersion === undefined || targetVersion < activeVersion) {
          return failResult(ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION);
        }
      }

      return passResult();
    } catch (err: unknown) {
      this.logger.warn(`Error during checkSourceVersions: ${err}`);
      return failResult(ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION);
    }
  }

  async runSmokeQueries(targetIndex: string): Promise<GateResult> {
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
          categories: { terms: { field: 'category_ids', size: 10 } },
          brands: { terms: { field: 'brand', size: 10 } },
          attributes: { terms: { field: 'attribute_pairs', size: 10 } },
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

      for (const hit of smokeRes.hits.hits) {
        if (
          hit._source?.visibility_status &&
          hit._source.visibility_status !== 'PUBLISHED'
        ) {
          return failResult(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
        }
      }

      return passResult();
    } catch (err: unknown) {
      this.logger.warn(`Error during runSmokeQueries: ${err}`);
      return failResult(ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED);
    }
  }
}
