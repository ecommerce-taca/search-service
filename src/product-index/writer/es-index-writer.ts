import { Client } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { ES_BULK_BATCH_SIZE } from '../../common/constants';
import { ELASTICSEARCH_CLIENT } from '../elasticsearch.module';
import {
  IIndexWriter,
  INDEX_WRITER,
  ProductSearchDocument,
} from '../interfaces';

export interface BulkOperationSummary {
  applied: number;
  skipped: number;
  failed: number;
}

@Injectable()
export class EsIndexWriter implements IIndexWriter {
  private readonly logger = new Logger(EsIndexWriter.name);

  constructor(
    @Inject(ELASTICSEARCH_CLIENT) private readonly client: Client,
  ) {}

  /**
   * Bulk upserts product documents with optimistic version guard:
   * version_type: external_gte, version: source_version.
   * Treats 409 version conflicts as SKIP, not error.
   */
  async bulkUpsert(
    index: string,
    docs: ProductSearchDocument[],
  ): Promise<void> {
    await this.bulkUpsertWithSummary(index, docs);
  }

  async bulkUpsertWithSummary(
    index: string,
    docs: ProductSearchDocument[],
  ): Promise<BulkOperationSummary> {
    const summary: BulkOperationSummary = { applied: 0, skipped: 0, failed: 0 };
    if (!docs || docs.length === 0) {
      return summary;
    }

    const chunks = this.chunkArray(docs, ES_BULK_BATCH_SIZE);
    for (const chunk of chunks) {
      const operations = chunk.flatMap((doc) => [
        {
          index: {
            _index: index,
            _id: doc.product_id,
            version: doc.source_version,
            version_type: 'external_gte' as const,
          },
        },
        doc,
      ]);

      const response = await this.client.bulk({
        operations,
        refresh: false,
      });

      this.tallyBulkItems(response.items, summary);
    }

    this.logger.debug(
      `Bulk upsert to ${index}: applied=${summary.applied}, skipped=${summary.skipped}, failed=${summary.failed}`,
    );
    return summary;
  }

  /**
   * Bulk partial updates product documents.
   * Treats 409 conflicts as SKIP.
   */
  async bulkPartialUpdate(
    index: string,
    updates: Array<{ id: string; doc: Partial<ProductSearchDocument> }>,
  ): Promise<void> {
    await this.bulkPartialUpdateWithSummary(index, updates);
  }

  async bulkPartialUpdateWithSummary(
    index: string,
    updates: Array<{ id: string; doc: Partial<ProductSearchDocument> }>,
  ): Promise<BulkOperationSummary> {
    const summary: BulkOperationSummary = { applied: 0, skipped: 0, failed: 0 };
    if (!updates || updates.length === 0) {
      return summary;
    }

    const chunks = this.chunkArray(updates, ES_BULK_BATCH_SIZE);
    for (const chunk of chunks) {
      const operations = chunk.flatMap((u) => [
        {
          update: {
            _index: index,
            _id: u.id,
            retry_on_conflict: 3,
          },
        },
        {
          doc: u.doc,
        },
      ]);

      const response = await this.client.bulk({
        operations,
        refresh: false,
      });

      this.tallyBulkItems(response.items, summary);
    }

    this.logger.debug(
      `Bulk partial update to ${index}: applied=${summary.applied}, skipped=${summary.skipped}, failed=${summary.failed}`,
    );
    return summary;
  }

  /**
   * Fetches current source_version for a list of document IDs using mget.
   */
  async getCurrentVersions(
    index: string,
    ids: string[],
  ): Promise<Map<string, number>> {
    const result = new Map<string, number>();
    const uniqueIds = Array.from(new Set(ids.filter((id) => Boolean(id))));
    if (uniqueIds.length === 0) {
      return result;
    }

    const chunks = this.chunkArray(uniqueIds, ES_BULK_BATCH_SIZE);
    for (const chunk of chunks) {
      const response = await this.client.mget<Pick<ProductSearchDocument, 'source_version'>>({
        index,
        ids: chunk,
        _source: ['source_version'],
      });

      if (response.docs) {
        for (const item of response.docs) {
          if ('found' in item && item.found && item._source) {
            const version = item._source.source_version ?? item._version;
            if (typeof version === 'number') {
              result.set(item._id, version);
            }
          }
        }
      }
    }

    return result;
  }

  private tallyBulkItems(
    items: Array<Record<string, any>> | undefined,
    summary: BulkOperationSummary,
  ): void {
    if (!items) {
      return;
    }

    for (const item of items) {
      const action =
        item.index || item.create || item.update || item.delete;
      if (!action) {
        continue;
      }

      const status = action.status as number;
      const error = action.error;

      if (
        status === 409 ||
        error?.type === 'version_conflict_engine_exception' ||
        error?.status === 409
      ) {
        summary.skipped++;
      } else if (error || (status >= 400 && status !== 404)) {
        summary.failed++;
        this.logger.warn(`ES bulk item failed with status ${status}: ${error?.reason || error?.type || 'error'}`);
      } else {
        summary.applied++;
      }
    }
  }

  private chunkArray<T>(array: T[], size: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += size) {
      chunks.push(array.slice(i, i + size));
    }
    return chunks;
  }
}
