import { Client, errors } from '@elastic/elasticsearch';
import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import {
  ES_BULK_BATCH_SIZE,
  getProductsAlias,
  getSearchCategoriesIndex,
} from '../../common/constants';
import { AppConfigService } from '../../config/config.service';
import { DomainEvent } from '../../event-consumer/interfaces';
import { ELASTICSEARCH_CLIENT } from '../elasticsearch.module';
import {
  CategoryProjection,
  CATEGORY_PROJECTION,
  ICategoryProjection,
  IIndexWriter,
  INDEX_WRITER,
  ProductSearchDocument,
} from '../interfaces';

export interface CategoryEventPayload {
  category_id?: string;
  name?: string;
  parent_id?: string | null;
  status?: string;
}

export type CategoryEventInput =
  | DomainEvent<unknown>
  | (CategoryEventPayload & {
      version: number;
      updated_at?: string;
      occurred_at?: string;
      event_id?: string;
      aggregate_id?: string;
      payload?: CategoryEventPayload;
    });

@Injectable()
export class CategoryProjectionWriter {
  private readonly logger = new Logger(CategoryProjectionWriter.name);
  private readonly client: Client;
  private readonly configService?: AppConfigService;
  private readonly categoryProjection: ICategoryProjection;
  private readonly indexWriter: IIndexWriter;

  constructor(
    @Inject(ELASTICSEARCH_CLIENT) client: Client,
    @Inject(AppConfigService)
    configServiceOrCatProj: AppConfigService | ICategoryProjection,
    @Inject(CATEGORY_PROJECTION)
    categoryProjectionOrIndexWriter?: ICategoryProjection | IIndexWriter,
    @Optional() @Inject(INDEX_WRITER) indexWriter?: IIndexWriter,
  ) {
    this.client = client;
    if (
      configServiceOrCatProj &&
      'resolvePathIds' in (configServiceOrCatProj as object)
    ) {
      this.categoryProjection = configServiceOrCatProj as ICategoryProjection;
      this.indexWriter = categoryProjectionOrIndexWriter as IIndexWriter;
    } else {
      this.configService = configServiceOrCatProj as AppConfigService;
      this.categoryProjection =
        categoryProjectionOrIndexWriter as ICategoryProjection;
      this.indexWriter = indexWriter as IIndexWriter;
    }
  }

  /**
   * Upserts search-categories documents from category events with version guard (external_gte).
   * Computes path_ids = parent.path_ids + [id].
   * On path change, re-resolves affected products via search_after (batch 500) and updates category_ids.
   */
  async handleCategoryEvent(evt: CategoryEventInput): Promise<void> {
    const rawPayload =
      'payload' in evt && evt.payload
        ? (evt.payload as CategoryEventPayload)
        : (evt as CategoryEventPayload);
    const categoryId =
      rawPayload.category_id ||
      ('aggregate_id' in evt
        ? (evt as DomainEvent<unknown>).aggregate_id
        : undefined);
    if (!categoryId) {
      return;
    }

    const categoriesIndex = getSearchCategoriesIndex(
      this.configService?.esIndexPrefix || '',
    );

    // Look up existing projection via ICategoryProjection
    let existing: CategoryProjection | undefined;
    if (this.categoryProjection) {
      const existingMap = await this.categoryProjection.getMany([categoryId]);
      existing = existingMap.get(categoryId);
    }

    if (existing && evt.version < existing.source_version) {
      this.logger.debug(
        `Skipping category event ${'event_id' in evt ? evt.event_id : categoryId}: version ${evt.version} < stored ${existing.source_version}`,
      );
      return;
    }

    const parentId =
      rawPayload.parent_id !== undefined
        ? rawPayload.parent_id
        : (existing?.parent_id ?? null);

    let pathIds: string[] = [];
    if (parentId && parentId.trim()) {
      const parentPathIds = await this.categoryProjection.resolvePathIds(
        parentId.trim(),
      );
      if (parentPathIds.length > 0) {
        pathIds = [...parentPathIds, categoryId];
      } else {
        pathIds = [parentId.trim(), categoryId];
      }
    } else {
      pathIds = [categoryId];
    }

    // Determine if path has changed
    const pathChanged =
      existing !== undefined &&
      (existing.path_ids.length !== pathIds.length ||
        existing.path_ids.some((id, idx) => id !== pathIds[idx]));

    const projectionDoc: CategoryProjection = {
      category_id: categoryId,
      name: rawPayload.name ?? existing?.name ?? '',
      parent_id: parentId,
      path_ids: pathIds,
      status: rawPayload.status ?? existing?.status ?? 'ACTIVE',
      source_version: evt.version,
      updated_at:
        'occurred_at' in evt && evt.occurred_at
          ? evt.occurred_at
          : 'updated_at' in evt && evt.updated_at
            ? evt.updated_at
            : new Date().toISOString(),
    };

    try {
      await this.client.index({
        index: categoriesIndex,
        id: categoryId,
        version: evt.version,
        version_type: 'external_gte',
        document: projectionDoc,
      });
    } catch (err: unknown) {
      if (
        (err instanceof errors.ResponseError ||
          (err as { name?: string })?.name === 'ResponseError') &&
        ((err as { statusCode?: number })?.statusCode === 409 ||
          (err as { meta?: { statusCode?: number } })?.meta?.statusCode === 409)
      ) {
        this.logger.debug(
          `Version conflict on category projection for ${categoryId}, skipping.`,
        );
        return;
      }
      throw err;
    }

    if (pathChanged) {
      this.logger.log(
        `Category ${categoryId} path changed, re-resolving affected products...`,
      );
      await this.reResolveAffectedProducts(categoryId);
    }
  }

  /**
   * Finds all product documents containing the given category_id in category_ids,
   * re-resolves their full category_ids path and updates them using bulk partial update.
   */
  async reResolveAffectedProducts(categoryId: string): Promise<void> {
    const productsAlias = getProductsAlias(
      this.configService?.esIndexPrefix || '',
    );
    let searchAfter: unknown = undefined;

    while (true) {
      const searchRes = await this.client.search<
        Pick<ProductSearchDocument, 'product_id' | 'category_ids'>
      >({
        index: productsAlias,
        size: ES_BULK_BATCH_SIZE,
        sort: [{ product_id: 'asc' }],
        query: {
          term: { category_ids: categoryId },
        },
        _source: ['product_id', 'category_ids'],
        search_after: searchAfter as (string | number)[] | undefined,
      });

      const hits = searchRes.hits?.hits || [];
      if (hits.length === 0) {
        break;
      }

      const updates: Array<{
        id: string;
        doc: Partial<ProductSearchDocument>;
      }> = [];

      for (const hit of hits) {
        if (!hit._id || !hit._source) {
          continue;
        }

        const currentCatIds = hit._source.category_ids || [];
        const leafId = currentCatIds[currentCatIds.length - 1];

        if (leafId) {
          const newPath = await this.categoryProjection.resolvePathIds(leafId);
          if (newPath.length > 0) {
            updates.push({
              id: hit._id,
              doc: {
                category_ids: newPath,
                indexed_at: new Date().toISOString(),
              },
            });
          }
        }
      }

      if (updates.length > 0) {
        await this.indexWriter.bulkPartialUpdate(productsAlias, updates);
      }

      if (hits.length < ES_BULK_BATCH_SIZE) {
        break;
      }

      searchAfter = hits[hits.length - 1].sort;
    }
  }
}
