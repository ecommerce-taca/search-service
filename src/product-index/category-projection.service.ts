import { Client, errors } from '@elastic/elasticsearch';
import { Inject, Injectable } from '@nestjs/common';
import {
  ES_REQUEST_TIMEOUT_MS,
  getSearchCategoriesIndex,
} from '../common/constants';
import { AppConfigService } from '../config/config.service';
import { ELASTICSEARCH_CLIENT } from './elasticsearch.module';
import { CategoryProjection, ICategoryProjection } from './interfaces';

@Injectable()
export class EsCategoryProjection implements ICategoryProjection {
  constructor(
    @Inject(ELASTICSEARCH_CLIENT) private readonly client: Client,
    private readonly configService: AppConfigService,
  ) {}

  async getMany(ids: string[]): Promise<Map<string, CategoryProjection>> {
    const uniqueIds = Array.from(new Set(ids));
    if (uniqueIds.length === 0) {
      return new Map<string, CategoryProjection>();
    }

    const index = getSearchCategoriesIndex(this.configService.esIndexPrefix);
    const response = await this.client.mget<CategoryProjection>(
      {
        index,
        ids: uniqueIds,
      },
      {
        requestTimeout: ES_REQUEST_TIMEOUT_MS,
      },
    );

    const result = new Map<string, CategoryProjection>();
    if (response.docs) {
      for (const item of response.docs) {
        if ('found' in item && item.found && item._source) {
          result.set(item._id, item._source as CategoryProjection);
        }
      }
    }
    return result;
  }

  async resolvePathIds(categoryId: string): Promise<string[]> {
    if (!categoryId || !categoryId.trim()) {
      return [];
    }

    const index = getSearchCategoriesIndex(this.configService.esIndexPrefix);
    try {
      const response = await this.client.get<CategoryProjection>(
        {
          index,
          id: categoryId,
        },
        {
          requestTimeout: ES_REQUEST_TIMEOUT_MS,
        },
      );

      if (response.found && response._source) {
        return response._source.path_ids ?? [];
      }
      return [];
    } catch (err: unknown) {
      if (
        (err instanceof errors.ResponseError ||
          (err as { name?: string })?.name === 'ResponseError') &&
        ((err as { statusCode?: number })?.statusCode === 404 ||
          (err as { meta?: { statusCode?: number } })?.meta?.statusCode === 404)
      ) {
        return [];
      }
      throw err;
    }
  }
}
