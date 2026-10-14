import { Client } from '@elastic/elasticsearch';
import {
  IndicesIndexSettings,
  MappingTypeMapping,
} from '@elastic/elasticsearch/lib/api/types';
import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import {
  getConsumerStateIndex,
  getProductsAlias,
  getReindexJobsIndex,
  getSearchCategoriesIndex,
  getVersionedIndexName,
  MAPPING_VERSION,
} from '../common/constants';
import { AppConfigService } from '../config/config.service';
import {
  CONSUMER_STATE_MAPPING_V1,
  PRODUCTS_INDEX_MAPPING_V1,
  PRODUCTS_INDEX_SETTINGS_V1,
  REINDEX_JOBS_MAPPING_V1,
  SEARCH_CATEGORIES_MAPPING_V1,
} from './mappings';

export interface BootstrapResult {
  alias: string;
  aliasCreated: boolean;
  activeTargetIndex?: string;
  consumerStateIndex: string;
  consumerStateCreated: boolean;
  reindexJobsIndex: string;
  reindexJobsCreated: boolean;
  searchCategoriesIndex: string;
  searchCategoriesCreated: boolean;
}

@Injectable()
export class IndexBootstrapService implements OnApplicationBootstrap {
  private readonly logger = new Logger(IndexBootstrapService.name);

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
  ) {}

  async onApplicationBootstrap() {
    try {
      await this.bootstrap();
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn(`Failed to bootstrap Elasticsearch indices: ${message}`);
    }
  }

  private isResourceAlreadyExistsError(err: unknown): boolean {
    if (!err || typeof err !== 'object') return false;
    const candidate = err as {
      meta?: {
        statusCode?: number;
        body?: {
          error?: {
            type?: string;
          };
        };
      };
      message?: string;
    };
    return (
      candidate.meta?.body?.error?.type ===
        'resource_already_exists_exception' ||
      Boolean(
        candidate.message &&
        candidate.message.includes('resource_already_exists_exception'),
      )
    );
  }

  private async ensureIndex(
    name: string,
    mappings: MappingTypeMapping,
    settings?: IndicesIndexSettings,
  ): Promise<boolean> {
    const exists = await this.client.indices.exists({ index: name });
    if (!exists) {
      try {
        await this.client.indices.create({
          index: name,
          mappings,
          ...(settings ? { settings } : {}),
        });
        if (settings) {
          this.logger.log(`Created index ${name}`);
        } else {
          this.logger.log(`Created internal index ${name}`);
        }
        return true;
      } catch (err: unknown) {
        if (!this.isResourceAlreadyExistsError(err)) {
          throw err;
        }
      }
    }
    return false;
  }

  async bootstrap(): Promise<BootstrapResult> {
    const prefix = this.configService.esIndexPrefix;
    const alias = getProductsAlias(prefix);

    let aliasCreated = false;
    let activeTargetIndex: string | undefined;

    // 1. Check if alias exists
    const aliasExists = await this.client.indices.existsAlias({ name: alias });
    if (!aliasExists) {
      activeTargetIndex = getVersionedIndexName(
        MAPPING_VERSION,
        undefined,
        prefix,
      );

      await this.ensureIndex(
        activeTargetIndex,
        PRODUCTS_INDEX_MAPPING_V1,
        PRODUCTS_INDEX_SETTINGS_V1,
      );

      await this.client.indices.updateAliases({
        actions: [
          {
            add: {
              index: activeTargetIndex,
              alias,
            },
          },
        ],
      });
      aliasCreated = true;

      // Re-read alias to check if another replica concurrently added to it
      const aliasMap = await this.client.indices.getAlias({ name: alias });
      const aliasedIndices = Object.keys(aliasMap).sort();

      if (aliasedIndices.length > 1) {
        const keeper = aliasedIndices[0];
        const others = aliasedIndices.slice(1);

        await this.client.indices.updateAliases({
          actions: others.map((otherIndex) => ({
            remove: {
              index: otherIndex,
              alias,
            },
          })),
        });

        // Delete the one this instance created if it is not the keeper
        if (others.includes(activeTargetIndex)) {
          try {
            await this.client.indices.delete({ index: activeTargetIndex });
          } catch (err: unknown) {
            this.logger.debug(
              `Index ${activeTargetIndex} already deleted: ${err}`,
            );
          }
        }
        activeTargetIndex = keeper;
      }
      this.logger.log(`Created index ${activeTargetIndex} and alias ${alias}`);
    } else {
      const aliasMap = await this.client.indices.getAlias({ name: alias });
      const aliasedIndices = Object.keys(aliasMap).sort();
      if (aliasedIndices.length > 0) {
        activeTargetIndex = aliasedIndices[0];
      }
    }

    // 2. Ensure search-consumer-state index exists
    const consumerStateIndex = getConsumerStateIndex(prefix);
    const consumerStateCreated = await this.ensureIndex(
      consumerStateIndex,
      CONSUMER_STATE_MAPPING_V1,
    );

    // 3. Ensure search-reindex-jobs index exists
    const reindexJobsIndex = getReindexJobsIndex(prefix);
    const reindexJobsCreated = await this.ensureIndex(
      reindexJobsIndex,
      REINDEX_JOBS_MAPPING_V1,
    );

    // 4. Ensure search-categories index exists
    const searchCategoriesIndex = getSearchCategoriesIndex(prefix);
    const searchCategoriesCreated = await this.ensureIndex(
      searchCategoriesIndex,
      SEARCH_CATEGORIES_MAPPING_V1,
    );

    return {
      alias,
      aliasCreated,
      activeTargetIndex,
      consumerStateIndex,
      consumerStateCreated,
      reindexJobsIndex,
      reindexJobsCreated,
      searchCategoriesIndex,
      searchCategoriesCreated,
    };
  }
}
