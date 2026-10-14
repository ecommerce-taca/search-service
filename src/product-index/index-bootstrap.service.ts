import { Client } from '@elastic/elasticsearch';
import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import {
  getConsumerStateIndex,
  getProductsAlias,
  getReindexJobsIndex,
  getVersionedIndexName,
  MAPPING_VERSION,
} from '../common/constants';
import { AppConfigService } from '../config/config.service';
import {
  CONSUMER_STATE_MAPPING_V1,
  PRODUCTS_INDEX_MAPPING_V1,
  PRODUCTS_INDEX_SETTINGS_V1,
  REINDEX_JOBS_MAPPING_V1,
} from './mappings';

export interface BootstrapResult {
  alias: string;
  aliasCreated: boolean;
  activeTargetIndex?: string;
  consumerStateIndex: string;
  consumerStateCreated: boolean;
  reindexJobsIndex: string;
  reindexJobsCreated: boolean;
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
      const indexExists = await this.client.indices.exists({
        index: activeTargetIndex,
      });
      if (!indexExists) {
        await this.client.indices.create({
          index: activeTargetIndex,
          settings: PRODUCTS_INDEX_SETTINGS_V1,
          mappings: PRODUCTS_INDEX_MAPPING_V1,
        });
      }

      await this.client.indices.putAlias({
        index: activeTargetIndex,
        name: alias,
      });
      aliasCreated = true;
      this.logger.log(`Created index ${activeTargetIndex} and alias ${alias}`);
    }

    // 2. Ensure search-consumer-state index exists
    const consumerStateIndex = getConsumerStateIndex(prefix);
    let consumerStateCreated = false;
    const consumerStateExists = await this.client.indices.exists({
      index: consumerStateIndex,
    });
    if (!consumerStateExists) {
      await this.client.indices.create({
        index: consumerStateIndex,
        mappings: CONSUMER_STATE_MAPPING_V1,
      });
      consumerStateCreated = true;
      this.logger.log(`Created internal index ${consumerStateIndex}`);
    }

    // 3. Ensure search-reindex-jobs index exists
    const reindexJobsIndex = getReindexJobsIndex(prefix);
    let reindexJobsCreated = false;
    const reindexJobsExists = await this.client.indices.exists({
      index: reindexJobsIndex,
    });
    if (!reindexJobsExists) {
      await this.client.indices.create({
        index: reindexJobsIndex,
        mappings: REINDEX_JOBS_MAPPING_V1,
      });
      reindexJobsCreated = true;
      this.logger.log(`Created internal index ${reindexJobsIndex}`);
    }

    return {
      alias,
      aliasCreated,
      activeTargetIndex,
      consumerStateIndex,
      consumerStateCreated,
      reindexJobsIndex,
      reindexJobsCreated,
    };
  }
}
